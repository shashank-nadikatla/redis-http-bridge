package main

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultAddr           = ":8087"
	defaultMaxBodySize    = 10 << 20 // 10 MB
	defaultClientCacheCap = 32
	// Grace period before a client evicted from the LRU is actually closed.
	// Must comfortably exceed the per-op redis timeouts below so any in-flight
	// command on the evicted pool can finish before its sockets are torn down.
	evictedCloseDelay = 30 * time.Second
)

type ErrorResponse struct {
	Error string `json:"error"`
}

type clientCacheEntry struct {
	key    string
	client *redis.Client
}

// clientCache is an LRU-bounded cache of pooled *redis.Client instances keyed by URL.
// Bounding it prevents unbounded memory and file-descriptor growth if arbitrary or
// hostile URLs are passed in (each entry owns a connection pool, not just a struct).
type clientCache struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List
	items map[string]*list.Element
}

func newClientCache(capacity int) *clientCache {
	if capacity < 1 {
		capacity = 1
	}
	return &clientCache{
		cap:   capacity,
		ll:    list.New(),
		items: make(map[string]*list.Element, capacity),
	}
}

func (c *clientCache) get(key string) (*redis.Client, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*clientCacheEntry).client, true
}

// getOrPut returns the canonical *redis.Client for key. If newClient was inserted,
// canonical == newClient and displaced is either nil or the LRU-evicted oldest entry
// (which still may have in-flight commands). If another goroutine inserted first,
// canonical is the existing entry and displaced is newClient (no in-flight commands).
func (c *clientCache) getOrPut(key string, newClient *redis.Client) (canonical, displaced *redis.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*clientCacheEntry).client, newClient
	}

	el := c.ll.PushFront(&clientCacheEntry{key: key, client: newClient})
	c.items[key] = el

	if c.ll.Len() > c.cap {
		oldest := c.ll.Back()
		c.ll.Remove(oldest)
		ent := oldest.Value.(*clientCacheEntry)
		delete(c.items, ent.key)
		return newClient, ent.client
	}
	return newClient, nil
}

func (c *clientCache) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.ll.Front(); el != nil; el = el.Next() {
		_ = el.Value.(*clientCacheEntry).client.Close()
	}
	c.ll.Init()
	c.items = make(map[string]*list.Element)
}

type Server struct {
	addr        string
	maxBodySize int64
	cache       *clientCache
}

func newServer() *Server {
	addr := defaultAddr
	maxBody := int64(defaultMaxBodySize)
	cap := defaultClientCacheCap
	if v := os.Getenv("REDIS_BRIDGE_ADDR"); v != "" {
		addr = v
	}
	if v := os.Getenv("REDIS_BRIDGE_MAX_BODY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxBody = int64(n)
		}
	}
	if v := os.Getenv("REDIS_BRIDGE_CACHE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cap = n
		}
	}
	return &Server{addr: addr, maxBodySize: maxBody, cache: newClientCache(cap)}
}

func (s *Server) getClient(redisURL string) (*redis.Client, error) {
	if c, ok := s.cache.get(redisURL); ok {
		return c, nil
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis url: %v", err)
	}
	opts.DialTimeout = 5 * time.Second
	opts.ReadTimeout = 5 * time.Second
	opts.WriteTimeout = 5 * time.Second
	c := redis.NewClient(opts)

	canonical, displaced := s.cache.getOrPut(redisURL, c)
	if displaced != nil {
		if canonical == c {
			// We inserted and bumped an older entry out of the cache. Defer Close
			// so concurrent in-flight requests on the evicted pool can finish.
			time.AfterFunc(evictedCloseDelay, func() { _ = displaced.Close() })
		} else {
			// Lost the insert race; our newly created client has no in-flight work.
			_ = displaced.Close()
		}
	}
	return canonical, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}

func (s *Server) redisErr(w http.ResponseWriter, op, key string, err error) {
	fmt.Fprintf(os.Stderr, "ERROR redis op=%s key=%s err=%v\n", op, key, err)
	writeError(w, http.StatusServiceUnavailable, "redis error")
}

func parseTTL(r *http.Request) (time.Duration, error) {
	v := r.URL.Query().Get("ttl")
	if v == "" {
		return 0, nil
	}
	secs, err := strconv.Atoi(v)
	if err != nil || secs <= 0 {
		return 0, errors.New("ttl must be a positive integer (seconds)")
	}
	return time.Duration(secs) * time.Second, nil
}

func (s *Server) prep(w http.ResponseWriter, r *http.Request) (*redis.Client, string, bool) {
	redisURL := r.URL.Query().Get("url")
	if redisURL == "" {
		writeError(w, http.StatusBadRequest, "url query param is required (e.g. redis://host:6379)")
		return nil, "", false
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key query param is required")
		return nil, "", false
	}
	c, err := s.getClient(redisURL)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return nil, "", false
	}
	return c, key, true
}

func (s *Server) readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	// MaxBytesReader (vs io.LimitReader) errors on overflow instead of silently
	// truncating, so we never write a partial payload to Redis as if it were complete.
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodySize)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytes *http.MaxBytesError
		if errors.As(err, &maxBytes) {
			writeError(w, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("request body exceeds %d bytes", s.maxBodySize))
			return nil, false
		}
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return nil, false
	}
	if len(body) == 0 {
		writeError(w, http.StatusBadRequest, "request body (value) is required")
		return nil, false
	}
	return body, true
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	client, key, ok := s.prep(w, r)
	if !ok {
		return
	}
	ttl, err := parseTTL(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}

	// Atomic set-if-not-exists; one round trip, no TOCTOU race.
	created, err := client.SetNX(r.Context(), key, body, ttl).Result()
	if err != nil {
		s.redisErr(w, "SETNX", key, err)
		return
	}
	if !created {
		writeError(w, http.StatusConflict, fmt.Sprintf("key '%s' already exists", key))
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	client, key, ok := s.prep(w, r)
	if !ok {
		return
	}

	ctx := r.Context()
	// GET + TTL in a single round trip.
	pipe := client.Pipeline()
	getCmd := pipe.Get(ctx, key)
	ttlCmd := pipe.TTL(ctx, key)
	_, _ = pipe.Exec(ctx)

	val, err := getCmd.Bytes()
	if errors.Is(err, redis.Nil) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("key '%s' not found", key))
		return
	}
	if err != nil {
		s.redisErr(w, "GET", key, err)
		return
	}

	ttl, _ := ttlCmd.Result()
	if ttl > 0 {
		w.Header().Set("X-Redis-TTL", strconv.Itoa(int(ttl.Seconds())))
	} else {
		w.Header().Set("X-Redis-TTL", "-1")
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(val)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(val)
}

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	client, key, ok := s.prep(w, r)
	if !ok {
		return
	}
	ttl, err := parseTTL(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	body, ok := s.readBody(w, r)
	if !ok {
		return
	}

	// Atomic set-if-exists; one round trip, no TOCTOU race.
	updated, err := client.SetXX(r.Context(), key, body, ttl).Result()
	if err != nil {
		s.redisErr(w, "SETXX", key, err)
		return
	}
	if !updated {
		writeError(w, http.StatusNotFound, fmt.Sprintf("key '%s' not found", key))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePatchTTL(w http.ResponseWriter, r *http.Request) {
	client, key, ok := s.prep(w, r)
	if !ok {
		return
	}
	ttl, err := parseTTL(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if ttl == 0 {
		writeError(w, http.StatusBadRequest, "ttl query param is required and must be > 0")
		return
	}

	// EXPIRE returns false iff the key does not exist; one round trip.
	got, err := client.Expire(r.Context(), key, ttl).Result()
	if err != nil {
		s.redisErr(w, "EXPIRE", key, err)
		return
	}
	if !got {
		writeError(w, http.StatusNotFound, fmt.Sprintf("key '%s' not found", key))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	client, key, ok := s.prep(w, r)
	if !ok {
		return
	}

	// DEL returns number of keys removed; one round trip.
	n, err := client.Del(r.Context(), key).Result()
	if err != nil {
		s.redisErr(w, "DEL", key, err)
		return
	}
	if n == 0 {
		writeError(w, http.StatusNotFound, fmt.Sprintf("key '%s' not found", key))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/redis", s.dispatch)
	return mux
}

func (s *Server) dispatch(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.handleCreate(w, r)
	case http.MethodGet:
		s.handleRead(w, r)
	case http.MethodPut:
		s.handleUpdate(w, r)
	case http.MethodPatch:
		s.handlePatchTTL(w, r)
	case http.MethodDelete:
		s.handleDelete(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func main() {
	srv := newServer()

	httpSrv := &http.Server{
		Addr:         srv.addr,
		Handler:      srv.routes(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		fmt.Fprintf(os.Stderr, "INFO redis bridge listening addr=%s\n", srv.addr)
		if err := httpSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "ERROR server failed err=%v\n", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)

	srv.cache.closeAll()
	fmt.Fprintln(os.Stderr, "INFO server stopped")
}
