FROM golang:1.26.3-alpine AS builder

WORKDIR /app

RUN apk add --no-cache upx

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
RUN CGO_ENABLED=0 GOFLAGS=-mod=readonly \
    go build -trimpath -buildvcs=false -ldflags="-s -w" -o redis-bridge . \
 && upx --best --lzma redis-bridge

FROM scratch
USER 65534:65534
COPY --from=builder /app/redis-bridge /redis-bridge
EXPOSE 8087
ENTRYPOINT ["/redis-bridge"]
