FROM golang:1.27.3-alpine3.23 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/control-plane ./cmd/control-plane \
    && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM alpine:3.23

RUN apk add --no-cache ca-certificates \
    && addgroup -S app \
    && adduser -S -G app app
COPY --from=build /out/control-plane /usr/local/bin/control-plane
COPY --from=build /out/worker /usr/local/bin/worker
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/control-plane"]
