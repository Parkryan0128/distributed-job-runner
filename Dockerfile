FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /runner ./cmd/runner

FROM alpine:3.23
RUN apk add --no-cache ca-certificates && addgroup -S runner && adduser -S runner -G runner
WORKDIR /app
COPY --from=build /runner /usr/local/bin/runner
COPY web ./web
USER runner
EXPOSE 8080
ENTRYPOINT ["runner"]
CMD ["api"]
