# One image holds all three binaries; docker-compose picks the command.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/server ./cmd/worker ./cmd/client

FROM alpine:3.22
COPY --from=build /out/ /usr/local/bin/
