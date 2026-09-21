# One image holds all three binaries; docker-compose picks the command.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/server ./cmd/worker ./cmd/client ./cmd/netbench

FROM alpine:3.22
# iproute2 provides tc, used by netem/entry.sh to impair the network.
RUN apk add --no-cache iproute2
COPY --from=build /out/ /usr/local/bin/
COPY netem/entry.sh /usr/local/bin/netem-entry.sh
