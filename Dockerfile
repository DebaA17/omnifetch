FROM golang:1.25-alpine AS build

WORKDIR /src

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG OMNIFETCH_VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X omnifetch/internal/version.Current=${OMNIFETCH_VERSION}" -o /out/omnifetch ./cmd/omnifetch

FROM alpine:3.20

RUN apk add --no-cache ca-certificates && adduser -D -g '' omnifetch \
	&& mkdir -p /home/omnifetch/downloads \
	&& chown -R omnifetch:omnifetch /home/omnifetch

ENV HOME=/home/omnifetch

WORKDIR /home/omnifetch

COPY --from=build /out/omnifetch /usr/local/bin/omnifetch

USER omnifetch

ENTRYPOINT ["/usr/local/bin/omnifetch"]