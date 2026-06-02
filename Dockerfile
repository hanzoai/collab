# Stage 1 — static Go build.
FROM golang:1.26-alpine AS build

RUN apk add --no-cache git ca-certificates tzdata
RUN addgroup -g 65532 -S nonroot && adduser -u 65532 -S nonroot -G nonroot
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG VERSION=dev
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN go build -ldflags="-s -w -X main.Version=${VERSION}" -o /collab ./cmd/collab

# Stage 2 — scratch runtime.
FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /etc/group /etc/group

WORKDIR /app
COPY --from=build /collab /app/collab

EXPOSE 3078
USER 65532:65532
ENTRYPOINT ["/app/collab"]
