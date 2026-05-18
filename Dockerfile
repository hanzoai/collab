# Stage 1 — static Go build.
FROM golang:1.26-alpine AS build

RUN apk add --no-cache git
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG VERSION=dev
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN go build -ldflags="-s -w -X main.Version=${VERSION}" -o /collab ./cmd/collab

# Stage 2 — distroless runtime.
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=build /collab /app/collab

EXPOSE 3078
USER nonroot:nonroot
ENTRYPOINT ["/app/collab"]
