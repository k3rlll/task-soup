FROM golang:1.24-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /soup ./cmd/soup

FROM alpine:3.21
RUN adduser -D -u 10001 soup

USER soup
COPY --from=build /soup /soup

ENTRYPOINT ["/soup"]
