# syntax=docker/dockerfile:1
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/... && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
# A named volume mounted here starts out with this ownership.
COPY --from=build --chown=nonroot:nonroot /data /data
EXPOSE 7000
ENTRYPOINT ["/usr/local/bin/kvnode"]
CMD ["-listen", ":7000", "-data", "/data"]
