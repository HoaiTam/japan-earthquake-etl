# syntax=docker/dockerfile:1.7

FROM golang:1.24.8-alpine3.22 AS build

ARG MINIO_VERSION
ARG MINIO_COMMIT

RUN apk add --no-cache git

WORKDIR /src

RUN git clone --filter=blob:none --no-checkout https://github.com/minio/minio.git . \
    && git checkout --detach "$MINIO_COMMIT" \
    && test "$(git rev-parse HEAD)" = "$MINIO_COMMIT" \
    && test "$(git tag --points-at HEAD)" = "$MINIO_VERSION"

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -tags kqueue -trimpath \
    --ldflags "$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go)" \
    -o /out/minio . \
    && /out/minio --version | grep -F "$MINIO_VERSION"

FROM alpine:3.22.1

RUN apk add --no-cache ca-certificates curl \
    && addgroup -S -g 1000 minio \
    && adduser -S -D -H -u 1000 -G minio minio \
    && mkdir -p /data /licenses \
    && chown -R minio:minio /data

COPY --from=build /out/minio /usr/bin/minio
COPY --from=build /src/LICENSE /licenses/LICENSE
COPY --from=build /src/CREDITS /licenses/CREDITS

ENV HOME=/tmp

USER minio:minio

EXPOSE 9000 9001
VOLUME ["/data"]

ENTRYPOINT ["/usr/bin/minio"]
CMD ["server", "/data"]
