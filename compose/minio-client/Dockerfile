# syntax=docker/dockerfile:1.7

FROM golang:1.24.8-alpine3.22 AS build

ARG MC_VERSION
ARG MC_COMMIT

RUN apk add --no-cache git
WORKDIR /src

RUN git clone --depth 1 --branch "$MC_VERSION" https://github.com/minio/mc.git . \
    && test "$(git rev-parse HEAD)" = "$MC_COMMIT" \
    && test "$(git tag --points-at HEAD)" = "$MC_VERSION"

# Tag date differs from commit date; use the release timestamp explicitly.
# Keep dependency verification enabled; cache downloads/compilation, not credentials.
RUN --mount=type=cache,id=ie212-mc-mod,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,id=ie212-mc-build,target=/root/.cache/go-build,sharing=locked \
    CGO_ENABLED=0 GOTOOLCHAIN=local go build -mod=readonly -trimpath \
    --ldflags "$(MC_RELEASE=RELEASE go run buildscripts/gen-ldflags.go 2025-08-13T08:35:41Z)" \
    -o /out/mc . \
    && /out/mc --version | grep -F "mc version $MC_VERSION (commit-id=$MC_COMMIT)"

# Init/smoke require /bin/sh and core utilities, so scratch is not suitable.
FROM alpine:3.22.1
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 1000 mc \
    && adduser -S -D -H -u 1000 -G mc mc
COPY --from=build /out/mc /usr/bin/mc
COPY --from=build /src/LICENSE /licenses/LICENSE
COPY --from=build /src/CREDITS /licenses/CREDITS

ENV HOME=/tmp
USER mc:mc
ENTRYPOINT ["mc"]

ARG PIPELINE_BUILD_SHA
LABEL com.japan-earthquake-etl.build-input-sha256=$PIPELINE_BUILD_SHA
