# Indexer: the pouw-indexer binary on slim Debian, run by pouw-deploy as indexer.
# curl is there for the compose healthcheck. The SQL migrations are built into the
# binary and run on start. `pouw-indexer seed` loads tests/fixtures into the database.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY . .
# The cache mounts keep crates and target/ between local builds, the binary is
# copied out because target/ is not part of the image
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release \
    && cp target/release/pouw-indexer /usr/local/bin/

FROM debian:bookworm-slim
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home pouw
COPY --from=build /usr/local/bin/pouw-indexer /usr/local/bin/
WORKDIR /app
COPY tests/fixtures tests/fixtures
USER 10001
EXPOSE 8000
ENTRYPOINT ["pouw-indexer"]
CMD ["serve"]
