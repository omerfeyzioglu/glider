# Build the server and admin tool. See README "Quickstart", "Docker Compose".
FROM rust:1.98.1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
COPY examples ./examples
COPY tests ./tests
RUN cargo build --locked --release --features server --bin glider-server --bin glider-admin

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /var/lib/glider glider \
    && mkdir -p /var/lib/glider/cache \
    && chown -R glider /var/lib/glider
COPY --from=build /src/target/release/glider-server /usr/local/bin/glider-server
COPY --from=build /src/target/release/glider-admin /usr/local/bin/glider-admin
USER glider
ENV GLIDER_LISTEN=0.0.0.0:8080 \
    GLIDER_CACHE_DIR=/var/lib/glider/cache
EXPOSE 8080
ENTRYPOINT ["glider-server"]
