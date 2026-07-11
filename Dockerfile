# Multi-stage build for the kalshi-mm bot.
#
# ONNX Runtime is statically linked into the binary by `ort`'s
# download-binaries feature (fetched from GitHub at build time — the build
# needs network, which Docker/CI has), so the runtime image ships just the
# single binary plus the system C++/OpenMP libs it links against dynamically.

# --- build stage --------------------------------------------------------------
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
# --locked: build exactly the pinned Cargo.lock (reproducible releases).
RUN cargo build --release --locked

# --- runtime stage ------------------------------------------------------------
FROM debian:bookworm-slim
# libstdc++6 + libgomp1: ONNX Runtime's C++ core and its OpenMP thread pool.
# ca-certificates: TLS trust roots for the Kalshi REST/WebSocket endpoints.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Non-root; HOME=/app so the single-instance lock (~/.kalshi_mm_rust.lock) and
# the log file both land in a writable, mountable place.
RUN useradd --create-home --home-dir /app --uid 10001 kalshi
WORKDIR /app
ENV HOME=/app

COPY --from=build /src/target/release/kalshi-mm /usr/local/bin/kalshi-mm
# Deployment config is baked in; .env (secrets) and models/ (checkpoints) are
# mounted at run time — they are intentionally NOT part of the image.
COPY --from=build --chown=kalshi:kalshi /src/config /app/config

USER kalshi
ENTRYPOINT ["kalshi-mm"]
# Safe default: paper mode. Override for live, e.g.
#   docker run ... ghcr.io/heywoodwt/kalshi-mm:VERSION --config lowvol
CMD ["--config", "lowvol", "--paper"]
