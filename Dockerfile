# OBDient hub — runs the harvest seed daemon + the senior proxy in one container
# (see src/serve.mjs). Target: an always-on VM (GCP Compute Engine), NOT a
# scale-to-zero serverless runtime — the seed is a long-lived P2P daemon with
# persistent feeds under /app/data.

FROM node:22-bookworm-slim

# Build tools for native deps (sodium-native / hypercore) in case a prebuilt
# binary isn't available for the platform.
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps first for layer caching.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY . .

# Persistent P2P feeds + keys.json live here — mount a volume/disk on this path.
VOLUME ["/app/data"]

# Proxy HTTP port (the seed uses outbound P2P only; no inbound port to expose).
EXPOSE 8787

# NVIDIA_API_KEY must be provided at runtime (Secret Manager / env), never baked in.
CMD ["npm", "start"]
