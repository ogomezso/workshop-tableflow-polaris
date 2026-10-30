#!/usr/bin/env bash
# Builds the Polaris Console (apache/polaris-tools has no released image) and loads it into kind.
# The Console runs an nginx that serves the UI and proxies /api/ to Polaris, so the browser only talks to one origin (no CORS changes).
set -euo pipefail
cd "$(dirname "$0")/.."
source env.sh

REF=${POLARIS_TOOLS_REF:-95f150582c935f7e522a86301024aef9154bcfba}      # pinned commit of apache/polaris-tools; override to try another
DIR=.cache/polaris-tools

[[ -d $DIR/.git ]] || { mkdir -p .cache; git init -q "$DIR"; git -C "$DIR" remote add origin https://github.com/apache/polaris-tools; }
git -C "$DIR" fetch -q --depth 1 origin "$REF"
git -C "$DIR" checkout -q -f FETCH_HEAD        # -f also discards the nginx.conf edit of a previous build

cat >> "$DIR/console/docker/nginx.conf" <<'NGINX'

    # Same-origin proxy to Polaris inside the kind cluster (added by scripts/build-console.sh)
    location /api/ { proxy_pass http://polaris.polaris.svc.cluster.local:8181; }
NGINX

docker build -f "$DIR/console/docker/Dockerfile" -t polaris-console:local "$DIR/console"
kind load docker-image polaris-console:local --name "$KIND_CLUSTER"
