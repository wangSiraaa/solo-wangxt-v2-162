#!/usr/bin/env bash
# 启动 ecstore 服务（自动确保 PostgreSQL 就绪）。
set -euo pipefail
cd "$(dirname "$0")/.."

export EC_DATABASE_URL="${EC_DATABASE_URL:-postgres://ecuser@127.0.0.1:5432/ecstore?sslmode=disable}"
export EC_NODES_ROOT="${EC_NODES_ROOT:-./nodes}"
export EC_LISTEN="${EC_LISTEN:-:8080}"
export EC_DATA_SHARDS="${EC_DATA_SHARDS:-4}"
export EC_PARITY_SHARDS="${EC_PARITY_SHARDS:-2}"
export EC_BLOCK_SIZE="${EC_BLOCK_SIZE:-65536}"

if ! command -v psql >/dev/null 2>&1; then
  bash scripts/dev-postgres.sh >/dev/null
fi
if ! command -v pg_isready >/dev/null 2>&1 || ! pg_isready -h 127.0.0.1 -p 5432 >/dev/null 2>&1; then
  bash scripts/dev-postgres.sh >/dev/null
fi

if [ ! -x "$HOME/sdk/go/bin/go" ]; then
  echo "未找到 Go (需要 1.23+): https://go.dev/dl/" >&2
  exit 1
fi
export PATH="$HOME/sdk/go/bin:$PATH"
GOTOOLCHAIN=local go run ./cmd/ecstore
