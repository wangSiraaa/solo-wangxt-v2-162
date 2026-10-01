#!/usr/bin/env bash
# 运行全部测试：不依赖 DB 的纯单元测试始终执行；
# 集成测试通过 EC_TEST_DATABASE_URL 指向一个已存在的管理库（脚本自动确保它就绪），
# 每个用例自建/自删独立数据库。
set -euo pipefail
cd "$(dirname "$0")/.."

export EC_TEST_DATABASE_URL="${EC_TEST_DATABASE_URL:-postgres://ecuser@127.0.0.1:5432/postgres?sslmode=disable}"
bash scripts/dev-postgres.sh

if [ -x "$HOME/sdk/go/bin/go" ]; then export PATH="$HOME/sdk/go/bin:$PATH"; fi
export GOTOOLCHAIN=local

echo ">> go test ./... (含 -race 与真实 PostgreSQL 集成测试)"
go test -race -count=1 ./...
