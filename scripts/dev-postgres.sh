#!/usr/bin/env bash
# 教学环境辅助脚本：在用户目录里跑一个免安装的 PostgreSQL 实例（端口 55432）。
# 适用于没有 root、无法用系统服务管理器的环境：把 postgresql deb 解包到 $HOME/.local/pg。
#
# 用法:
#   scripts/dev-postgres.sh init     # 解包 deb（如需要）并 initdb
#   scripts/dev-postgres.sh start    # 启动并创建 erasure_store 数据库
#   scripts/dev-postgres.sh stop
#   scripts/dev-postgres.sh status
set -euo pipefail

PG_PORT="${PG_PORT:-55432}"
PGDATA="${PGDATA:-$HOME/pgdata}"
PG_PREFIX="$HOME/.local/pg"
PGBIN="$PG_PREFIX/usr/lib/postgresql/15/bin"
DEB_DIR="${DEB_DIR:-/tmp/pgdebs}"

init_local_pg() {
  if [ ! -x "$PGBIN/initdb" ]; then
    echo ">> $PGBIN/initdb 不存在，尝试从 Debian deb 包解包..."
    mkdir -p "$DEB_DIR" "$PG_PREFIX"
    base="https://deb.debian.org/debian/pool/main/p/postgresql-15"
    ver="15.18-0+deb12u1"
    (cd "$DEB_DIR" && {
      [ -f "postgresql-15_${ver}_arm64.deb" ] || curl -fSL -O "$base/postgresql-15_${ver}_arm64.deb"
      [ -f "postgresql-client-15_${ver}_arm64.deb" ] || curl -fSL -O "$base/postgresql-client-15_${ver}_arm64.deb"
      for f in *.deb; do dpkg-deb -x "$f" "$PG_PREFIX"; done
    })
  fi
  export LD_LIBRARY_PATH="$PG_PREFIX/usr/lib/aarch64-linux-gnu:${LD_LIBRARY_PATH:-}"
  [ -d "$PGDATA" ] || "$PGBIN/initdb" -D "$PGDATA" -U postgres --auth=trust --encoding=UTF8
}

case "${1:-}" in
  init)
    init_local_pg
    echo ">> 初始化完成: PGDATA=$PGDATA"
    ;;
  start)
    init_local_pg
    export LD_LIBRARY_PATH="$PG_PREFIX/usr/lib/aarch64-linux-gnu:${LD_LIBRARY_PATH:-}"
    "$PGBIN/pg_ctl" -D "$PGDATA" -o "-p $PG_PORT -k /tmp" -l "$PGDATA/server.log" start
    sleep 1
    "$PGBIN/psql" -h /tmp -p "$PG_PORT" -U postgres -tc \
      "SELECT 1 FROM pg_database WHERE datname='erasure_store'" | grep -q 1 \
      || "$PGBIN/psql" -h /tmp -p "$PG_PORT" -U postgres -c "CREATE DATABASE erasure_store"
    echo ">> PostgreSQL 已启动: /tmp:$PG_PORT, 数据库 erasure_store"
    ;;
  stop)
    export LD_LIBRARY_PATH="$PG_PREFIX/usr/lib/aarch64-linux-gnu:${LD_LIBRARY_PATH:-}"
    "$PGBIN/pg_ctl" -D "$PGDATA" stop
    ;;
  status)
    export LD_LIBRARY_PATH="$PG_PREFIX/usr/lib/aarch64-linux-gnu:${LD_LIBRARY_PATH:-}"
    "$PGBIN/pg_isready" -h /tmp -p "$PG_PORT" -U postgres
    ;;
  *)
    echo "用法: $0 {init|start|stop|status}" >&2
    exit 2
    ;;
esac
