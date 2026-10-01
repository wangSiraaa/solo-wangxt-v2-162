#!/usr/bin/env bash
# 教学环境一键脚本：在用户目录准备便携版 PostgreSQL（Debian arm64 deb 解压，
# 无需 root），初始化并启动数据库集群，创建 ecstore 数据库。
# 已安装系统 postgres 的机器可跳过本脚本，直接导出 EC_DATABASE_URL。
set -euo pipefail

PG_VERSION=15.18
PG_REL="0+deb12u1"
DEB_REL="${PG_VERSION}-${PG_REL}"
SDK="$HOME/sdk"
PGBIN="$SDK/pgroot/usr/lib/postgresql/15/bin"
PGDATA="${EC_PGDATA:-$HOME/pgdata}"
PGPORT="${EC_PGPORT:-5432}"
PGHOST="${EC_PGHOST:-127.0.0.1}"
PGUSER="${EC_PGUSER:-ecuser}"
PGDB="${EC_PGDB:-ecstore}"

ARCH="$(dpkg --print-architecture 2>/dev/null || uname -m)"
case "$ARCH" in
  arm64|aarch64) DEB_ARCH=arm64 ;;
  amd64|x86_64) DEB_ARCH=amd64 ;;
  *) echo "不支持的架构: $ARCH（可手动安装 PostgreSQL 15+ 后设置 EC_DATABASE_URL）"; exit 1 ;;
esac

if [ ! -x "$PGBIN/postgres" ]; then
  echo ">> 下载并解压 PostgreSQL $PG_VERSION ($DEB_ARCH) deb 包到 $SDK/pgroot"
  work="$(mktemp -d)"
  base="http://deb.debian.org/debian/pool/main/p/postgresql-15"
  ( cd "$work"
    for pkg in postgresql-15_${DEB_REL}_${DEB_ARCH}.deb \
               postgresql-client-15_${DEB_REL}_${DEB_ARCH}.deb \
               libpq5_${DEB_REL}_${DEB_ARCH}.deb; do
      echo "   - $pkg"
      curl -fsSL -o "$pkg" "$base/$pkg"
    done
    mkdir -p "$SDK/pgroot"
    for pkg in *.deb; do dpkg-deb -x "$pkg" "$SDK/pgroot"; done
  )
  rm -rf "$work"
fi
export LD_LIBRARY_PATH="$SDK/pgroot/usr/lib/$DEB_ARCH-linux-gnu:${LD_LIBRARY_PATH:-}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo ">> initdb -> $PGDATA (user=$PGUSER, trust 本地连接)"
  mkdir -p "$HOME/pgrun"
  "$PGBIN/initdb" -D "$PGDATA" -U "$PGUSER" --auth=trust --encoding=UTF8 >/dev/null
  cat >> "$PGDATA/postgresql.conf" <<EOF
listen_addresses = '$PGHOST'
port = $PGPORT
unix_socket_directories = '$HOME/pgrun'
EOF
fi

if ! "$PGBIN/pg_isready" -h "$PGHOST" -p "$PGPORT" >/dev/null 2>&1; then
  echo ">> 启动 PostgreSQL ($PGHOST:$PGPORT)"
  "$PGBIN/pg_ctl" -D "$PGDATA" -l "$HOME/pg.log" start >/dev/null
  for _ in $(seq 1 30); do
    "$PGBIN/pg_isready" -h "$PGHOST" -p "$PGPORT" >/dev/null 2>&1 && break
    sleep 0.5
  done
fi

if ! PGPASSWORD= "$PGBIN/psql" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname='$PGDB'" | grep -q 1; then
  echo ">> 创建数据库 $PGDB"
  "$PGBIN/psql" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -c "CREATE DATABASE $PGDB"
fi

echo ">> PostgreSQL 就绪"
echo "   连接串: postgres://$PGUSER@$PGHOST:$PGPORT/$PGDB?sslmode=disable"
