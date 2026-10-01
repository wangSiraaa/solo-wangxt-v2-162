# 纠删码对象存储（教学项目）

用本地多个**目录**模拟相互独立的存储节点，使用
[`klauspost/reedsolomon`](https://github.com/klauspost/reedsolomon) 做 Reed-Solomon 纠删码，
PostgreSQL 保存**对象清单、分片布局和逐块校验值**。不包含前端。

## 它演示了什么

- **可配置的纠删码布局**：数据分片数 D 与校验分片数 P 可配置（环境变量，也可在每次上传时覆盖），
  对象 ID 为内容的 SHA-256。
- **补齐长度如实记录**：对象长度不是 `D` 的整数倍时，编码器在尾部补零。
  补零字节数存进 `objects.padding`，读取时严格按 `objects.size` 截断——
  补零永远不会被当成原文件内容返回，哪怕原文自己的尾部就是零字节。
- **两类故障分别检测**：
  - 节点不可用（`POST /nodes/{n}/down`，目录/文件原封不动）或文件缺失 → `unavailable`；
  - 节点在线、字节能读到，但长度不对或 SHA-256 不符 → `corrupt`。
- **达到恢复条件才重建，且逐块校验**：有效分片 `>= D` 时用 Reed-Solomon 重建所有缺失分片，
  每个**重建出的分片都要与清单中的校验值逐一核对**，通过后才原子回写节点。
- **有效分片不足时明确不可恢复**：有效分片 `< D` 返回 `UnrecoverableError`（HTTP 409），
  分别列出哪些分片不可用、哪些损坏，绝不返回拼凑出来的内容。
- **上传中断不会发布"读不出来的完整清单"**：分片先写暂存目录 → 自检 → rename 到最终位置 →
  再自检 → **最后**才在一个数据库事务里提交 `objects`+`shards`。任何一步失败，清单都不存在；
  即便分片已落盘而事务未提交，它们也只是无主孤儿文件，读取端查无此对象。

## 数据模型（PostgreSQL）

| 表 | 作用 |
|---|---|
| `objects` | 对象清单：`id`(=内容SHA-256)、`size`(原始大小)、`data_shards`、`parity_shards`、`shard_size`、**`padding`(末尾补齐长度)**、`checksum_sha256`(整体校验) |
| `shards` | 分片布局：每片一行，记录 `shard_index`、所在 `node_id`、`kind`(data/parity)、`size`、**`checksum_sha256`(逐块校验依据)** |
| `nodes` | 模拟节点状态：`up` / `down`（只改状态，不动目录文件） |

磁盘布局：

```
$STORAGE_ROOT/
├── node0/<id前2位>/<id>.shard0     # 数据分片0
├── node1/.../<id>.shard1
├── ...
├── nodeD/.../<id>.shardD           # 校验分片0
└── nodeD+P-1/.../<id>.shard(D+P-1)
```

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `HTTP_ADDR` | `:8080` | 监听地址 |
| `DATABASE_URL` | `host=/tmp port=55432 user=postgres dbname=erasure_store sslmode=disable` | pgx 连接串/关键字 DSN |
| `STORAGE_ROOT` | `./data` | 节点目录根 |
| `DATA_SHARDS` | `4` | 默认数据分片数 D |
| `PARITY_SHARDS` | `2` | 默认校验分片数 P（容错上限） |
| `MAX_OBJECT_BYTES` | `64MiB` | 对象大小上限 |

## 快速开始

```bash
# 1) 准备 PostgreSQL 并建库（已有实例时跳过脚本，直接 CREATE DATABASE erasure_store）
scripts/dev-postgres.sh init      # 在 $HOME/pgdata 初始化一个 55432 端口的本地实例
scripts/dev-postgres.sh start

# 2) 启动服务（自动建表、创建 node0..node(D+P-1) 目录）
DATABASE_URL='host=/tmp port=55432 user=postgres dbname=erasure_store sslmode=disable' \
  go run ./cmd/server
```

## HTTP API

| 方法与路径 | 说明 |
|---|---|
| `PUT /objects[?data_shards=&parity_shards=]` | 上传（请求体即对象字节）；成功返回内容定址 ID 与 padding |
| `GET /objects` | 对象清单 |
| `GET /objects/{id}` | 整对象读取 |
| `GET /objects/{id}` + `Range: bytes=start-end` | 范围读取（返回 206） |
| `GET /objects/{id}?start=&end=` | 范围读取（end 不含；省略 end 读到末尾） |
| `GET /objects/{id}/status` | 逐块检测，分别计数 `ok/unavailable/corrupt`，给出 `healthy/recoverable` |
| `POST /objects/{id}/repair` | 显式重建并回写；不可恢复时 409 |
| `DELETE /objects/{id}` | 删除（测试清理） |
| `GET /nodes` | 节点状态列表 |
| `POST /nodes/{n}/down` · `POST /nodes/{n}/up` | 模拟节点不可用 / 恢复 |
| `POST /objects/{id}/shards/{s}/corrupt` | body `{"mode":"flip"\|"truncate"\|"garbage"}`，模拟内容损坏 |
| `DELETE /objects/{id}/shards/{s}` | 模拟在线节点上的分片文件丢失 |

## 命令行演示

```bash
B=http://127.0.0.1:8080

# 上传一个非整块长度对象（4003 字节，D=4 => padding=1）
ID=$(curl -s -X PUT --data-binary @file.bin $B/objects | jq -r .id)

# 范围读取（Range 头或查询参数均可）
curl -s -H 'Range: bytes=4000-4002' $B/objects/$ID
curl -s "$B/objects/$ID?start=0&end=100"

# 制造故障：1 个节点不可用 + 1 片内容损坏
curl -s -X POST $B/nodes/5/down
curl -s -X POST -d '{"mode":"flip"}' -H 'Content-Type: application/json' \
  $B/objects/$ID/shards/0/corrupt
curl -s $B/objects/$ID/status        # 两类故障分别计数；有效 >= D 时 recoverable=true

# 再多坏一片（有效分片 < D）=> 明确不可恢复
curl -s -X POST -d '{"mode":"truncate"}' -H 'Content-Type: application/json' \
  $B/objects/$ID/shards/1/corrupt
curl -s -i $B/objects/$ID            # HTTP/1.1 409 + “不可恢复：需要 4 个有效分片，当前仅有 3 个 ...”

# 恢复节点 => 有效分片回到 D => 读取自动重建并逐块校验、回写
curl -s -X POST $B/nodes/5/up
curl -s $B/objects/$ID/repair        # healthy=true
```

## 测试

```bash
# 需要 PostgreSQL（默认连 host=/tmp port=55432 的 postgres 维护库，
# 会自动创建/删除一次性的测试库）：
go test ./...

# 竞态检测：
go test -race ./...
```

覆盖的场景：

1. 常规编码/落盘/读回往返，摘要一致；
2. **空对象**：无分片、清单存在、读回长度为 0；
3. **非整块长度**：padding 如实记录，补齐零被剔除、真实尾零被保留；
4. 范围读取（任意区间、越过尾部截断、起点 EOF、越界报错）；
5. 节点不可用与内容损坏分别识别（`unavailable` vs `corrupt`）；
6. 容错上限内（恰好 P 片失效）可重建，且**重建分片逐块校验**后回写；
7. 混合故障（down + 损坏 + 缺片）；
8. **恰好超过容错数量（P+1 片失效）**：明确 `UnrecoverableError`，Read/Status/Repair 全部拒绝，
   故障回落到边界后恢复可读；
9. **上传中断**（暂存后 / rename 后两个注入点）：不发布清单，孤儿分片对外不可见，重新上传成功可读。

## 代码结构

```
cmd/server/           HTTP 服务入口
internal/config/      环境变量配置
internal/db/          pgx 连接池 + 启动建表迁移（schema.sql）
internal/store/       纠删码核心：manager / put / get / status / faults
internal/api/         HTTP handler（对象、范围读取、故障注入）
```
