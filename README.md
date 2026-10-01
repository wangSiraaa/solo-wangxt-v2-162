# ecstore — 纠删码对象存储教学项目

用 **Go + [klauspost/reedsolomon](https://github.com/klauspost/reedsolomon) + PostgreSQL**
实现的教学级对象存储，无前端，只提供 HTTP/JSON API。
对象被编码为 D 个数据分片 + P 个校验分片，分布在由独立目录模拟的 D+P 个“节点”上；
对象清单、分片布局和**逐块校验值**存放在 PostgreSQL。

## 核心特性（对应需求逐条）

| 需求 | 实现 |
|---|---|
| 多独立目录模拟节点 | `EC_NODES_ROOT/node-00..node-(D+P-1)`，每个节点独立目录、独立 DOWN 标记 |
| Go reedsolomon 纠删码 | `internal/erasure`，条带(stripe)编码，D/P/块大小均可配置 |
| PostgreSQL 存清单/布局/校验值 | `objects`（清单 + 尾部补齐长度 + 全对象 sha256）与 `shard_blocks`（每条带×每节点一块的 sha256） |
| 数据/校验分片数可配置 | `EC_DATA_SHARDS`、`EC_PARITY_SHARDS`、`EC_BLOCK_SIZE` |
| 记录末尾补齐长度，恢复不把补零当原内容 | `objects.tail_padding`；按条带补零到 `D*BlockSize`，读取时按 `size` 截断 |
| 节点不可用与内容损坏分别检测 | 不可用=`DOWN`标记/IO错误/文件缺失；损坏=节点在线、字节能读但 sha256 不符 |
| 达到恢复条件才重建并逐块校验 | 有效块 ≥ D 时 RS `Reconstruct`；重建块再与 DB 权威 sha256 逐块比对 |
| 缺足够分片时明确不可恢复 | 返回 `UnrecoverableError`（好块数、损坏/不可用/缺失节点清单），repair 返回 409 |
| 对象上传 | `PUT /v1/objects/{id}`，先写临时分片→fsync→原子rename→**单事务发布清单** |
| 范围读取 | `GET /v1/objects/{id}` + `Range: bytes=`，206 分片响应；全量读做端到端 sha256 |
| 模拟故障 API | 节点 down/up、在线节点注入块损坏、在线节点删除整份分片 |
| 空对象 | 0 条带、0 分片、只发空清单，sha256 为空串哈希，可正常读/查健康 |
| 非整块长度 | 1 字节、块边界±1、跨条带对象均有自动化测试 |
| 恰好超过容错数量 | 恰好 P 个失效可读，P+1 个失效 409 且列出不可恢复详情 |
| 上传中断不发布无法读取的完整清单 | 断连清理临时分片；清单只在全部分片落盘后由单个事务提交 |

## 数据布局与“补零不当内容”

```
对象 (size 字节)
 └─ 条带 0: 逻辑 D*BlockSize 字节
     ├─ 数据块 0..D-1（不足部分补 0）
     └─ RS 编码 → 校验块 D..D+P-1
 └─ 条带 1 ...
```

- 每个**块**定长 `BlockSize`（块内补零）；最后一个条带逻辑不足 `D*BlockSize`
  时，整条尾部补零，补齐字节数记为 `tail_padding = D*BlockSize - size % (D*BlockSize)`。
- 节点 j 的分片文件 = 所有条带第 j 块的顺序拼接（一个块也不缺，长度固定）。
- 读取时按 `size` 截断：补零区永远不会返回给客户端；但补零字节**参与块 sha256**，
  在补零区发生的位翻转同样会被检出。

## 发布顺序：为什么不会出现“清单完整但读不出来”

1. 预留对象 ID（存在临时或正式分片即拒绝）；
2. 逐条带 RS 编码，块写入各节点 `tmp/<id>.tmp`（`O_APPEND` 顺序拼接）；
3. 每个临时分片 `fsync`；
4. `rename` 原子替换为 `shards/<id>`（此时数据库里还没有清单，对象对外不可见）；
5. **一个数据库事务**写入 `objects` 一行 + `shard_blocks` 全部行后提交。

任一步失败 → 回滚事务并删除本次的临时/正式分片。客户端中途断连同理，
并额外区分“正常 EOF”与“连接错误”——即使断连恰好发生在条带边界，
没有收到完整请求体也绝不发布清单。

## 故障检测模型

| 情形 | 检测方式 | health 中的状态 |
|---|---|---|
| 节点不可用（掉电/断网） | 节点目录存在 `DOWN` 标记文件；读路径返回 I/O 错误 | `unavailable`，`node_up=false` |
| 分片丢失（节点在线但文件没了） | 打开分片文件 `ENOENT` | `missing`，`node_up=true` |
| 内容损坏（静默 bit rot） | 节点在线、字节能读出，但块 sha256 ≠ DB 记录 | `corrupt` |
| 健康 | sha256 一致 | `ok` |

每个条带独立统计：好块 ≥ D 才能 `Reconstruct`；重建出的块必须通过 DB 中
权威 sha256 的逐块校验才允许写回。修复时写回后会再读回校验一次。

## API

| 方法与路径 | 说明 |
|---|---|
| `PUT /v1/objects/{id}` | 上传（请求体即对象内容；冲突 409；失败 5xx 且无清单） |
| `GET /v1/objects/{id}` | 全量读取（200，端到端 sha256 校验，响应头 `X-Object-SHA256`） |
| `GET /v1/objects/{id}` + `Range: bytes=start-end\|start-\|-suffix` | 范围读取（206，`Content-Range`）；不支持多区间 |
| `HEAD /v1/objects/{id}` | 元信息（大小、条带数、补齐长度、sha256） |
| `GET /v1/objects` | 对象清单 |
| `GET /v1/objects/{id}/health` | 只检测不改动：每节点 ok/corrupt/missing/unavailable 计数、每条带好块数、`recoverable/healthy`；不可恢复时 409 |
| `POST /v1/objects/{id}/repair` | 重建并写回在线节点的坏/缺块；下线节点跳过；不可恢复时 409 |
| `POST /v1/objects/{id}/faults/corrupt` `{"node":N,"stripe":S}` | 节点保持在线，翻转一块一字节 |
| `POST /v1/objects/{id}/faults/remove-shard` `{"node":N}` | 节点在线，删除其整份分片 |
| `POST /v1/nodes/{N}/down` `{"reason":"..."}` | 模拟节点不可用（创建 DOWN 标记） |
| `POST /v1/nodes/{N}/up` | 节点恢复（下线期间错过的修复需再调 repair） |
| `GET /healthz` | 进程存活 |

## 运行

需要 Go 1.23+ 与 PostgreSQL 14+。无 root 的 Debian arm64 机器可直接用脚本
（自动从 Debian 仓库下载便携版 PG 二进制并解压到家目录）：

```bash
# 1) 准备/启动 PostgreSQL（已有 PG 可跳过，直接 export EC_DATABASE_URL）
scripts/dev-postgres.sh

# 2) 启动服务
scripts/dev-run.sh
# 默认 :8080，D=4 P=2，block=64KiB，节点目录 ./nodes

# 3) 另一个终端：端到端演示（上传/两类故障/边界不可恢复/修复）
scripts/demo.sh

# 4) 全部测试（单元 + 真实 PG 集成，含 -race）
scripts/test.sh
```

环境变量：

| 变量 | 默认值 |
|---|---|
| `EC_LISTEN` | `:8080` |
| `EC_DATABASE_URL` | `postgres://ecuser@127.0.0.1:5432/ecstore?sslmode=disable` |
| `EC_NODES_ROOT` | `./nodes` |
| `EC_DATA_SHARDS` (D) | `4` |
| `EC_PARITY_SHARDS` (P) | `2` |
| `EC_BLOCK_SIZE` | `65536` |
| `EC_TEST_DATABASE_URL` | 仅测试用，指向一个已存在的管理库（如 postgres 库） |

## 快速手工示例

```bash
head -c 300000 /dev/urandom > o.bin
curl -sS -X PUT --data-binary @o.bin localhost:8080/v1/objects/a | jq
curl -sS -H 'Range: bytes=0-15' localhost:8080/v1/objects/a -i        # 206
curl -sS -X POST -d '{"reason":"断电"}' localhost:8080/v1/nodes/4/down
curl -sS -X POST -d '{"node":0,"stripe":0}' \
  localhost:8080/v1/objects/a/faults/corrupt
curl -sS localhost:8080/v1/objects/a/health | jq                      # 区分两类故障
curl -sS -X POST localhost:8080/v1/objects/a/repair | jq
```

## 代码结构

```
cmd/ecstore            进程入口（装配 + 优雅退出）
internal/config        环境变量配置（D/P/块大小校验）
internal/erasure       RS 条带编解码（补齐、条带数、重建）
internal/nodestore     目录节点：临时分片/原子提交/块读写/DOWN 标记/故障注入
internal/pgdb          清单与逐块校验值（schema 内嵌，单事务发布）
internal/service       上传发布、范围读取、逐块校验、健康检查、修复
internal/api           HTTP/JSON 路由（Go 1.22 ServeMux）
scripts/               PG 准备、启动、演示、测试脚本
```

## 教学项目的刻意简化

- 单副本元数据（PostgreSQL 自身的高可用不在讨论范围）；
- 修复由 API 显式触发，读取路径只在内存重建、不回写；
- 校验用 SHA-256（非性能优化的 CRC/批量摘要）；
- 上传全量经过内存条带缓冲（`D*BlockSize` 一条），不做后端分片流水线。
