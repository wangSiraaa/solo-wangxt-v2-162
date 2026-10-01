-- 对象清单表：只记录已完整发布的对象（上传中断时不会留下记录）
CREATE TABLE IF NOT EXISTS objects (
    id              TEXT PRIMARY KEY,              -- 内容 SHA-256 的十六进制摘要
    size            BIGINT      NOT NULL,          -- 原始对象字节数
    data_shards     INT         NOT NULL,
    parity_shards   INT         NOT NULL,
    shard_size      BIGINT      NOT NULL,          -- 每个分片补齐后的统一长度（reedsolomon 要求等长）
    padding         BIGINT      NOT NULL,          -- 对象末尾补齐的零字节数；读取时必须截掉，不能当成原文件内容
    checksum_sha256 TEXT        NOT NULL,          -- 原始对象整体 SHA-256
    status          TEXT        NOT NULL DEFAULT 'complete',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 分片布局与逐块校验值
CREATE TABLE IF NOT EXISTS shards (
    object_id   TEXT NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    shard_index INT  NOT NULL,                     -- 0..data-1 为数据分片，其后为校验分片
    node_id     INT  NOT NULL,
    kind        TEXT NOT NULL,                     -- 'data' | 'parity'
    size        BIGINT NOT NULL,                   -- 落盘分片字节数（== objects.shard_size）
    checksum_sha256 TEXT NOT NULL,                 -- 该分片内容 SHA-256，重建时逐块校验的依据
    PRIMARY KEY (object_id, shard_index)
);

CREATE INDEX IF NOT EXISTS idx_shards_object ON shards(object_id);

-- 模拟节点状态（目录本身始终存在；down 表示“节点不可用”，与分片内容损坏严格区分）
CREATE TABLE IF NOT EXISTS nodes (
    node_id  INT PRIMARY KEY,
    state    TEXT NOT NULL DEFAULT 'up',           -- 'up' | 'down'
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
