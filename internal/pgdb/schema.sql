-- 对象清单：一个对象一行，发布即代表 D+P 个分片已全部持久化并校验。
CREATE TABLE IF NOT EXISTS objects (
    id            TEXT PRIMARY KEY,
    size          BIGINT      NOT NULL CHECK (size >= 0),
    data_shards   INT         NOT NULL CHECK (data_shards > 0),
    parity_shards INT         NOT NULL CHECK (parity_shards > 0),
    block_size    INT         NOT NULL CHECK (block_size > 0),
    stripes       INT         NOT NULL CHECK (stripes >= 0),
    -- 最后一个数据块末尾补齐的零字节数（0 表示无需补齐；空对象也是 0）
    tail_padding  INT         NOT NULL CHECK (tail_padding >= 0),
    sha256        TEXT        NOT NULL, -- 原始对象内容 sha256（不含补齐零）
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 分片布局 + 逐块校验值：每个 (对象, 条带, 节点) 对应一个定长块。
-- sha256 针对的是节点上的实际块字节（条带内补零也参与），
-- 因此任何静默位翻转都会被发现。
CREATE TABLE IF NOT EXISTS shard_blocks (
    object_id TEXT NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    stripe    INT  NOT NULL CHECK (stripe >= 0),
    node      INT  NOT NULL CHECK (node >= 0),
    size      INT  NOT NULL CHECK (size > 0),
    sha256    TEXT NOT NULL,
    PRIMARY KEY (object_id, stripe, node)
);

CREATE INDEX IF NOT EXISTS idx_shard_blocks_object ON shard_blocks (object_id);
