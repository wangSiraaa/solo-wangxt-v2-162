#!/usr/bin/env bash
# 端到端演示：上传 → 健康检查 → 注入“节点不可用”和“内容损坏”两类故障
# → 达到恢复条件时读取/修复 → 超过容错数量时明确不可恢复。
set -euo pipefail
B="${B:-http://127.0.0.1:8080}"
ID="demo-$(date +%s)"

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
jq_or_cat() { if command -v jq >/dev/null; then jq .; else cat; fi; }

say "1) 上传一个 300KB 对象（D=4 P=2, block=64KB → 2 个条带，末尾补零被记录）"
head -c 300000 /dev/urandom > /tmp/ec-demo.bin
curl -sS -X PUT --data-binary @/tmp/ec-demo.bin "$B/v1/objects/$ID" | jq_or_cat

say "2) 健康检查（6 个节点全部 ok）"
curl -sS "$B/v1/objects/$ID/health" | jq_or_cat

say "3) 故障 A：节点 4 下线（不可用）；故障 B：节点 0 在线但一块内容被翻转（损坏）"
curl -sS -X POST -d '{"reason":"电源故障"}' "$B/v1/nodes/4/down" | jq_or_cat
curl -sS -X POST -d '{"node":0,"stripe":0}' "$B/v1/objects/$ID/faults/corrupt" | jq_or_cat

say "4) 有效块仍有 4(=D)，读取成功且字节一致；健康检查区分 unavailable 与 corrupt"
curl -sS "$B/v1/objects/$ID" -o /tmp/ec-demo.got
cmp /tmp/ec-demo.bin /tmp/ec-demo.got && echo "内容一致 OK"
curl -sS "$B/v1/objects/$ID/health" | jq_or_cat

say "5) 修复：节点 0 在线可写回；节点 4 仍下线，跳过（上线后再 repair）"
curl -sS -X POST "$B/v1/objects/$ID/repair" | jq_or_cat

say "6) 节点 4 恢复上线；其分片从未损坏，整体恢复健康"
curl -sS -X POST "$B/v1/nodes/4/up" | jq_or_cat
curl -sS "$B/v1/objects/$ID/health" | jq_or_cat

say "7) 超过容错数量：下线 1、5 再损坏 2 => 好块 3 < D=4，明确不可恢复"
curl -sS -X POST "$B/v1/nodes/1/down" >/dev/null
curl -sS -X POST "$B/v1/nodes/5/down" >/dev/null
curl -sS -X POST -d '{"node":2,"stripe":0}' "$B/v1/objects/$ID/faults/corrupt" >/dev/null
curl -sS "$B/v1/objects/$ID/health" | jq_or_cat || true
echo "-- repair 返回 409 与不可恢复详情："
curl -sS -X POST "$B/v1/objects/$ID/repair" | jq_or_cat || true

say "8) 清理故障节点状态（回到 6 个好块）"
for n in 1 5; do curl -sS -X POST "$B/v1/nodes/$n/up" >/dev/null; done
curl -sS -X POST "$B/v1/objects/$ID/repair" | jq_or_cat
echo "演示对象 ID: $ID"
