# 海上搜救协调系统

标准库实现的独立协调原型，使用 SQLite 保存事件、搜救资源、搜索区域、线索、离线批次和时间线。

## 运行

要求 Python 3.11+（在当前 Python 3.9 环境也可运行）。

```bash
python3 app.py --init --seed
python3 app.py
```

默认地址为 `http://127.0.0.1:8206`，数据库默认为 `maritime_sar.db`。`--db`、`--host`、`--port` 可覆盖默认值。

## 主要接口

写操作使用 JSON，并需要 `X-User` 与 `X-Role` 请求头。角色包括 `coordinator`、`operator`、`field`、`analyst`、`viewer`。

- `GET /health`、`GET /api/state`
- `POST /api/incidents`：创建遇险事件并识别重复报警
- `POST /api/assets`：登记资源
- `POST /api/areas`：创建搜索区域
- `POST /api/assignments`：按能力、海况和航程分配资源
- `POST /api/clues`、`POST /api/clues/verify`
- `POST /api/assets/withdraw`：撤回资源并释放任务
- `POST /api/incidents/transfer`、`POST /api/incidents/close`
- `POST /api/offline/batch`：幂等合并离线记录
- `GET /api/offline/batches`：按批次查看已生效 / 待裁决 / 已拒绝
- `POST /api/offline/conflicts/resolve`：协调员裁决冲突（`apply`/`dismiss`，可 `force`）
- `GET /api/incidents/{id}/timeline`

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整协调流程、重复报警、错误位置、资源并发占用、离线幂等、按现场时间重放、冲突挂起与裁决和权限拒绝。

## 离线批次重放规则

船端离线终端把操作整批补传到 `POST /api/offline/batch`：

- 事件类型：`clue`（线索）、`timeline`（现场备注）、`assign`/`asset.assign`（资源占用）、`area.withdraw`（区域撤回）、`asset.withdraw`（资源撤回）、`transfer`/`incident.transfer`（事件移交）。
- 占用、撤回、移交必须带 `occurred_at`（ISO 8601，兼容 `Z`）。服务端**按现场时间排序逐条重放**，上传顺序不代表处置顺序；时间线用 `occurred_at` 归位。
- 成功项在同一事务内立即写入资源状态、搜索区域和时间线。岸端已改动同一资源（版本不一致）、已占用、已结束事件等状态冲突（409）不会被丢弃：原样进入 `offline_events`，状态为 `conflict`，批次为 `pending_review`。海况、能力、航程、缺字段等硬错误标记为 `rejected`。
- 幂等分两层：相同 `client_batch_id` 重试返回首次结果；相同 `client_event_id` 跨批次补传只引用既有 ledger 结论，绝不第二次占用资源或重复写时间线。
- 冲突由协调员在 `POST /api/offline/conflicts/resolve` 裁决：`dismiss` 维持岸端现状；`apply` 默认在岸端已有更新时要求 `force=true`，强制时以现场意图改派/撤回/移交（硬能力条件仍不放松）；已结束事件的线索只能 `force` 补录。
- 页面与 `GET /api/offline/batches` 按批次列出每个事件的 `merged`（已生效）、`conflict`（待裁决）、`rejected`（已拒绝）数量和明细。

## 局限

身份依赖调用方传入的用户和角色头；坐标使用球面距离近似；不会自动计算漂移概率区；文件附件、气象服务、真实通信链路和地理围栏未包含在内。
