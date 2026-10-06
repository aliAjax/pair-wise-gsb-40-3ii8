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
- `POST /api/offline/batch`：按现场时间幂等重放离线记录
- `POST /api/offline/resolve`：协调员裁决冲突项（`apply` / `discard`）
- `GET /api/offline/batches`：按批次查看已生效与待裁决事件
- `GET /api/incidents/{id}/timeline`

### 离线批次协议

船端离线期间记录资源占用（`assignment`）、区域撤回/资源撤回（`withdraw`）、
事件移交（`transfer`）、线索（`clue`）和现场备注（`timeline`），回连后整批补传。

- 每条事件必须带 `client_event_id` 与 ISO 8601 现场时间 `occurred_at`（支持时区，无时区按 UTC）。
- 合并时**按现场时间排序逐条重放，与上传顺序无关**；事件内可带
  `expected_asset_version` / `expected_version` 作为乐观版本。
- 成功项立即写入资源状态、搜索区域和时间线（时间线保留现场时间 `occurred_at`）。
- 与岸端改动冲突（版本不一致、资源已被占用、事件已结束等）的事件**原样保留为
  `conflict` 待裁决**，不会改动任何业务数据；格式错误事件标记为 `rejected`。
- 重试同一批次或在其它批次中重传同一事件均幂等：命中已处理事件直接返回原结果，
  不会重复占用资源。
- 协调员通过 `POST /api/offline/resolve` 对冲突项裁决：`apply` 强制以现场操作为准
  （自动释放原占用关系），`discard` 放弃该操作；批次内冲突清零后状态回到 `merged`。
- 页面按批次分组显示，每项标注已生效 / 待裁决 / 已拒绝 / 已放弃，冲突项可直接裁决。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整协调流程、重复报警、错误位置、资源并发占用、离线幂等和权限拒绝。

## 局限

身份依赖调用方传入的用户和角色头；坐标使用球面距离近似；不会自动计算漂移概率区；文件附件、气象服务、真实通信链路和地理围栏未包含在内。
