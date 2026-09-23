# observability-pipeline-java

本地内存 + 本地存储承载的**可观测性指标采集与告警判定**管线（Spring Boot 3 / Java 21）。
支持多来源指标上报、时间窗口聚合、可解释的受理/拒绝结论、规则状态机告警、
标签基数与上报速率的降级保护。

完整语义说明见 [`docs/OBSERVABILITY.md`](docs/OBSERVABILITY.md)。

## 能力一览

- **指标受理**：`POST /api/v1/metrics/samples`（单条）、`POST /api/v1/metrics/samples:batch`（批量）。
  迟到、乱序样本照常受理并打标；重复、非法时间戳/数值/标签、超基数、超速均返回
  **可区分的 `reason`**，绝不静默丢弃。
- **窗口聚合**：固定 epoch 对齐滚动窗口；封板后宽限期内仍可并入迟到样本并确定性重算，
  同一窗口聚合值可重复复现。查询：`GET /api/v1/metrics/{metric}/aggregates?from=&to=`。
- **告警判定**：规则状态机 `INACTIVE → PENDING → FIRING → RESOLVED`，
  条件未持续满足不误报；同规则按标签维度去重；恢复后再触发产生新事件。
  规则/状态/事件：`/api/v1/alerts/**`。
- **降级保护**：单指标与全局序列基数上限、每秒上报速率上限；超限拒绝且在
  `GET /api/v1/admin/stats` 中以可区分原因呈现，压力回落后自动恢复。
- **本地持久化**：告警事件可追加落盘为 JSON Lines（配置 `observability.event-log-path`）。

## 本地构建与验证

```bash
mvn test           # 19 个单测/集成测试，全部使用可控时钟，结论可复现
mvn spring-boot:run
```

测试日志会打印**输入样本与判定依据**（`status/reason/window/late/outOfOrder/detail`）
以及每次状态迁移产生的告警事件（`observed/threshold/reason`），便于人工核对。

### 手动冒烟（服务启动后）

```bash
# 1) 上报样本
curl -s -X POST localhost:8080/api/v1/metrics/samples \
  -H 'Content-Type: application/json' \
  -d '{"metric":"cpu","labels":{"host":"a"},"value":95,"timestamp":'$(date +%s%3N)'}'

# 2) 建规则：cpu 平均值 > 50 连续 1 个窗口触发
curl -s -X POST localhost:8080/api/v1/alerts/rules -H 'Content-Type: application/json' \
  -d '{"ruleId":"r1","metric":"cpu","aggregation":"AVG","op":"GT","threshold":50,"consecutiveFires":1}'

# 3) 封板推进（生产环境每秒自动执行；本地可用接口手动驱动）
curl -s -X POST localhost:8080/api/v1/admin/tick

# 4) 查看状态 / 事件 / 计数与降级
curl -s localhost:8080/api/v1/alerts/states
curl -s localhost:8080/api/v1/alerts/events
curl -s localhost:8080/api/v1/admin/stats
```

## 关键配置（`src/main/resources/application.properties`）

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `observability.window-size-millis` | 60000 | 滚动窗口长度 |
| `observability.grace-period-millis` | 30000 | 封板后迟到样本宽限期 |
| `observability.max-future-skew-millis` | 5000 | 允许的未来钟偏 |
| `observability.retention-millis` | 300000 | 窗口/序列保留时长，过期回收释放基数 |
| `observability.max-series-per-metric` | 100 | 单指标序列基数上限 |
| `observability.max-series-global` | 1000 | 全局序列基数上限 |
| `observability.max-samples-per-second` | 0 | 每秒受理上限（0 不限） |
| `observability.event-log-path` | 空 | 告警事件 JSON Lines 落盘路径 |
