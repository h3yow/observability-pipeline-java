# 指标模型、时间窗口、告警状态与降级语义

本文档说明本管线的数据模型、受理结论、窗口聚合、告警状态机、基数与速率降级规则，
以及结论可复现性的保证方式。

## 1. 指标模型

一次上报的样本 `MetricSample`：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `metric` | string | 指标名，非空 |
| `labels` | map<string,string> | 维度标签；键非空。标签集在内部按 key 排序规范化，提交顺序不影响序列归属 |
| `value` | double | 必须为有限值（NaN / ±Infinity 拒绝） |
| `timestamp` | long | epoch 毫秒 |

**序列（Series）** = `metric + 规范化标签集`（`SeriesKey`）。
**窗口聚合 `WindowAggregate`** 针对 `(序列, 窗口)`，输出
`count / sum / min / max / avg / lastTimestamp`。

## 2. 时间窗口与迟到、乱序、重复

### 2.1 固定对齐滚动窗口

- 窗口起点：`windowStart = floor(timestamp / windowSize) * windowSize`（对负时间戳同样 floor）。
- 窗口按 epoch 边界对齐：同一时间戳在任何时刻都归属同一窗口，与到达时刻无关。
- 生命周期：
  1. **开窗口**：`now < windowEnd`，正常受理；
  2. **封板 + 宽限期**：`windowEnd <= now <= windowEnd + gracePeriod`，窗口不再接受“正常”写入，
     但仍受理**迟到样本**（`ACCEPTED_LATE, late=true`）并重算聚合；
  3. **关闭**：`now > windowEnd + gracePeriod`，迟到样本拒绝（`WINDOW_CLOSED`）。

### 2.2 受理结论（互不静默丢弃）

| 情况 | 结论 | 标记/原因 |
| --- | --- | --- |
| 落入开窗口 | `ACCEPTED` | — |
| 样本时间戳早于该序列已受理最大时间戳 | `ACCEPTED`/`ACCEPTED_LATE` | `outOfOrder=true`（仍受理） |
| 封板宽限期内到达 | `ACCEPTED_LATE` | `late=true` |
| 宽限期之后到达 | `REJECTED` | `WINDOW_CLOSED` |
| 同窗口内 (时间戳, 值) 完全相同 | `REJECTED` | `DUPLICATE` |
| 时间戳 > now + 钟偏 | `REJECTED` | `TIMESTAMP_IN_FUTURE` |
| 时间戳早于保留期下界 | `REJECTED` | `TIMESTAMP_TOO_OLD` |
| 指标名为空 / 标签键为空 / 数值非有限 | `REJECTED` | `EMPTY_METRIC_NAME` / `EMPTY_LABEL_KEY` / `NON_FINITE_VALUE` |
| 超单指标/全局基数 | `REJECTED` | `CARDINALITY_LIMIT` / `GLOBAL_CARDINALITY_LIMIT` |
| 超每秒速率 | `REJECTED` | `RATE_LIMITED` |

迟到与乱序是两个**可同时成立**的受理标记；重复按“同窗口相同时间戳+相同值”判定，
而“同时间戳不同值”视为不同样本。

### 2.3 聚合可复现性

`WindowBucket` 内保存样本集合，聚合时先按 `(时间戳, 值, 到达序号)` 全排序再顺序累积：
聚合值**只依赖窗口内样本集合，与到达顺序无关**。因此乱序写入、宽限期内迟到并入后重算，
得到的 `count/sum/min/max/avg` 与样本本就按序到达完全一致（确定性求和顺序，舍入也确定）。

## 3. 告警状态机

规则 `AlertRule`：

```
ruleId, metric, aggregation(SUM|AVG|COUNT|MIN|MAX),
op(GT|GTE|LT|LTE|EQ), threshold,
consecutiveFires(连续满足多少个窗口才触发),
labelFilter(只对匹配该标签子集的序列生效，空=全部)
```

每个 **(ruleId, 标签维度)** 独立维护一个状态实例：

```
                 条件满足(首窗口)                 连续满足达 consecutiveFires
  INACTIVE ───────────────────────► PENDING ───────────────────────────────► FIRING
     ▲                                │  │                                      │
     │                                │  └──────────── 条件不满足 ────────────┐ │
     │                                ▼                                       │ │
     │                             INACTIVE（清零，绝不误报）                  │ │
     │                                                                        │ │
     │                                            条件不满足                   │ │
     │             FIRING ───────────────────────────────────────► RESOLVED   │ │
     │               ▲                                                  │     │ │
     └───────────────┴────────────── 下一窗口评估自动回 INACTIVE ◄────────┘     │ │
                                                                              │ │
              恢复后再次连续满足条件 => 新的 FIRING 事件（新 eventSeq）◄────────┘ │
```

语义要点：

- **未触发 INACTIVE → 待确认 PENDING**：条件第一次满足只计数，不产生事件。
- **防误报**：PENDING 期间任意一个窗口条件不满足，立即回 INACTIVE 并清零连续计数。
  仅当**连续** `consecutiveFires` 个窗口都满足才产生 `FIRING` 事件。
  `consecutiveFires=1` 时首窗口直接 FIRING。
- **维度去重**：FIRING 状态下条件继续满足不重复产生事件；不同标签维度各自独立
  FIRING/RESOLVED，互不影响。
- **恢复**：FIRING 后条件不满足，产生 `RESOLVED` 事件，状态置 RESOLVED（可查询的瞬态），
  下一个窗口评估自动回到 INACTIVE。
- **恢复后再触发**：再次经历 PENDING→FIRING 时产生**新的、eventSeq 单调递增的 FIRING 事件**，
  与上一次触发可区分。
- **评估时机与幂等**：只评估“已封板且超出宽限期”的窗口（`MaintenanceService.tick`），
  避免晚到样本导致结论翻转；同一窗口重复评估幂等。规则与维度均按字典序处理，
  因此相同输入序列总产生相同事件顺序。

事件 `AlertEvent` 字段：`eventSeq, ruleId, metric, labels, type(FIRING|RESOLVED),
windowStart, observedValue, threshold, reason, eventTime`。

## 4. 基数与速率降级

保护目标：高基数标签或突发流量只能导致**显式拒绝 + 可观测降级**，不能撑爆内存或拖垮判定。

### 4.1 序列基数

- 上限：`max-series-per-metric`（单指标）、`max-series-global`（全局）。
- 计量口径：**保留期内出现过的序列**。新序列落桶前在临界区内判定，并发下不会突破上限；
  已存在序列继续可写，保证存活流量不受新出现的高基数标签影响。
- 超限返回 `CARDINALITY_LIMIT` / `GLOBAL_CARDINALITY_LIMIT`，并在 `/admin/stats` 的
  `degraded` 中置 `cardinality.per-metric` / `cardinality.global`（含首次出现时间）。
- **自动恢复**：窗口超过 `retention-millis` 被回收，当序列的全部窗口都过期时注销序列、
  释放计数；计数回落到限内后，下一次维护 tick 自动清除降级标记，新序列恢复受理。

### 4.2 上报速率

- 固定秒窗口计数 `max-samples-per-second`（0 不限）；超限返回 `RATE_LIMITED`，
  置 `rate` 降级标记。
- 秒窗口每翻篇自动重置配额；tick 发现用量回落即清除降级标记——压力消失即恢复。

`GET /api/v1/admin/stats` 返回 `accepted / acceptedLate / outOfOrder / duplicate /
rejected{原因->次数} / activeSeries / rateUsedThisSecond / degraded{原因->起始时间}`。

## 5. 并发与确定性

- `WindowStore` 使用 `ConcurrentHashMap` 分序列存储，新序列注册在对象临界区内完成基数判定，
  单个 bucket 写入与聚合在 bucket 锁内进行；`AlertStateStore` 的每个状态条目在自身锁内迁移。
- 全部时间相关组件依赖 `Clock` 抽象：生产为系统时钟 + 每秒定时 tick；测试注入 `TestClock`，
  时间推进与封板评估全部由测试显式驱动，因此测试结论稳定、可复现（不依赖真实 sleep）。
- 事件内存保存在 `EventJournal`，配置路径后同时以 JSON Lines 追加落盘，供本地持久核对。

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/metrics/samples` | 单条上报，回显输入与判定结论 |
| POST | `/api/v1/metrics/samples:batch` | 批量上报，逐条结论 + 受理/拒绝小计 |
| GET | `/api/v1/metrics/{metric}/aggregates?from=&to=` | 时间范围内窗口聚合 |
| POST | `/api/v1/alerts/rules` | 创建规则 |
| GET | `/api/v1/alerts/rules` | 规则列表 |
| GET | `/api/v1/alerts/states` | 规则维度状态快照 |
| GET | `/api/v1/alerts/events?ruleId=` | 告警事件（可按规则过滤） |
| GET | `/api/v1/admin/stats` | 计数、拒绝原因、降级状态、基数、速率 |
| POST | `/api/v1/admin/tick` | 手动触发一次封板评估 + 回收 + 降级刷新 |

## 7. 本地验证

```bash
mvn test
```

测试覆盖：

- `WindowAggregationTest`：窗口 epoch 对齐；乱序/迟到下聚合确定性复现；宽限期迟到受理。
- `IngestValidationTest`：各类异常输入的可区分拒绝原因；重复计数；乱序标记；超宽限期关闭。
- `ConcurrentIngestTest`：8 线程并发写入下不丢不重、聚合闭合；并发新序列不突破基数上限。
- `AlertStateMachineTest`：PENDING 防误报、FIRING 去重、RESOLVED 恢复、恢复后再触发新事件、
  多维度独立去重。
- `CardinalityDegradationTest`：单指标/全局基数拒绝与降级标记、回收后恢复受理。
- `RateLimitTest`：秒级限流拒绝、降级标记、秒翻篇后自动恢复。
- `MaintenanceTickTest`：仅靠时钟 + tick 自动按序补齐多窗口评估，事件确定且可重放复现。
- `EventJournalTest`：事件序号单调与 JSON Lines 落盘。
- `WebApiIntegrationTest`：HTTP 端到端冒烟。

测试日志使用 SLF4J 打印每个输入样本与其 `status/reason/window/late/outOfOrder/detail`，
以及每次状态迁移事件的 `observedValue/threshold/reason`，可直接人工核对判定依据。
