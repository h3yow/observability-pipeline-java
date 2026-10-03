# 本地可观测性指标采集与告警判定（observability-pipeline-java）

一个**纯本地**、无外部中间件依赖的指标采集 / 窗口聚合 / 告警判定 / 降级防护管线：
指标样本经 HTTP 入口写入内存聚合器，并以本地 JSONL 追加日志持久化；
聚合窗口到期后封存并驱动告警状态机；标签基数或上报速率超限时**降级拒绝而非崩溃**。
所有异常输入都有可区分的受理/拒绝结果，所有结论在并发、乱序、迟到、重放下**确定且可复现**。

---

## 1. 指标模型

### 1.1 样本（MetricSample）

| 字段 | 含义 | 约束 |
| --- | --- | --- |
| `metric` | 指标名 | 非空，仅允许 `[A-Za-z0-9_.-]`，长度 ≤ 256 |
| `labels` | 标签键值对 | 数量、键长、值长受配置限制；键非空 |
| `timestampMs` | epoch 毫秒时间戳 | 可空（服务端补当前时间）；必须为正 |
| `value` | 数值 | 有限数（拒绝 NaN / ±Infinity） |
| `sourceId` | 上报来源 | 多来源区分；为空时记为 `anonymous` |

**序列（series）= 指标名 + 规范化标签集合的指纹**。
标签规范化规则：键值裁剪首尾空白、null 值归空串、按键排序，再对规范串取
SHA-256 十六进制指纹（空标签集合使用稳定常量 `none`）。
因此同一逻辑序列无论标签书写顺序如何都只有一个指纹；**基数统计按序列去重，
同一规则的告警也按序列维度去重，不按上报次数堆积**。

### 1.2 受理结果（IngestResult）

任何输入都返回结构化结果，**绝不静默丢弃**：

| status | 含义 | 是否计入聚合 |
| --- | --- | --- |
| `ACCEPTED` | 正常受理，写入当前开放窗口 | 是 |
| `ACCEPTED_LATE` | 迟到样本，补入已关闭但仍在宽限期内的窗口 | 是 |
| `DUPLICATE` | 与窗口内已有样本完全重复（序列+时间戳+数值+来源） | 否 |
| `INVALID_METRIC` / `INVALID_VALUE` / `INVALID_LABELS` / `INVALID_TIMESTAMP` | 输入非法 | 否 |
| `TOO_OLD` | 超出迟到宽限/保留边界（或窗口刚被封存的竞争） | 否（竞争情形仍落盘可追溯） |
| `TOO_FAR_FUTURE` | 时间戳超过 `now + futureSkewMs` | 否 |
| `CARDINALITY_LIMIT` | 单指标序列基数超限 | 否 |
| `GLOBAL_CARDINALITY_LIMIT` | 全局序列基数超限 | 否 |
| `RATE_LIMITED` | 上报速率超过令牌桶配额 | 否 |

结果中同时携带 `seriesKey`、`windowStartMs`、`detail`（判定依据）与
`receiveSeq`（全局接收序号），便于人工核对。

### 1.3 聚合函数

窗口内支持 `COUNT / SUM / AVG / MIN / MAX / LAST`：

- `COUNT/SUM/MIN/MAX` 天然与写入顺序无关；
- `LAST` 按 **(样本时间戳, 接收序号)** 决胜，乱序写入结果相同；
- 重复样本在写入前按窗口去重；
- 窗口封存后聚合值**冻结**。

因此同一窗口无论样本以何种顺序、是否迟到（宽限期内）到达，聚合值都**可重复复现**。

---

## 2. 时间窗口语义

固定窗口按 epoch 对齐：

```
windowStart = floor(timestampMs / windowSizeMs) * windowSizeMs
windowEnd   = windowStart + windowSizeMs
```

窗口生命周期：

```
OPEN（开放，接收正常样本）
  │  到达 windowEnd
  ▼
CLOSED（逻辑上关闭；在 latenessMs 宽限期内仍可补写迟到样本）
  │  到达 windowEnd + latenessMs
  ▼
SEALED（封存：聚合冻结，驱动一次且仅一次告警判定）
  │  到达 windowEnd + retentionMs
  ▼
EVICTED（从内存驱逐；若该序列已无任何窗口，释放其基数配额）
```

以处理时刻 `now` 对样本时间戳分类：

| 分类 | 条件 | 处理 |
| --- | --- | --- |
| NORMAL | 时间戳位于当前窗口（或未来偏差内的当前/未来窗口） | `ACCEPTED` |
| LATE | 窗口已关闭但 `windowEnd + latenessMs > now` | `ACCEPTED_LATE`，补入聚合 |
| TOO_OLD | `timestamp < now - retentionMs`，或窗口已过迟到宽限/已封存 | 拒绝 |
| TOO_FAR_FUTURE | `timestamp > now + futureSkewMs` | 拒绝 |

推进由 `PipelineEngine.tick()` 完成：**封存到期窗口 → 按（窗口起点, 序列）
排序后驱动告警判定 → 驱逐过期窗口并释放基数配额**。生产环境由 Spring
定时任务每 `observability.tick-ms`（默认 1s）调用；测试中手动调用，
配合可替换时钟保证时间线完全可控、结果可复现。

---

## 3. 告警规则与状态语义

规则（AlertRule）：

```
对 metric 中 labelSelector 命中的每个序列，
取其每个封存窗口的 function 聚合值，用 comparator 与 threshold 比较；
条件连续满足 fireAfterWindows（≥1）个窗口后触发。
```

`labelSelector` 为空表示命中该指标全部维度；非空时样本标签须包含全部选择器键值对。

### 3.1 状态机（每个「规则 × 序列指纹」维度独立）

```
            条件连续命中第 1 个窗口
 INACTIVE ───────────────────────────────▶ PENDING
    ▲▲                                       │
    │└──────── 任一窗口不满足（连续计数清零）──┘ 条件继续命中，累计到 fireAfterWindows
    │                                         ▼
    │                                      FIRING（产生 FIRED 事件）
    │                                         │ 条件不满足
    │                                         ▼
    └──────── 下一判定窗口仍不满足 ────── RECOVERED（产生 RECOVERED 事件）
```

- **未触发（INACTIVE）**：无连续命中计数。
- **待确认（PENDING）**：条件已连续命中但尚未达到 `fireAfterWindows`；
  期间任何一个窗口不满足，计数清零并回到 INACTIVE——**条件未持续满足绝不误报**。
- **触发（FIRING）**：持续满足达到阈值，产生一次 `FIRED` 事件；
  FIRING 期间继续命中**不产生重复事件**（按维度去重，而不是重复堆积）。
- **恢复（RECOVERED）**：FIRING 后条件不满足，产生一次 `RECOVERED` 事件；
  再观察一个判定窗口，仍不满足则静默回到 INACTIVE。
- **恢复后再次触发**：`fireOccurrence` 递增，`eventId` 不同，
  与上一轮触发事件**可区分**。

每个（规则, 维度, 窗口）只评估一次：评估要求窗口起点严格推进，
窗口按起点升序处理，因此判定结论与线程调度无关、可复现。

### 3.2 事件（AlertEvent）

类型：`PENDING_STARTED`（仅观测用）/ `FIRED` / `RECOVERED`。
字段包含规则、维度指纹与标签、窗口起点/大小、观测值、聚合函数、比较符、
阈值、`fireOccurrence`、评估时间，以及唯一 `eventId`，完整记录判定依据。

---

## 4. 通知控制层（静默 / 冷却 / 抖动）

状态机如实反映指标状态，但"是否对外发通知"叠加了一层独立、可解释的控制：
`NotificationController` 在每个（规则, 维度, 窗口）评估完成后，
对状态机产出的通知类事件（`FIRED` / `RECOVERED`）做出**通知判定**，
结果逐条落入通知台账（`NotificationRecord`），发出与抑制都留痕：

| decision | 含义 |
| --- | --- |
| `NOTIFIED` | 正常对外通知 |
| `SUPPRESSED_BY_SILENCE` | 命中静默/维护窗口，被抑制 |
| `SUPPRESSED_BY_COOLDOWN` | 处于通知冷却期，被抑制 |
| `SUPPRESSED_BY_FLAPPING` | 维度处于抖动降噪状态，被抑制 |

每条记录还带有 `origin`（`INITIAL` 本轮首次 / `REPEAT` 冷却后再通知 /
`FLAP_EXIT` 抖动退出结论）、`fireOccurrence`、`notifyRound` 与唯一
`notificationId`——**冷却后的新一轮通知与同一轮里被抑制的重复项一眼可区分**。
所有判定以**窗口起点**为时间基准，不依赖墙钟，同样的输入序列结论可复现。

### 4.1 静默 / 维护窗口（Silence）

- 对某条规则、或规则下被 `labelSelector` 选中的部分维度，划定 `[startMs, endMs)`；
- 被评估窗口的起点落在区间内时，通知类事件被抑制并记录为
  `SUPPRESSED_BY_SILENCE`；**状态机照常推进、结论不作假**；
- 静默结束后不补发积压命中、也不凭空产生恢复：若届时仍在 FIRING，
  只补一条 `REPEAT` 通知；静默期间发生的恢复只留一条被抑制的记录；
- 设置结果明确可区分（不会默默生效或失效）：
  `CREATED` / `INVALID_RANGE`（end≤start）/ `IN_PAST`（整体落在过去）/
  `DUPLICATE`（同规则+同选择器+同区间）/ `OVERLAP`（同规则同选择器区间重叠）/
  `LIMIT_REACHED`（达到 `max-silences` 上限）；
- 过期静默随窗口推进自动回收；规则删除时其静默即刻回收。

### 4.2 通知冷却与去重（Cooldown）

- 某维度发出 `FIRED` 通知后进入 `notification-cooldown-ms` 冷却期，
  期内同维度的再次触发通知被抑制（记录为 `SUPPRESSED_BY_COOLDOWN`）；
- 冷却期满时若维度仍在 FIRING，补发一条 `origin=REPEAT`、`notifyRound`
  递增的通知，与首次通知可区分；
- `RECOVERED` 通知**不受冷却影响**，该出就出；
- 冷却跟踪按维度活跃度老化：维度停止评估超过保留期后回收，
  且条目至少存活一个完整冷却期。

### 4.3 抖动识别（Flap Damping）

- 每个维度在 `flap-window-ms` 观察窗口内，触发/恢复切换次数达到
  `flap-transition-threshold`（默认 4，正常抖一两次不会误判）即进入
  **降噪（damping）**：期间通知类事件被抑制并记录为 `SUPPRESSED_BY_FLAPPING`；
- 降噪期间只观察稳定性：连续稳定满足、或连续稳定不满足达到
  `flap-stable-windows` 个窗口后退出，并给出一条 `origin=FLAP_EXIT`
  的一次性结论（方向与稳定结果一致：稳定满足→`FIRED`，稳定不满足→`RECOVERED`）；
- 每个维度的抖动依据可查询（`GET /notifications/flapping`）：
  观察窗内切换次数、两个方向的连续稳定计数、最近切换窗口，
  与每次判定的输入一一对应。

### 4.4 状态回收与上限

| 状态 | 上限配置 | 回收时机 | 到达上限的行为 |
| --- | --- | --- | --- |
| 静默条目 | `max-silences`（128） | 过期随 tick 回收；规则删除即刻回收 | 新增返回 `LIMIT_REACHED`（HTTP 429） |
| 抖动跟踪 | `max-notification-trackers`（2000） | 维度老化（保留期）随 tick 回收；规则删除即刻回收 | 淘汰最旧条目并累计 `flapTrackersEvicted` |
| 冷却跟踪 | `max-notification-trackers`（2000） | 同上（按维度活跃度老化） | 淘汰最旧条目并累计 `cooldownTrackersEvicted` |

淘汰计数在 `GET /api/v1/notifications/status` 可见——到达上限是可观测事件，
不崩溃也不静默失效。所有跟踪状态只在评估锁内按窗口顺序变更，
并发上报与后台推进同时发生时结论确定、可复现。

### 4.5 与原有状态机的配合及兼容范围

- 状态机（INACTIVE/PENDING/FIRING/RECOVERED）与生命周期事件
  （`AlertEvent`）**完全不变**：通知控制层只读状态结论、不改写；
  原有接口 `/alerts`、`/alerts/events` 语义不变，旧客户端不受影响；
- 通知控制是**纯叠加层**：不配置静默、冷却设为 0、抖动阈值调大，
  行为即退化为"状态机产出什么就通知什么"（`PENDING_STARTED` 本就不是通知）；
- 通知台账（`/notifications`）与生命周期事件（`/alerts/events`）相互独立：
  前者回答"该不该发、发了没有、为什么没发"，后者回答"状态机判出了什么"。

---

## 5. 基数与降级规则

系统只在**配额内增长**，超限拒绝新序列，绝不无限膨胀：

| 守卫 | 默认值（配置项） | 超限行为 | 恢复 |
| --- | --- | --- | --- |
| 单指标序列基数 | 100（`max-series-per-metric`） | 拒绝该指标新序列，状态 `CARDINALITY_LIMIT`，原因 `METRIC_CARDINALITY_EXCEEDED` | 旧窗口驱逐、序列注销后配额自动回落 |
| 全局序列基数 | 1000（`max-global-series`） | 拒绝任意新序列，状态 `GLOBAL_CARDINALITY_LIMIT`，原因 `GLOBAL_CARDINALITY_EXCEEDED` | 同上 |
| 上报速率 | 10000/s（`rate-limit-per-sec`） | 令牌桶取令牌失败则 `RATE_LIMITED` | 时间推移令牌回补，自动恢复 |

要点：

- **已注册序列永远放行**——降级只挡新增高基数，不影响既有指标的正常上报；
- 接纳与注册是**原子**操作，并发突发不会超发序列；
- 拒绝原因可区分，并在 `GET /api/v1/pressure` 自观测快照中可见：
  受理/迟到/重复/各类拒绝计数、当前序列数、每指标序列数、估算速率、
  各限流标志与 `primaryReason`；
- 窗口超过保留期被驱逐后，若该序列不再有任何窗口，守卫注销该序列，
  **配额或压力回落后系统自动恢复正常判定**。

---

## 6. 本地存储与重放

- 已受理样本在分配接收序号的同一临界区内以 JSONL 追加写入
  `${journal-dir}/samples.jsonl`（默认 `data/observability/samples.jsonl`）；
- 接收序号单调递增，落盘顺序即接收顺序；`LAST` 决胜与全链路追溯以此为准；
- 启动时（`replay-on-startup=true`，默认开启）重放日志：重建窗口聚合、
  恢复基数注册，并把序号拨到历史最大值，重启后聚合结果一致、序号续接；
- 损坏行跳过并计数告警，不会因单行坏数据导致全量不可读；
- 置 `journal-enabled=false` 退化为纯内存（测试使用）。

---

## 7. HTTP 接口（/api/v1）

| 方法与路径 | 说明 |
| --- | --- |
| `POST /metrics/ingest` | 上报单条样本，返回 IngestResult |
| `POST /metrics/ingest-batch` | 批量上报（逐条独立判定，单条异常不影响其他） |
| `GET /metrics?metric=&windowStartMs=&fingerprint=` | 查询窗口聚合（COUNT/SUM/AVG/MIN/MAX/LAST/状态） |
| `PUT /rules` | 创建/更新告警规则 |
| `GET /rules` / `DELETE /rules/{id}` | 列出 / 删除规则 |
| `GET /alerts?ruleId=` | 规则维度状态快照（INACTIVE/PENDING/FIRING/RECOVERED） |
| `GET /alerts/events?ruleId=` | 告警生命周期事件（含 occurrence 与判定依据） |
| `PUT /silences` | 设置静默窗口；结果状态可区分（CREATED/INVALID_RANGE/IN_PAST/DUPLICATE/OVERLAP/LIMIT_REACHED） |
| `GET /silences?ruleId=` / `DELETE /silences/{id}` | 查询 / 取消静默（取消结果 CANCELLED/NOT_FOUND） |
| `GET /notifications?ruleId=` | 通知台账：每条通知判定的 kind/origin/decision/round 与依据 |
| `GET /notifications/flapping` | 各维度抖动跟踪快照（切换计数、稳定计数、是否降噪） |
| `GET /notifications/status` | 通知控制自观测：各类跟踪条目数与上限淘汰计数 |
| `GET /pressure` | 压力与降级自观测快照 |
| `POST /admin/tick` | 手动推进一个周期（封存→判定→驱逐） |

---

## 8. 本地验证方法

环境要求：JDK 21+、Maven 3.9+。

### 8.1 运行全部测试

```bash
mvn test
```

测试共 32 个（全部确定性，无随机等待依赖；测试时钟可手动推进、存储纯内存）：

| 测试类 | 覆盖内容 |
| --- | --- |
| `IngestValidationTest` | 各类异常输入的可区分拒绝、服务端补时间戳 |
| `WindowAggregationTest` | 乱序/顺序聚合一致、LAST 决胜、重复去重、迟到补入、封存冻结、可复现 |
| `AlertStateTransitionTest` | PENDING 不误报、持续触发、FIRING 维度去重、恢复、恢复后再触发可区分、RECOVERED 回落 INACTIVE、多维度独立 |
| `SilenceControlTest` | 静默期间状态照常推进但通知抑制、结束后只补一条 REPEAT、静默内恢复不重冒、维度级静默、设置结果全部分支（非法/过去/重复/重叠/上限/取消）、过期回收 |
| `NotificationCooldownTest` | 持续触发冷却期满才补 REPEAT、冷却期内再触发被抑制、恢复不受冷却影响、相同输入序列重放结论一致 |
| `FlapDetectionTest` | 4 次切换进入降噪、稳定满足/不满足退出并给出 FLAP_EXIT 结论、3 次切换不误判 |
| `NotificationLifecycleTest` | 规则删除即刻回收、维度老化回收、跟踪上限淘汰最旧并计数 |
| `DegradationTest` | 单指标/全局基数超限原因区分、已有序列不受影响、驱逐后配额恢复、令牌桶限流与恢复 |
| `ConcurrentIngestTest` | 8 线程并发不同样本计数精确、SUM 确定；16 线程并发同一逻辑样本恰好计入一次 |
| `JournalReplayTest` | 重启重放后聚合一致、基数恢复、接收序号续接 |
| `ObservabilityApiTest` | HTTP 端到端：受理/拒绝、规则、封存、FIRING、基数降级、压力接口 |
| `NotificationApiTest` | HTTP 端到端：静默设置各结果分支与 HTTP 状态码、查询/取消、通知台账与自观测接口 |

**人工核对方式**：测试使用 SLF4J 日志逐条打印输入与判定依据，例如：

```
INGEST in={{cpu, labels={host=a}, ts=..., value=150.0, source=s1}}
  -> ACCEPTED_LATE | seq=42 | detail=accepted as late into closed window ...
TICK at=... -> sealed=1, events=1, evictedWindows=0, deregisteredSeries=0
W1 state=INACTIVE consecutive=0
```

查看完整日志：

```bash
mvn test -Dtest=AlertStateTransitionTest
cat target/surefire-reports/TEST-*AlertStateTransitionTest.xml | grep -o 'INGEST[^"]*'
```

### 8.2 启动服务并手工验证

```bash
mvn spring-boot:run
# 上报（时间戳改为当前毫秒附近）
curl -s -XPOST localhost:8080/api/v1/metrics/ingest \
  -H 'Content-Type: application/json' \
  -d '{"metric":"cpu","labels":{"host":"a"},"value":150.0,"sourceId":"curl"}'
# 建规则
curl -s -XPUT localhost:8080/api/v1/rules -H 'Content-Type: application/json' \
  -d '{"id":"r1","metric":"cpu","labelSelector":{},"function":"AVG",
       "comparator":"GT","threshold":100.0,"fireAfterWindows":2}'
# 等两个窗口周期后推进（或等待后台调度），查状态/事件/压力
curl -s -XPOST localhost:8080/api/v1/admin/tick
curl -s 'localhost:8080/api/v1/alerts?ruleId=r1'
curl -s localhost:8080/api/v1/alerts/events
curl -s localhost:8080/api/v1/pressure

# 通知控制：给 r1 划一个 10 分钟静默窗口（时间戳按当前毫秒调整）
curl -s -XPUT localhost:8080/api/v1/silences -H 'Content-Type: application/json' \
  -d '{"ruleId":"r1","labelSelector":{"host":"a"},"startMs":START,"endMs":END,
       "reason":"nightly deploy"}'
curl -s 'localhost:8080/api/v1/silences?ruleId=r1'
# 推进后查看通知台账（发出/抑制及原因）、抖动快照与自观测计数
curl -s -XPOST localhost:8080/api/v1/admin/tick
curl -s 'localhost:8080/api/v1/notifications?ruleId=r1'
curl -s localhost:8080/api/v1/notifications/flapping
curl -s localhost:8080/api/v1/notifications/status
# 取消静默（ID 来自设置或查询结果）
curl -s -XDELETE localhost:8080/api/v1/silences/<id>
```

### 8.3 主要配置（application.properties / 环境变量）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `observability.window-size-ms` | 10000 | 窗口大小 |
| `observability.lateness-ms` | 30000 | 迟到宽限 |
| `observability.future-skew-ms` | 60000 | 允许的未来时间偏差 |
| `observability.retention-ms` | 3600000 | 内存窗口保留期（到期驱逐并释放基数） |
| `observability.max-series-per-metric` | 100 | 单指标序列基数上限 |
| `observability.max-global-series` | 1000 | 全局序列基数上限 |
| `observability.rate-limit-per-sec` | 10000 | 每秒速率配额（令牌桶） |
| `observability.journal-enabled` | true | 是否启用本地 JSONL 持久化 |
| `observability.replay-on-startup` | true | 启动是否重放 |
| `observability.journal-dir` | data/observability | 日志目录 |
| `observability.tick-ms` | 1000 | 后台推进间隔 |
| `observability.notification-cooldown-ms` | 120000 | FIRED 通知后的冷却期（0 表示关闭冷却） |
| `observability.flap-window-ms` | 300000 | 抖动观察窗口 |
| `observability.flap-transition-threshold` | 4 | 进入降噪所需的切换次数 |
| `observability.flap-stable-windows` | 3 | 退出降噪所需的连续稳定窗口数 |
| `observability.max-silences` | 128 | 静默条目上限 |
| `observability.max-notification-trackers` | 2000 | 冷却/抖动跟踪条目上限 |

---

## 9. 工程结构

```
model/        样本、受理结果、窗口聚合、规则、告警状态/事件、压力快照等类型
time/         可替换时钟；epoch 对齐窗口与迟到/异常边界
guard/        标签规范化与指纹、基数守卫、令牌桶
storage/      样本存储接口与本地 JSONL 实现（重放、序号分配）
aggregate/    窗口聚合器（映射、封存、驱逐）
ingest/       采集流水线（校验→限流→时间分类→基数→去重→落盘→聚合）
alert/        规则注册、告警状态存储、四态状态机判定器
notify/       通知控制层：静默存储、抖动跟踪、冷却与通知台账（叠加在状态机之上）
engine/       推进器（封存→判定→驱逐）与 Spring 定时任务
web/          HTTP 入口与请求体
config/       参数与 Spring 装配
```

所有核心组件均为普通 Java 类、构造器手工装配，可脱离 Spring 直接在测试中实例化。
