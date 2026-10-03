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

状态机（第 3 节）如实回答“现在是什么状态”；通知控制层只回答
“**这一次要不要对外发通知**”。两者完全分离：

```
封存窗口 → 状态机判定（AlertEvent，状态真相，不受任何控制影响）
        → 通知控制层（NotificationGate）→ NotificationRecord（下发 / 带原因的抑制）
```

判定器同时输出每次评估的输入快照（`AlertEvaluation`：规则、维度、窗口、
观察值、是否命中、新状态），通知层的每条结论都能回指到具体评估输入，
**输入与判定依据一一对应**。

### 4.1 静默 / 维护窗口（SilenceWindow）

- 对某条规则（可限定到若干维度指纹；空集合 = 全规则）设置 `[startMs, endMs)`；
- 生效期间评估照常进行、状态照常迁移，只是通知类记录判定为
  `SUPPRESSED_SILENCE` 不下发；
- **静默结束立即与真实状态对齐**：仍触发且本轮从未通知过 → 补发**一次**
  FIRED 结论；期间恢复过且对应触发确实下发过 → 补发**一次** RECOVERED；
  触发被静默吞掉的 → 恢复同样不发（`SUPPRESSED_ORPHAN_RECOVERY`），
  **不会凭空冒出恢复，也不会把静默期间的命中补发成一串通知**；
- 设置/取消结果可区分：`CREATED` / `REJECTED_DUPLICATE`（同内容重复设置）/
  `REJECTED_OVERLAP`（同规则同范围时间重叠）/ `REJECTED_INVALID_RANGE` /
  `REJECTED_PAST`（区间整体在过去）/ `REJECTED_CAPACITY` / `CANCELLED` /
  `NOT_FOUND`；已结束的静默在写入时自动回收。

### 4.2 通知冷却与去重（cooldown）

- 同一「规则 × 维度」下发 FIRED 后进入 `notification-cooldown-ms` 冷却期；
- 冷却期内的再次触发（含恢复后快速复触发）判定为 `SUPPRESSED_COOLDOWN`，
  记录中携带**所属通知轮次** `notificationRound`；
- 冷却结束仍触发 → 再发一轮 FIRED（`notificationRound` 递增、ID 不同，
  与被抑制的重复项一眼可辨）；持续触发时每冷却周期至多补发一轮；
- **恢复通知不受冷却影响**，该出就出；
- 默认 `0` = 关闭冷却，行为与引入通知层之前完全一致。

### 4.3 抖动识别（flapping）

- 在 `notification-flap-window-ms` 观察窗内，FIRED/RECOVERED 转移次数达到
  `notification-flap-transition-threshold` → 进入**抖动降噪**，产生一条
  `FLAPPING_ENTERED` 通知（含转移次数等判定依据），期间逐条事件全部
  `SUPPRESSED_FLAPPING`；
- 之后连续稳定满足 / 连续稳定不满足达到
  `notification-flap-exit-stable-windows` 个窗口 → 退出并产生
  `FLAPPING_EXITED`（含稳定计数依据），同时按真实状态补齐结论
  （结清旧轮次恢复 / 补发新轮次触发）；
- 只抖一两次（转移数低于阈值）不会误判；进入降噪的转移窗口本身不计入
  稳定计数；
- 默认观察窗 `0` = 关闭抖动识别。

### 4.4 一致性与治理

- 同一维度**下发的**通知流始终保持 FIRED/RECOVERED 交替，与真实状态一致；
- 通知跟踪器（规则 × 维度）数量受 `notification-max-trackers` 限制：
  超限不崩溃也不静默失效，降级为 `DELIVERED_UNTRACKED`（可区分）；
- 规则删除时其跟踪器与静默窗口立即回收；长期无评估的跟踪器按
  `notification-tracker-idle-ms` 老化回收；观察窗内的转移历史随窗口推进
  自动过期——通知层状态不随上报与维度无限增长；
- 通知判定只在推进线程内同步执行，输入按（窗口, 规则, 维度）排序，
  **同样输入序列重放结论完全一致**（有测试佐证）。

### 4.5 通知记录（NotificationRecord）

每条记录包含：种类（`FIRED`/`RECOVERED`/`PENDING`/`FLAPPING_ENTERED`/
`FLAPPING_EXITED`）、判定（`DELIVERED` / `DELIVERED_UNTRACKED` /
`SUPPRESSED_SILENCE` / `SUPPRESSED_COOLDOWN` / `SUPPRESSED_FLAPPING` /
`SUPPRESSED_ORPHAN_RECOVERY` / `OBSERVATION`）、通知轮次、状态机轮次、
来源事件 ID（对账补发的结论为 null）、判定依据 `detail`。

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
| `GET /pressure` | 压力与降级自观测快照 |
| `PUT /silences` | 设置静默窗口（结果码区分 重复/重叠/过去/非法/容量） |
| `GET /silences?ruleId=` | 查询静默窗口（含 SCHEDULED/ACTIVE/EXPIRED 状态） |
| `DELETE /silences/{id}` | 取消静默（CANCELLED / NOT_FOUND） |
| `GET /notifications?ruleId=` | 通知判定记录（下发/各类抑制、轮次、依据） |
| `GET /notifications/trackers` | 通知跟踪器快照（冷却计时、抖动状态等） |
| `POST /admin/tick` | 手动推进一个周期（封存→判定→通知控制→驱逐） |

---

## 8. 本地验证方法

环境要求：JDK 21+、Maven 3.9+。

### 8.1 运行全部测试

```bash
mvn test
```

测试共 30 个（全部确定性，无随机等待依赖；测试时钟可手动推进、存储纯内存）：

| 测试类 | 覆盖内容 |
| --- | --- |
| `IngestValidationTest` | 各类异常输入的可区分拒绝、服务端补时间戳 |
| `WindowAggregationTest` | 乱序/顺序聚合一致、LAST 决胜、重复去重、迟到补入、封存冻结、可复现 |
| `AlertStateTransitionTest` | PENDING 不误报、持续触发、FIRING 维度去重、恢复、恢复后再触发可区分、RECOVERED 回落 INACTIVE、多维度独立 |
| `DegradationTest` | 单指标/全局基数超限原因区分、已有序列不受影响、驱逐后配额恢复、令牌桶限流与恢复 |
| `ConcurrentIngestTest` | 8 线程并发不同样本计数精确、SUM 确定；16 线程并发同一逻辑样本恰好计入一次 |
| `JournalReplayTest` | 重启重放后聚合一致、基数恢复、接收序号续接 |
| `ObservabilityApiTest` | HTTP 端到端：受理/拒绝、规则、封存、FIRING、基数降级、压力接口 |
| `SilenceControlTest` | 静默期状态真相保留、静默结束单次结论、不凭空恢复、维度级静默、设置/取消各类结果码 |
| `NotificationCooldownTest` | 冷却期抑制复触发、冷却到期新一轮可区分、持续触发按周期补发、恢复不受冷却影响、跟踪器容量降级 |
| `FlappingDetectionTest` | 转移计数进入降噪、稳定满足/不满足退出结论、通知流交替一致、偶尔抖动不误判 |
| `NotificationGovernanceTest` | 规则删除回收、空闲跟踪器老化回收、同输入重放结论完全一致 |

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

# 通知控制：给规则 r1 划 10 分钟静默（维护窗口），期间状态照常判定但不发通知
NOW=$(($(date +%s%3N)))
curl -s -XPUT localhost:8080/api/v1/silences -H 'Content-Type: application/json' \
  -d '{"ruleId":"r1","startMs":'"$NOW"',"endMs":'"$((NOW+600000))"',"reason":"deploy"}' 
curl -s 'localhost:8080/api/v1/silences?ruleId=r1'
# 查看每条通知的判定（下发/静默抑制/冷却抑制/抖动抑制）与依据
curl -s 'localhost:8080/api/v1/notifications?ruleId=r1'
# 取消静默
curl -s -XDELETE localhost:8080/api/v1/silences/<silenceId>
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
| `observability.notification-cooldown-ms` | 0 | 触发通知冷却期；0 = 关闭（兼容旧行为） |
| `observability.notification-flap-window-ms` | 0 | 抖动观察窗；0 = 关闭抖动识别 |
| `observability.notification-flap-transition-threshold` | 6 | 观察窗内转移次数阈值（>=2 生效） |
| `observability.notification-flap-exit-stable-windows` | 3 | 退出降噪所需连续稳定窗口数 |
| `observability.notification-max-silences` | 256 | 静默窗口数量上限 |
| `observability.notification-max-trackers` | 10000 | 通知跟踪器（规则×维度）上限 |
| `observability.notification-tracker-idle-ms` | 3600000 | 跟踪器空闲回收时长 |

---

## 9. 工程结构

```
model/        样本、受理结果、窗口聚合、规则、告警状态/事件、压力快照等类型
time/         可替换时钟；epoch 对齐窗口与迟到/异常边界
guard/        标签规范化与指纹、基数守卫、令牌桶
storage/      样本存储接口与本地 JSONL 实现（重放、序号分配）
aggregate/    窗口聚合器（映射、封存、驱逐）
ingest/       采集流水线（校验→限流→时间分类→基数→去重→落盘→聚合）
alert/        规则注册、告警状态存储、四态状态机判定器（输出评估快照）
notify/       通知控制层：静默注册、通知门禁（冷却/抖动/对账）、通知记录存储
engine/       推进器（封存→判定→驱逐）与 Spring 定时任务
web/          HTTP 入口与请求体
config/       参数与 Spring 装配
```

所有核心组件均为普通 Java 类、构造器手工装配，可脱离 Spring 直接在测试中实例化。
