# Payload 解码与 dash 展示优化 — 设计文档

- 日期: 2026-06-30
- 范围: `tools/asynq`（CLI `task ls` / `task inspect` + `dash` TUI）
- 目标: 让 asynq 任务 payload 在 CLI 与 dash 中可读——解码 base64、识别 nsp 信封、JSON 美化打印、长内容可滚动。

## 1. 背景

`tools/asynq` 既提供 CLI（`asynq task ls/inspect/...`）也提供交互式 TUI（`asynq dash`）。两者目前都用「原样字节」方式展示 payload：

- CLI: `sprintBytes()`（`tools/asynq/cmd/root.go:466`）—— 可打印就原样输出，否则 `"non-printable bytes"`。
- dash: `formatByteSlice()`（`tools/asynq/cmd/dash/draw.go:591`）—— 单行、按弹窗宽度截断，无滚动。

在 nsp-platform 的 `taskqueue` 封装（`asynqbroker/trace_envelope.go`）下，入队时业务 payload 会被包进一个 trace 信封：

```json
{"_v":1,"_smpl":false,"_meta":{"source":"quick-producer"},"payload":"eyJhYmMiOjF9"}
```

由于 Go 的 `json.Marshal` 把 `[]byte` 序列化为 base64，**信封里只有内层 `payload` 字段是 base64**（`eyJ...` 即 `{"`）。当前工具直接展示这串 base64，几乎不可读（见 `asynq-dash.png`）。

## 2. 需求（已与用户确认）

1. payload 以 JSON 形式美化展示。
2. dash 中 payload 较长时支持上下键翻页（**弹窗内滚动**，非全屏视图）。
3. 展示时识别并解码 base64（含 nsp 信封的内层 payload）。
4. CLI `task ls` 增加 `--decode` 选项，解码 base64 内容。
5. **保留 envelope 元数据**（`_meta`、`_tid`/`_sid`/`_smpl` 等）——在详情视图（dash 弹窗 / `task inspect`）中展示；`task ls` 表格列只放解码后的业务 payload。
6. 数据形态：队列中只有两类消息——nsp 信封格式、或 asynq 原生消息（无信封，payload 是原始字节：JSON / 文本 / 二进制）。
7. `--decode` 同时做「解码 + 美化」。

## 3. 关键决策

| 决策点 | 选择 | 理由 |
|---|---|---|
| 解码策略 | 信封识别 + 通用 base64/JSON 判断（**两层**） | 精确覆盖两类数据；不依赖 nsp-platform |
| 与 nsp 的耦合 | **不依赖** `trace_envelope.go` | asynq 工具保持通用；靠 `_v`+`payload` 字段特征识别信封 |
| 共享逻辑位置 | 新增叶子包 `tools/asynq/payload` | `cmd → dash` 已有依赖；放叶子包避免循环依赖，且可独立测试 |
| dash 翻页方式 | 弹窗内滚动 | 体感一致，改动集中 |
| CLI 默认行为 | 不加 `--decode` 完全保持现状 | 不破坏现有脚本 |
| dash 默认行为 | 弹窗默认解码 + 美化 | TUI 是给人看的 |
| 元数据展示 | 仅详情视图展示（dash 弹窗 / inspect） | 表格列无空间；详情才完整 |

## 4. 共享解码包: `tools/asynq/payload`

纯函数叶子包，无对 `cmd` / `cmd/dash` 的依赖。被 CLI 与 dash 共同复用。

### 4.1 类型

```go
package payload

// Result 是对一段 asynq payload 解码后的结构化结果。
type Result struct {
    // Kind 分类输入:
    //   "envelope" — nsp 信封，内层 payload 已解码
    //   "base64"   — 整段为 base64
    //   "json"     — 普通 JSON
    //   "text"     — 普通可打印文本
    //   "binary"   — 不可打印二进制
    Kind string

    // Payload 是解码后、人类可读的业务 payload 文本。
    //   indent=true  → JSON 多行缩进
    //   indent=false → JSON 单行紧凑（用于表格列）
    //   二进制 → "<non-printable, N bytes>"
    Payload string

    // Envelope 仅当 Kind == "envelope" 时非 nil。
    Envelope *Envelope
}

// Envelope 是从 nsp 信封中提取的传输/trace 元信息。
type Envelope struct {
    Version   int               // _v
    TraceID   string            // _tid
    SpanID    string            // _sid
    Sampled   bool              // _smpl
    HasSampled bool             // _smpl 字段是否存在
    ReplyTo   string            // _rto 的紧凑 JSON 字符串（便于展示）
    Meta      map[string]string // _meta
}
```

### 4.2 函数

```go
// Decode 解码并美化一段 payload，返回结构化结果（含 envelope 元信息）。
// 用于 dash 弹窗与 task inspect。
func Decode(data []byte, indent bool) Result

// Format 是 Decode 的便捷封装，只返回业务 payload 文本（不含元信息）。
// 用于 task ls 的表格列（indent=false 单行）。
func Format(data []byte, indent bool) string
```

### 4.3 解码算法

```
Decode(data, indent):
  if len(data) == 0: return Result{Kind:"text", Payload:""}   // 或 "<nil>" 由调用方决定
  ① 信封? data 是 JSON 对象 且 含 _v(int) 且 含 payload(base64 字符串)
        inner = base64decode(payload)
        return Result{Kind:"envelope", Payload: prettyOrRaw(inner, indent), Envelope: extractEnv(...)}
  ② 整段 base64? data 匹配 ^[A-Za-z0-9+/]+={0,2}$ 且 len%4==0 且 解码成功
        decoded = base64decode(data)
        return Result{Kind:"base64", Payload: prettyOrRaw(decoded, indent)}
  ③ JSON? json.Valid(data)
        return Result{Kind:"json", Payload: indent? pretty : compact}
  ④ 文本? isPrintable(data)
        return Result{Kind:"text", Payload: string(data)}
  ⑤ 否则
        return Result{Kind:"binary", Payload: "<non-printable, N bytes>"}

prettyOrRaw(b, indent):
  if json.Valid(b): return indent ? indentJSON(b) : compactJSON(b)
  if isPrintable(b): return string(b)
  return "<non-printable, N bytes>"
```

**注意点**:
- 信封识别要求同时满足 `_v`(int) 与 `payload`(base64 字符串)，降低误判。
- 不做深层递归遍历任意 JSON 树（用户已确认只有两类数据形态），保持简单。
- base64 判断须校验字符集 + padding + 长度，并对解码结果要求可打印或为 JSON，避免把普通文本误当 base64。
- `isPrintable` 沿用 `cmd/dash/draw.go:575` 与 `cmd/root.go:473` 现有语义。

### 4.4 测试（表驱动）

`payload_test.go` 覆盖:
- nsp 信封（内层 payload 为 JSON / 文本 / 二进制）
- 整段 base64（解码后为 JSON / 文本）
- 普通 JSON（多行/单行两种 indent）
- 普通文本
- 二进制（含不可打印字节）
- nil / 空
- 退化为 asynq 原生（无 `_v` 但有 `payload` 字段，或反之）→ 不识别为信封
- envelope 元数据提取正确性（`_meta`、`_tid`、`_smpl` 缺省）

## 5. dash TUI 改造

### 5.1 State（`cmd/dash/dash.go`）

```go
type State struct {
    ...
    payloadScrollOffset int  // 弹窗 payload 区当前滚动行偏移；弹窗关闭/换任务时置 0
}
```

### 5.2 渲染（`cmd/dash/draw.go`）

- 表格列：`formatByteSlice(t.Payload)` → `payload.Format(t.Payload, false)`（单行解码）。`Result` 列同理。
- 弹窗 `drawTaskModal`:
  - 调 `payload.Decode(task.Payload, true)` 得 `Result`。
  - 业务 payload 多行展示；按弹窗内容宽度**折行**为 `payloadLines`。
  - 若 `Result.Envelope != nil`，在 payload 区后追加 envelope 摘要行（如 `trace_id=.. span_id=.. sampled=..` 及每个 `_meta` 键值）。
  - payload 区为可滚动窗口: `visible = payloadLines[offset : offset+可用行数]`，`offset` 夹紧到 `[0, max(0, len-可用行数)]`。
  - 超出可视区时，payload 标签后显示 `(↑/↓ scroll  offset/total)`。
- footer（`drawFooter`）：弹窗打开时追加提示 `↑/↓: Scroll payload`。
- `drawHelp`：补充弹窗滚动说明。

### 5.3 键位（`cmd/dash/key_event.go`）

- 弹窗打开（`state.taskID != ""`）时，`j/k/↑/↓` 调整 `payloadScrollOffset`（夹紧），而非移动表格行。当前 `downKeyQueueDetails`/`upKeyQueueDetails` 仅在 `taskID == ""` 时移动 `taskTableRowIdx`，正好分支处理。
- `enterKeyQueueDetails` 选中任务时 `payloadScrollOffset = 0`。
- `goBack` 关闭弹窗时 `payloadScrollOffset = 0`。

### 5.4 dash 测试

`draw_test.go` 增补（纯函数）:
- `payload.Decode` 各分支已在 payload 包测过，dash 侧重渲染辅助: 折行 `wrapLines(s, width)`、窗口切片 `visibleWindow(lines, offset, n)`、offset 夹紧 `clampOffset(offset, total, n)`。

## 6. CLI 改造（`tools/asynq/cmd/task.go` + `root.go`）

### 6.1 `task list --decode`

```go
taskListCmd.Flags().BoolP("decode", "d", false,
    "decode base64/envelope payloads and pretty-print (single-line)")
```

- `taskList` 读取 `decode`，透传给各 `listXxxTasks(qname, pageNum, pageSize, decode)`。
- 各 `listXxxTasks`: `sprintBytes(t.Payload)` → `decode ? payload.Format(t.Payload, false) : sprintBytes(t.Payload)`。
- 不加 flag 时**完全等同现状**。

### 6.2 `task inspect --decode`（顺带补全 payload 展示）

当前 `printTaskInfo`（`task.go:448`）**未打印 payload**。补全:
```go
taskInspectCmd.Flags().BoolP("decode", "d", false, "decode base64/envelope payload and pretty-print")
```

- `printTaskInfo(info, decode bool)`: 当 `decode` 或 payload 非空时，调 `payload.Decode(info.Payload, true)`，多行美化打印业务 payload；若 `Result.Envelope != nil`，额外打印 envelope 元数据（`trace_id`/`span_id`/`sampled`/`_meta`）。

## 7. 变更清单

**新增**
- `tools/asynq/payload/payload.go`
- `tools/asynq/payload/payload_test.go`

**修改**
- `tools/asynq/cmd/dash/dash.go` — State 增 `payloadScrollOffset`
- `tools/asynq/cmd/dash/draw.go` — 解码渲染 + 可滚动 payload 区 + envelope 摘要 + footer/help 提示；新增 `wrapLines`/`visibleWindow`/`clampOffset` 辅助
- `tools/asynq/cmd/dash/key_event.go` — 弹窗滚动键位 + offset 重置
- `tools/asynq/cmd/dash/draw_test.go` — 渲染辅助测试
- `tools/asynq/cmd/task.go` — `task list` / `task inspect` 加 `--decode`，inspect 补全 payload
- `tools/asynq/cmd/root.go` — 在 `sprintBytes`（`:466`）旁新增 `formatPayload(payload []byte, decode bool) string`：`decode` 时调 `payload.Format(payload, false)`，否则回落到 `sprintBytes`

## 8. 非目标 / 明确不做

- 不让 asynq 工具 import nsp-platform。
- 不做任意 JSON 树的深层 base64 递归（用户确认只有两类数据形态）。
- 不改变 CLI 无 `--decode` 时的输出格式。
- 不新增全屏 payload 查看器（用弹窗内滚动）。
- 不修改 asynq 库本身的 `Task`/`TaskInfo` 语义（仅工具层）。

## 9. 风险与缓解

| 风险 | 缓解 |
|---|---|
| base64 误判普通文本 | 校验字符集+padding+长度，且解码结果须可打印/为 JSON |
| 信封字段特征误判（非 nsp 的 JSON 恰有 `_v`+`payload`） | 概率低；且即便误判，行为是「解出内层 payload 展示」，可读性不降 |
| `task ls` 表格中解码后单行过长 | 终端自然换行；与现状（base64 同样长）持平，可接受 |
| 弹窗滚动实现复杂度 | 把折行/切片/夹紧做成纯函数，单独测试 |
