# ASR 配置参考

本文档分别描述 ASR 应用配置和 `github.com/caitunai/asr-sdk-go` SDK 配置。SDK 不依赖 Viper；`services/asr` 和 WebSocket handler 负责把应用配置转换成 SDK 类型。

## 应用最小配置

```toml
[asr]
enabled=true

[asr.providers.generic]
baseURL="https://asr.example.com"
path="/v1/audio/transcriptions"
apiKey="..."
```

generic adapter 固定使用 WAV、multipart `file`、Bearer 鉴权和 `response_format=json`。请求上下文中的 prompt 去除首尾空白后非空时才发送 `prompt`；不会发送 model、language、hotwords 或 language_hints。

## 应用层完整配置

下表中的非必填项均有代码默认值。它们仍可通过 TOML 或 Viper 环境变量覆盖，但通常只在真实语料评测和容量测试后调整。

| Viper 键 | 类型 | 默认值 | 必填条件 | 作用 |
|---|---|---:|---|---|
| `asr.enabled` | bool | false | 否 | 是否在进程启动时创建 ASR provider/client。 |
| `asr.defaultEnabled` | bool | true | 否 | WebSocket `audio.start` 未提供 ASR enabled 时的会话默认值。 |
| `asr.provider` | string | `generic` | 否 | provider adapter 名称。当前应用实现 `generic`；未来 provider 应新增独立 adapter。 |
| `asr.defaultLanguage` | string | `auto` | 否 | 会话未指定语言时的 BCP 47 tag 或自动检测 sentinel。 |
| `asr.requestTimeout` | duration string | `8s` | 否 | 单次 provider HTTP 请求超时，例如 `"8s"`。 |
| `asr.retryCount` | int | 1 | 否 | 同一 provider 的可恢复错误重试次数；当前只允许 0 或 1。 |
| `asr.maxConcurrency` | int | 16 | 否 | 全局 ASR client 并发上限；单会话 scheduler 仍保持一个 active。 |
| `asr.contextSilenceMs` | duration/ms | 200ms | 否 | 拼接两个有间隔 VAD segment 时插入的静音。时间连续时不插入。 |
| `asr.tailFinalizeSilenceMs` | duration/ms | 900ms | 否 | 长片段或未启用短片段等待时，从 segment EndAt 计算的尾段确认静音。 |
| `asr.tailFinalizeResultTimeoutMs` | duration/ms | 20s | 否 | 尾段正式任务完成后仍无可采用结果时的最终等待上限。 |
| `asr.shortSegmentMaxDurationMs` | duration/ms | 6s | 否 | 小于该时长的完整 VAD 优先等待相邻 VAD 上下文。 |
| `asr.shortSegmentNeighborWaitMs` | duration/ms | 3s | 否 | 短片段从 EndAt 起等待下一次 SpeechStarted 的最长时间。不得短于 tail finalize silence。 |
| `asr.longSpeechCommitAfterMs` | duration/ms | 15s | 否 | 当前未正式提交的连续语音达到该时长后，允许把历史短静音边界升级为正式 Segment。 |
| `asr.longSpeechCommitPrefixMs` | duration/ms | 5s | 否 | 在当前未提交起点后的该前缀区间内选择最后一个短静音作为正式 end，并从同一点重新 start。必须小于 commit after。 |
| `asr.maxWindowMs` | duration/ms | 65s | 否 | 单个 standalone/pair ASR 音频窗口的最大时长。 |
| `asr.stopTimeoutMs` | duration/ms | 23s | 否 | WebSocket stop 等待 ASR completed 的最小时间；会按正式任务数量自动扩展。 |
| `asr.tailAnchorEnabled` | bool | true | 否 | 尾段缺少下一窗口时是否提交同 provider 的 standalone anchor。 |
| `asr.providers.generic.baseURL` | string | 无 | generic 启用时必填 | HTTP 服务根地址。远程必须 HTTPS；localhost/loopback 可用 HTTP。 |
| `asr.providers.generic.path` | string | 无 | generic 启用时必填 | 转写接口路径，例如 `/v1/audio/transcriptions`。 |
| `asr.providers.generic.apiKey` | string | 无 | generic 启用时必填 | Bearer API key。应通过环境变量或私密配置注入，不能进入日志或前端。 |

`duration/ms` 表示应用 loader 同时接受 Go duration 字符串（如 `900ms`、`3s`）或正整数毫秒。`asr.requestTimeout` 应使用 Go duration 字符串。

### WebSocket 会话上下文

以下字段来自 `audio.start`，不是进程级 provider 配置：

| 字段 | 默认值 | 作用 |
|---|---|---|
| `asr.enabled` | `asr.defaultEnabled` | 单个 WebSocket 是否启用 ASR。 |
| `asr.language` | `asr.defaultLanguage` | BCP 47 语言或 `auto`。 |
| `asr.languageHints` | 空 | 自动检测时的候选语言，最多 4 个。generic 应用预设不会把它发送给 provider，但其他 adapter 可使用。 |
| `asr.prompt` | 空 | 会话上下文。非空白时复制到 standalone、pair、tail 和 preview 请求。 |
| `asr.terms` | 空 | 专有名词列表，最多 100 个；用于对齐保护，也可由支持的 provider 使用。 |

## `GenericHTTPConfig`

应用 preset 只暴露三个 endpoint/credential 字段；独立 SDK 使用者可以通过完整 `GenericHTTPConfig` 适配其他 multipart 协议。

| 字段 | 默认值 | 作用 |
|---|---:|---|
| `Name` | 无 | Provider 稳定名称，必填。 |
| `Model` | 无 | Provider model 标识，必填；可仅用于结果元数据。 |
| `BaseURL` | 无 | 服务根地址，必填。HTTPS 默认允许；HTTP 仅 loopback 或显式 `AllowInsecureHTTP`。 |
| `Path` | 无 | POST 路径，必填。 |
| `APIKey` | 空 | 鉴权 token。空值时不发送鉴权头，除非 `RequireAPIKey=true`。 |
| `AuthHeader` | `Authorization` | 鉴权 header 名称。 |
| `AuthScheme` | `Bearer` | API key 前的鉴权 scheme。 |
| `FileField` | `file` | multipart 音频字段名。 |
| `ModelField` | `model` | model 表单字段；`OmitModel=true` 时禁用。 |
| `LanguageField` | `language` | language 表单字段；auto 不发送，`OmitLanguage=true` 时禁用。 |
| `PromptField` | `prompt` | prompt 表单字段；prompt 为空白或 `OmitPrompt=true` 时不发送。 |
| `TermsField` | `hotwords` | 专有名词 JSON 数组字段；`OmitTerms=true` 时禁用。 |
| `LanguageHintsField` | `language_hints` | language hints JSON 数组字段。还要求 `SupportsLanguageHints=true`。 |
| `ExtraFields` | 空 map | 每次请求固定附加的表单字段，例如 `response_format=json`。 |
| `RequireAPIKey` | false | true 时 API key 为空会导致初始化失败。 |
| `OmitModel` | false | 不发送 model。 |
| `OmitLanguage` | false | 不发送 language。 |
| `OmitPrompt` | false | 不发送 prompt。 |
| `OmitTerms` | false | 不发送 terms/hotwords。 |
| `OmitLanguageHints` | false | 不发送 language hints。 |
| `ResponseBodyLimit` | 2 MiB | Resty 响应体大小上限。 |
| `AllowInsecureHTTP` | false | 允许非 loopback HTTP。生产环境不建议启用。 |
| `SupportsWordTimes` | false | 声明 provider 可返回 word/segment timestamps。 |
| `SupportsAutoLanguage` | false | 声明 provider 支持自动语言检测。 |
| `SupportsLanguageHints` | false | 声明 provider 支持语言提示。 |
| `StripLeadingLanguageLabel` | false | 去除 `[English]`、`[日本語]` 等首部标签。 |
| `AudioFormat` | `wav_pcm_s16le` | 请求音频格式，可选 `wav_pcm_s16le`、`raw_pcm_s16le`。 |

响应 JSON 至少需要 `text`。可选 `language`、`words` 或 `segments`；时间项使用 `word`/`text`、`start`、`end`、`confidence`。

## `ClientConfig`

| 字段 | SDK 默认值 | 应用默认值 | 作用 |
|---|---:|---:|---|
| `RequestTimeout` | 8s | 8s | 一次转写请求及有限重试共享的 deadline。 |
| `RetryCount` | 0 | 1 | 可恢复错误重试次数，只允许 0 或 1。 |
| `MaxConcurrency` | 16 | 16 | client 全局并发 semaphore 容量。 |
| `AudioFormat` | WAV PCM16 | WAV PCM16 | float32 PCM 编码后的 provider payload 格式，必须在 provider capabilities 中。 |

## `SessionConfig`

| 字段 | SDK 默认值 | 应用默认值 | 作用 |
|---|---:|---:|---|
| `SessionID` | 无 | 自动生成 | 会话唯一 ID，SDK 调用时必填。 |
| `Language` | `auto` | `auto` | BCP 47 tag 或自动检测。 |
| `LanguageHints` | 空 | 会话输入 | 候选 BCP 47 tag，自动去重，不允许 `auto`。 |
| `Context` | 空 | 会话输入 | `RecognitionContext`，包含 prompt、terms、extra fields。 |
| `SampleRate` | 无 | codec sample rate | PCM 采样率，必填且必须大于 0。 |
| `Channels` | 无 | 1 | 当前只支持单声道，必须为 1。 |
| `EventBuffer` | 64 | 64 | SDK 事件 channel buffer。消费过慢会对 session actor 产生背压。 |
| `ContextSilence` | 200ms | 200ms | 有时间间隔的双 segment 拼接静音。 |
| `TailFinalizeSilence` | 900ms | 900ms | 尾段静音确认目标。 |
| `TailFinalizeResultWait` | 20s | 20s | 尾段结果等待上限。 |
| `ShortSegmentMaxDuration` | 0（关闭） | 6s | 短片段判定门槛。 |
| `ShortSegmentNeighborWait` | 0（关闭） | 3s | 短片段等待相邻 SpeechStarted 的时间。两个短片段参数必须同时为 0 或同时为正。 |
| `MaxWindowDuration` | 30s | 65s | standalone/pair 组合窗口的最大时长。 |
| `TailAnchorEnabled` | false | true | 是否允许尾段 standalone anchor。 |

`RecognitionContext`：

| 字段 | 作用 |
|---|---|
| `Prompt` | 自然语言上下文；generic provider 仅在非空白时发送。 |
| `Terms` | 专有名词；用于 provider hotwords（若支持）和 Unicode 对齐保护。 |
| `ExtraFields` | 请求级 provider 扩展字段。固定 provider 字段优先，不能被它覆盖。 |

## `SegmentedSessionConfig`

`SegmentedSession` 在低层 `Session` 之上增加连续 PCM、通用 speech boundary、短静音 Preview、buffer 和完成等待管理。

| 字段 | SDK 默认值 | 作用 |
|---|---:|---|
| `Session` | 无 | 上表中的 `SessionConfig`。`SessionID` 为空时由高层会话安全生成。 |
| `MaxBufferedSamples` | 2 分钟 PCM | 当前活动语音允许保留的最大 sample 数。达到边界时先处理释放，再检查最终保留量。 |
| `IdlePreRollSamples` | 100ms PCM | 没有活动语音时保留的前置音频，用于覆盖 VAD speech padding；其余静音立即释放。 |
| `RequestTimeoutHint` | 8s | 估算停止阶段 authoritative drain 时间使用的单请求 timeout hint。 |
| `MinimumWaitTimeout` | 23s | `RecommendedWaitTimeout` 返回的最小建议等待时间。 |
| `LongSpeechCommitAfter` | 15s | 未正式提交的连续语音达到该时长后启用历史 soft-boundary 水位提交。 |
| `LongSpeechCommitPrefix` | 5s | 在当前未提交起点后的该前缀区间选择最后一个 soft boundary；必须小于 `LongSpeechCommitAfter`。 |

高层会话在 `Session.ShortSegmentMaxDuration` 和 `ShortSegmentNeighborWait` 同时为零时启用推荐默认 `6s/3s`，在两个 long-speech 字段同时为零时启用 `15s/5s`，并将默认最大窗口设为 65s。直接使用低层 `Session` 时仍保留其原有零值语义。

高层会话会保存已经确认并产生 Preview 的 soft boundary。当未提交范围达到 `LongSpeechCommitAfter` 时，它选择 `[start,start+prefix]` 内最后一个候选，重新提交从 start 到该候选的 authoritative ASR，并把候选同时解释为正式 end 和后续内部 start。此路径显式绕过短片段 neighbor wait；成功后才产生 `stable/degraded`，不会直接把非权威 Preview 文本改成 stable。如果前缀区间内没有短静音，SDK 不会在有声位置硬切。

`SpeechBoundary` 不属于应用配置，但调用方必须保证 source index 和绝对 sample index 单调。`FinalAudioChunk.FinalBoundaries` 只传 finish-time delta。

## Streaming Provider 配置边界

未来供应商 WebSocket adapter 使用 `StreamingRequest` 和 `StreamingCapabilities`。连接超时、write timeout、read idle timeout、ping interval、finalization timeout 和 server VAD 等字段属于具体 adapter 配置，不与 HTTP `ClientConfig` 或 `SegmentedSessionConfig` 混用。实时音频 FIFO 满时不能覆盖 PCM，应阻塞或返回 `ErrStreamingBackpressure`。

## `AlignmentConfig`

| 字段 | 默认值 | 作用 |
|---|---:|---|
| `MaxTokens` | 512 | 每侧参与 suffix-prefix 对齐的最大 Unicode grapheme token 数。 |
| `MinAnchors` | 4 | 可靠重叠所需的最少 anchor 数。 |
| `MinScore` | 0.82 | 归一化对齐分数下限，范围 `(0,1]`。 |
| `MinCoverage` | 0.60 | 共享文本覆盖率下限，范围 `(0,1]`。 |

这些阈值不应由普通客户端修改。需要按语言或业务 profile 调整时，应通过离线 CER/WER/MER 和边界对齐评测验证。

## Scheduler 配置语义

`ScheduledRecognizer` 当前没有队列容量参数，策略固定为：

- 一个 active 请求；
- `Authoritative=true` 进入按 `AudioEndAt` 排序的无损 FIFO；
- `Authoritative=false` 最多保留一个最新 pending preview；
- authoritative 到达时清理 pending preview，并取消 active preview；active authoritative 不取消。

调用方必须正确设置 `TranscriptionRequest.Authoritative`。VAD soft-boundary preview 使用 false；完整 VAD、双片段窗口和 tail anchor 使用 true。

## 安全与部署

- API key 只通过私密配置或环境变量注入，例如 `ASR_PROVIDERS_GENERIC_APIKEY`。
- 不把 API key、完整 prompt、音频内容写入普通日志或指标 label。
- 远程 provider 使用 HTTPS。`AllowInsecureHTTP` 只用于受控开发环境。
- 模型、endpoint、对齐阈值和 provider 选择属于服务端配置，不能由 WebSocket 客户端覆盖。
- 每个 ASR session 固定一个 provider/model；同一 evidence chain 不切换供应商。
