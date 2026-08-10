# ASR 配置参考

本文档分别描述 ASR 应用配置和 `github.com/caitunai/asr` SDK 配置。SDK 不依赖 Viper；`services/asr` 和 WebSocket handler 负责把应用配置转换成 SDK 类型。

## 应用最小配置

```toml
[asr]
enabled=true
provider="generic"
# 可选：低成本单段模式；默认 contextual
# segmentStrategy="single_segment"

[asr.providers.generic]
baseURL="https://asr.example.com"
path="/v1/audio/transcriptions"
apiKey="..."
```

Microsoft Speech REST 模式使用：

```toml
[asr]
enabled=true
provider="microsoft"

[asr.providers.microsoft]
endpoint="https://eastus.stt.speech.microsoft.com"
apiKey="..."
defaultLanguage="en-US"
```

Microsoft 模式继续使用本地 VAD、HTTP window 和相邻片段确认。区域根地址会自动补齐 conversation recognition path；`auto` 会话语言使用 `defaultLanguage`，而不是把无效的 `auto` locale 发送给服务端。

Qwen 实时模式使用：

```toml
[asr]
enabled=true
provider="qwenRealtime"

[asr.providers.qwenRealtime]
endpoint="wss://WORKSPACE_ID.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime"
apiKey="..."
model="qwen3-asr-flash-realtime"
audioChunkMs=100
```

Qwen server VAD 由 SDK 默认启用，默认阈值 `0.0`、静音确认 `400ms`，因此最小应用配置不需要写 VAD 项。下表中的 VAD key 仍可按需覆盖。

Qwen-Audio-3.0-ASR-Flash-Streaming 使用独立的 DashScope inference 协议：

```toml
[asr]
enabled=true
provider="qwenInferenceRealtime"

[asr.providers.qwenInferenceRealtime]
endpoint="wss://WORKSPACE_ID.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference"
# apiKey/workspaceID 未设置时继承 qwenRealtime。
model="qwen-audio-3.0-asr-flash-streaming"
```

它不会给 endpoint 添加 model query。应用的 16kHz mono PCM16 以 WebSocket binary frame 直接发送；`Prompt` 映射 run-task context，`Terms` 映射即时热词。

Qwen Omni 实时模式可以复用上述 endpoint、API key 和 workspace：

```toml
[asr]
enabled=true
provider="qwenOmniRealtime"

[asr.providers.qwenOmniRealtime]
model="qwen3.5-omni-plus-realtime"
```

若 Omni section 未设置 `endpoint`、`apiKey` 或 `workspaceID`，应用会分别读取 `asr.providers.qwenRealtime` 中的同名配置。SDK 默认使用 `semantic_vad`、阈值 `0.5`、静音确认 `800ms`，最小配置无需重复这些稳定参数。

OpenAI Realtime transcription 使用：

```toml
[asr]
enabled=true
provider="openaiRealtime"

[asr.providers.openaiRealtime]
apiKey="..."
```

默认模型为 `gpt-4o-mini-transcribe`。只有显式切换到 `gpt-realtime-whisper` 时才需要配置 `delay`。

Gemini Live input transcription 使用：

```toml
[asr]
enabled=true
provider="geminiRealtime"

[asr.providers.geminiRealtime]
apiKey="..."
```

默认模型为 `gemini-3.1-flash-live-preview`，自动 VAD、16kHz 重采样和长会话上下文压缩由 SDK 配置。

ElevenLabs Scribe v2 Realtime 使用：

```toml
[asr]
enabled=true
provider="elevenLabsRealtime"

[asr.providers.elevenLabsRealtime]
apiKey="..."
```

默认模型为 `scribe_v2_realtime`，使用 16kHz mono PCM16、VAD commit、300ms 静音确认和 committed stable；不发布缺少 words/置信度的 partial。当前 prompt/`previous_text` 保持禁用，terms 映射为 keyterms。ElevenLabs 默认创建受限 API key，所用 key 必须在控制台显式启用 Speech to Text（`speech_to_text`）权限，否则握手后会返回 `auth_error`。

Inworld Realtime STT 使用：

```toml
[asr]
enabled=true
provider="inworldRealtime"

[asr.providers.inworldRealtime]
apiKey="..."
```

默认模型为 `inworld/inworld-stt-1`，使用 server VAD、100ms mono PCM16 chunk，并发布 interim preview 和 final stable。SDK 负责 Basic 鉴权、`transcribeConfig`、`audioChunk`、`endTurn` 和 `closeStream` 完整生命周期；只有 terms 作为 Inworld `prompts` 发送，通用 prompt 不会变成输出语言指令。

vLLM Realtime STT 使用：

```toml
[asr]
enabled=true
provider="vllmRealtime"

[asr.providers.vllmRealtime]
endpoint="ws://127.0.0.1:8000/v1/realtime"
model="mistralai/Voxtral-Mini-4B-Realtime-2602"
# apiKey="..." # 仅在 vLLM 使用 --api-key 时配置
```

vLLM adapter 只接受 16kHz mono PCM16。`transcription.delta` 是只追加的已确认前缀，映射 provisional；`transcription.done` 是唯一 stable。协议没有 server VAD，Finish 发送一次 `input_audio_buffer.commit` 且 `final=true`，因此最终 stable 在输入结束时产生。

generic adapter 固定使用 WAV、multipart `file`、Bearer 鉴权和 `response_format=json`。请求上下文中的 prompt 去除首尾空白后非空时才发送 `prompt`；不会发送 model、language、hotwords 或 language_hints。

## 应用层完整配置

下表中的非必填项均有代码默认值。它们仍可通过 TOML 或 Viper 环境变量覆盖，但通常只在真实语料评测和容量测试后调整。

| Viper 键 | 类型 | 默认值 | 必填条件 | 作用 |
|---|---|---:|---|---|
| `asr.enabled` | bool | false | 否 | 是否在进程启动时创建 ASR provider/client。 |
| `asr.defaultEnabled` | bool | true | 否 | WebSocket `audio.start` 未提供 ASR enabled 时的会话默认值。 |
| `asr.provider` | string | `generic` | 否 | `generic`、`microsoft`、`qwenRealtime`、`qwenInferenceRealtime`、`qwenOmniRealtime`、`openaiRealtime`、`geminiRealtime`、`elevenLabsRealtime`、`inworldRealtime` 或 `vllmRealtime`。一个输入会话固定使用其中一个 provider。 |
| `asr.segmentStrategy` | string | `contextual` | 否 | HTTP 分段 provider 的识别策略。`contextual` 使用短静音 preview、邻接双窗口和对齐；`single_segment` 对每个正式 VAD/超长语音安全 Segment 只创建一个识别任务并直接 stable。实时 provider 忽略该项。 |
| `asr.defaultLanguage` | string | `auto` | 否 | 会话未指定语言时的 BCP 47 tag 或自动检测 sentinel。 |
| `asr.requestTimeout` | duration string | generic `8s`；microsoft `20s` | 否 | 单次 provider HTTP 请求超时。 |
| `asr.retryCount` | int | 1 | 否 | 同一 provider 的可恢复错误重试次数；当前只允许 0 或 1。 |
| `asr.maxConcurrency` | int | 16 | 否 | 全局 ASR client 并发上限；单会话 scheduler 仍保持一个 active。 |
| `asr.contextSilenceMs` | duration/ms | 200ms | 否 | 拼接两个有间隔 VAD segment 时插入的静音。时间连续时不插入。 |
| `asr.tailFinalizeSilenceMs` | duration/ms | 900ms | 否 | 长片段或未启用短片段等待时，从 segment EndAt 计算的尾段确认静音。 |
| `asr.tailFinalizeResultTimeoutMs` | duration/ms | 20s | 否 | 尾段正式任务完成后仍无可采用结果时的最终等待上限。 |
| `asr.shortSegmentMaxDurationMs` | duration/ms | 6s | 否 | 小于该时长的完整 VAD 优先等待相邻 VAD 上下文。 |
| `asr.shortSegmentNeighborWaitMs` | duration/ms | 3s | 否 | 短片段从 EndAt 起等待下一次 SpeechStarted 的最长时间。不得短于 tail finalize silence。 |
| `asr.longSpeechCommitAfterMs` | duration/ms | 15s | 否 | 当前未正式提交的连续语音达到该时长后，允许把历史短静音边界升级为正式 Segment。 |
| `asr.longSpeechCommitPrefixMs` | duration/ms | 5s | 否 | 在当前未提交起点后的该前缀区间内选择最后一个短静音作为正式 end，并从同一点重新 start。必须小于 commit after。 |
| `asr.maxWindowMs` | duration/ms | generic `65s`；microsoft `55s` | 否 | 单个 standalone/pair ASR 音频窗口的最大时长。Microsoft short-audio REST 硬上限为 60 秒。 |
| `asr.stopTimeoutMs` | duration/ms | 23s | 否 | WebSocket stop 等待 ASR completed 的最小时间；会按正式任务数量自动扩展。 |
| `asr.tailAnchorEnabled` | bool | true | 否 | 尾段缺少下一窗口时是否提交同 provider 的 standalone anchor。 |
| `asr.providers.generic.baseURL` | string | 无 | generic 启用时必填 | HTTP 服务根地址。远程必须 HTTPS；localhost/loopback 可用 HTTP。 |
| `asr.providers.generic.path` | string | 无 | generic 启用时必填 | 转写接口路径，例如 `/v1/audio/transcriptions`。 |
| `asr.providers.generic.apiKey` | string | 无 | generic 启用时必填 | Bearer API key。应通过环境变量或私密配置注入，不能进入日志或前端。 |
| `asr.providers.microsoft.endpoint` | string | 无 | microsoft 启用时必填 | Microsoft Speech 区域根地址或完整 conversation recognition URL。`wss/ws` 会对应转换成 `https/http`；远程生产地址必须为 HTTPS。 |
| `asr.providers.microsoft.apiKey` | string | 无 | microsoft 启用时必填 | Speech subscription key 或 Bearer token，只进入供应商鉴权 header。 |
| `asr.providers.microsoft.model` | string | `speech-recognition-conversation` | 否 | 结果元数据中的稳定模型标识，不作为 REST query 发送。 |
| `asr.providers.microsoft.authMode` | string | `auto` | 否 | `auto`、`subscription_key` 或 `bearer`。auto 根据 Bearer 前缀/JWT 形态选择鉴权 header。 |
| `asr.providers.microsoft.defaultLanguage` | string | `en-US` | 否 | 会话语言为 `auto` 时实际发送的 Microsoft locale。 |
| `asr.providers.microsoft.responseBodyLimit` | int | 2 MiB | 否 | 单次 JSON 响应体字节上限。 |
| `asr.providers.microsoft.allowInsecureHTTP` | bool | false | 否 | 允许非 loopback 明文 HTTP，仅限受控开发网络。 |
| `asr.providers.qwenRealtime.endpoint` | string | 无 | qwenRealtime 启用时必填 | 供应商实时 WebSocket URL；远程地址必须使用 `wss`。model query 由 adapter 写入。 |
| `asr.providers.qwenRealtime.apiKey` | string | 无 | qwenRealtime 启用时必填 | 百炼 API key，只用于供应商侧 Authorization header。 |
| `asr.providers.qwenRealtime.model` | string | `qwen3-asr-flash-realtime` | 否 | 实时模型名称。 |
| `asr.providers.qwenRealtime.workspaceID` | string | 空 | 否 | 可选 `X-DashScope-WorkSpace` header；workspace 已在 endpoint 子域中时可留空。 |
| `asr.providers.qwenRealtime.serverVADEnabled` | bool | SDK 默认 true | 否 | 覆盖供应商 server VAD。显式设为 false 时整次输入在 Finish 时统一 commit。 |
| `asr.providers.qwenRealtime.serverVADThreshold` | float | SDK 默认 0.0 | 否 | 覆盖 server VAD 阈值，范围 `[-1,1]`。 |
| `asr.providers.qwenRealtime.serverVADSilenceMs` | duration/ms | SDK 默认 400ms | 否 | 覆盖 server VAD 静音确认时长，范围 200ms–6s。 |
| `asr.providers.qwenRealtime.audioChunkMs` | duration/ms | 100ms | 否 | 聚合后发送给供应商的 PCM chunk 时长，范围 20ms–1s；Finish 会刷新不足一块的尾音频。 |
| `asr.providers.qwenRealtime.handshakeTimeout` | duration | 10s | 否 | 等待 `session.updated` 的上限。 |
| `asr.providers.qwenRealtime.writeTimeout` | duration | 5s | 否 | 单次供应商 WebSocket 写入上限。 |
| `asr.providers.qwenRealtime.finishTimeout` | duration | 20s | 否 | 发送 `session.finish` 后等待 `session.finished` 的上限。 |
| `asr.providers.qwenRealtime.eventBuffer` | int | 128 | 否 | Provider 和统一 session 的事件缓冲。 |
| `asr.providers.qwenRealtime.allowInsecureWebSocket` | bool | false | 否 | 允许非 loopback `ws`，仅限受控开发环境。生产必须用 `wss`。 |
| `asr.providers.qwenInferenceRealtime.endpoint` | string | 无 | qwenInferenceRealtime 启用时必填 | 固定 `/api-ws/v1/inference` WebSocket URL；不允许 query，远程必须为 `wss`。 |
| `asr.providers.qwenInferenceRealtime.apiKey` | string | 继承 qwenRealtime | 无法继承时必填 | 百炼 API key，只进入 Bearer Authorization header。 |
| `asr.providers.qwenInferenceRealtime.model` | string | `qwen-audio-3.0-asr-flash-streaming` | 否 | `run-task.payload.model`。同一协议也可配置受支持的 Fun-ASR-Realtime 模型。 |
| `asr.providers.qwenInferenceRealtime.workspaceID` | string | 继承 qwenRealtime | 否 | 可选 `X-DashScope-WorkSpace` header。 |
| `asr.providers.qwenInferenceRealtime.userAgent` | string | 空 | 否 | 可选客户端标识，经校验后写入 `User-Agent` header。 |
| `asr.providers.qwenInferenceRealtime.vocabularyID` | string | 空 | 否 | 预编译热词列表 ID；存在会话 Terms 时即时 vocabulary 优先生效。 |
| `asr.providers.qwenInferenceRealtime.vocabularyWeight` | int | 5 | 否 | 会话 Terms 的即时热词权重，支持 1–5 或 50；权重 50 时最多发送 50 个。 |
| `asr.providers.qwenInferenceRealtime.semanticPunctuationEnabled` | bool | false | 否 | true 使用语义断句，false 使用供应商 VAD 断句。 |
| `asr.providers.qwenInferenceRealtime.maxSentenceSilenceMs` | duration/ms | 1300ms | 否 | VAD 句尾静音，范围 200ms–6s。 |
| `asr.providers.qwenInferenceRealtime.multiThresholdModeEnabled` | bool | false | 否 | VAD 模式下启用多阈值，避免单句过长。语义断句时不生效。 |
| `asr.providers.qwenInferenceRealtime.heartbeat` | bool | false | 否 | 持续静音时启用供应商心跳；心跳结果不会发布为文本。 |
| `asr.providers.qwenInferenceRealtime.speechNoiseThreshold` | float | 供应商默认 | 否 | 语音/噪声阈值，范围 -1–1。 |
| `asr.providers.qwenInferenceRealtime.specialWordFilter` | string | 空 | 否 | 原样传入供应商敏感词过滤配置。 |
| `asr.providers.qwenInferenceRealtime.automaticContextEnabled` | bool | true | 否 | 收到 stable 后，是否在下一块音频前通过 `continue-task` 自动发送最近识别上下文。 |
| `asr.providers.qwenInferenceRealtime.contextHistoryItems` | int | 4 | 否 | SDK 保存的最近 stable 条数；DashScope 最终最多发送 5 条 input_text，初始 Prompt 会占一条。 |
| `asr.providers.qwenInferenceRealtime.contextHistoryRunes` | int | 400 | 否 | SDK stable 历史字符上限；Provider 还会确保 Prompt 与历史合计不超过供应商的 400 字符限制。 |
| `asr.providers.qwenInferenceRealtime.audioChunkMs` | duration/ms | 100ms | 否 | 聚合后以 binary frame 发送的 PCM chunk 时长。 |
| `asr.providers.qwenInferenceRealtime.handshakeTimeout` | duration | 10s | 否 | 等待 `task-started` 的上限。 |
| `asr.providers.qwenInferenceRealtime.writeTimeout` | duration | 5s | 否 | run/finish task 和 binary audio 单次写入上限。 |
| `asr.providers.qwenInferenceRealtime.finishTimeout` | duration | 20s | 否 | `finish-task` 后等待 `task-finished` 的上限。 |
| `asr.providers.qwenInferenceRealtime.eventBuffer` | int | 128 | 否 | Provider 与统一 session 的事件缓冲。 |
| `asr.providers.qwenInferenceRealtime.allowInsecureWebSocket` | bool | false | 否 | 允许非 loopback `ws`，仅用于受控测试。 |
| `asr.providers.qwenOmniRealtime.endpoint` | string | 继承 qwenRealtime | Omni 启用且无法继承时必填 | Omni 实时 WebSocket URL。 |
| `asr.providers.qwenOmniRealtime.apiKey` | string | 继承 qwenRealtime | Omni 启用且无法继承时必填 | 百炼 API key。 |
| `asr.providers.qwenOmniRealtime.workspaceID` | string | 继承 qwenRealtime | 否 | 可选 workspace header。 |
| `asr.providers.qwenOmniRealtime.model` | string | `qwen3.5-omni-plus-realtime` | 否 | Omni 会话模型；Adapter 不指定 `input_audio_transcription.model`。 |
| `asr.providers.qwenOmniRealtime.turnDetectionType` | string | `semantic_vad` | 否 | `semantic_vad` 或 `server_vad`。 |
| `asr.providers.qwenOmniRealtime.serverVADEnabled` | bool | SDK 默认 true | 否 | 是否由供应商自动切句；false 时 Finish 发送 commit。 |
| `asr.providers.qwenOmniRealtime.vadThreshold` | float | SDK 默认 0.5 | 否 | VAD 阈值，范围 `[-1,1]`。 |
| `asr.providers.qwenOmniRealtime.vadSilenceMs` | duration/ms | SDK 默认 800ms | 否 | VAD 静音确认时长，范围 200ms–6s。 |
| `asr.providers.qwenOmniRealtime.instructions` | string | SDK 的 ASR-only 指令 | 否 | 给 Omni 主模型的指令；不会作为输入转写 prompt。 |
| `asr.providers.qwenOmniRealtime.keepModelResponses` | bool | false | 否 | 是否保留 Omni 对话回答。ASR-only 场景应保持 false。 |
| `asr.providers.qwenOmniRealtime.audioChunkMs` | duration/ms | 100ms | 否 | 聚合后发送的 PCM chunk 时长。 |
| `asr.providers.qwenOmniRealtime.handshakeTimeout` | duration | 10s | 否 | 等待 `session.updated` 的上限。 |
| `asr.providers.qwenOmniRealtime.writeTimeout` | duration | 5s | 否 | 单次供应商 WebSocket 写入上限。 |
| `asr.providers.qwenOmniRealtime.finishTimeout` | duration | 20s | 否 | 输入结束后等待最后转写结果的硬上限。 |
| `asr.providers.qwenOmniRealtime.eventBuffer` | int | 128 | 否 | Provider 与统一 session 事件缓冲。 |
| `asr.providers.qwenOmniRealtime.allowInsecureWebSocket` | bool | false | 否 | 允许非 loopback `ws`，仅限受控开发环境。 |
| `asr.providers.openaiRealtime.endpoint` | string | `wss://api.openai.com/v1/realtime` | 否 | OpenAI Realtime WebSocket URL；SDK 自动添加 `intent=transcription`。 |
| `asr.providers.openaiRealtime.model` | string | `gpt-4o-mini-transcribe` | 否 | OpenAI 实时转写模型名称。 |
| `asr.providers.openaiRealtime.apiKey` | string | 无 | openaiRealtime 启用时必填 | 只用于供应商 WebSocket 握手的 Bearer Authorization header。 |
| `asr.providers.openaiRealtime.delay` | string | `medium` | 否 | 仅用于 `gpt-realtime-whisper` 的 `minimal/low/medium/high/xhigh` 准确率与延迟策略；其他模型不发送该字段。 |
| `asr.providers.openaiRealtime.turnDetectionEnabled` | bool | true | 否 | 是否使用 OpenAI 远端 turn detection；`gpt-realtime-whisper` 会自动禁用。 |
| `asr.providers.openaiRealtime.turnDetectionType` | string | `semantic_vad` | 否 | `semantic_vad` 根据语义完整性断句；`server_vad` 根据静音断句。 |
| `asr.providers.openaiRealtime.semanticVADEagerness` | string | `auto` | 否 | semantic VAD 的 `low`、`medium`、`high` 或 `auto`；越高通常越快断句。 |
| `asr.providers.openaiRealtime.serverVADThreshold` | float | `0.5` | 否 | server VAD 语音激活阈值，范围 0–1。 |
| `asr.providers.openaiRealtime.serverVADPrefixPaddingMs` | duration/ms | 300ms | 否 | server VAD 在语音起点前保留的音频。 |
| `asr.providers.openaiRealtime.serverVADSilenceMs` | duration/ms | 800ms | 否 | server VAD 确认一句结束所需的连续静音。 |
| `asr.providers.openaiRealtime.commitIntervalMs` | duration/ms | 3s | 否 | 关闭 turn detection 或使用 `gpt-realtime-whisper` 时的手动提交周期，范围 500ms–30s。 |
| `asr.providers.openaiRealtime.audioChunkMs` | duration/ms | 100ms | 否 | 应用聚合后交给 adapter 的 PCM chunk 时长。 |
| `asr.providers.openaiRealtime.handshakeTimeout` | duration | 10s | 否 | 等待 `session.updated` 的上限。 |
| `asr.providers.openaiRealtime.writeTimeout` | duration | 5s | 否 | 单次 WebSocket 写入上限。 |
| `asr.providers.openaiRealtime.finishTimeout` | duration | 20s | 否 | 尾部 commit 后等待全部 final 结果的上限。 |
| `asr.providers.openaiRealtime.eventBuffer` | int | 128 | 否 | Provider 与统一 session 的事件缓冲。 |
| `asr.providers.openaiRealtime.allowInsecureWebSocket` | bool | false | 否 | 允许非 loopback `ws`，仅用于受控测试。 |
| `asr.providers.geminiRealtime.endpoint` | string | Gemini v1beta BidiGenerateContent WSS | 否 | Gemini Live WebSocket URL；API key 由 SDK 写入认证 query，不记录到日志。 |
| `asr.providers.geminiRealtime.model` | string | `gemini-3.1-flash-live-preview` | 否 | Gemini Live 模型名称，可带或不带 `models/` 前缀。 |
| `asr.providers.geminiRealtime.apiKey` | string | 无 | geminiRealtime 启用时必填 | Gemini Developer API key。 |
| `asr.providers.geminiRealtime.systemInstruction` | string | SDK 极简确认指令 | 否 | 要求模型在每轮只给极短中性确认；输出音频会被 adapter 丢弃。可按部署需要覆盖。 |
| `asr.providers.geminiRealtime.startOfSpeechSensitivity` | string | `START_SENSITIVITY_HIGH` | 否 | 自动 VAD 起音灵敏度，支持 `START_SENSITIVITY_HIGH/LOW`。 |
| `asr.providers.geminiRealtime.endOfSpeechSensitivity` | string | `END_SENSITIVITY_HIGH` | 否 | 自动 VAD 结束灵敏度，支持 `END_SENSITIVITY_HIGH/LOW`；默认 HIGH 让较短停顿也能及时结束输入 turn。长篇慢速口述若切分过碎可改为 LOW。 |
| `asr.providers.geminiRealtime.prefixPaddingMs` | duration/ms | 300ms | 否 | 起音检测前保留的音频，降低首音节截断风险。 |
| `asr.providers.geminiRealtime.silenceDurationMs` | duration/ms | 300ms | 否 | 自动 VAD 确认一轮结束的静音长度。面向低延迟 ASR 默认使用 300ms，使新闻播报中的短停顿更快形成 turn；若慢速口述被切分过碎，可调回 500–800ms。 |
| `asr.providers.geminiRealtime.continuousTurnFlushEnabled` | bool | true | 否 | 连续语音长期没有服务端 VAD 边界时，是否使用可恢复的 `audioStreamEnd` 强制刷新当前输入流。关闭后，无停顿音频可能直到 Finish 才返回转写。 |
| `asr.providers.geminiRealtime.maxContinuousTurnMs` | duration/ms | 15s | 否 | 距离最近一次 Gemini turn boundary 的最大连续音频时长，范围 2s–5min。达到后会在下一音频块前刷新，连接和上下文保持不变。 |
| `asr.providers.geminiRealtime.finalTranscriptDrainMs` | duration/ms | 500ms | 否 | 收到生成/轮次完成后继续等待无序 inputTranscription 事件的窗口。 |
| `asr.providers.geminiRealtime.finishIdleTimeoutMs` | duration/ms | 2s | 否 | `audioStreamEnd` 后未收到明确边界时，以转写空闲确认尾段的回退窗口。 |
| `asr.providers.geminiRealtime.audioChunkMs` | duration/ms | 40ms | 否 | 应用聚合后发送给 Gemini 的 PCM chunk 时长。 |
| `asr.providers.geminiRealtime.contextWindowCompressionEnabled` | bool | true | 否 | 是否启用 sliding-window 上下文压缩以支持长会话。 |
| `asr.providers.geminiRealtime.handshakeTimeout` | duration | 10s | 否 | 等待 `setupComplete` 的上限。 |
| `asr.providers.geminiRealtime.writeTimeout` | duration | 5s | 否 | 单次 WebSocket 写入上限。 |
| `asr.providers.geminiRealtime.finishTimeout` | duration | 20s | 否 | 输入结束后等待尾部转写的硬上限。 |
| `asr.providers.geminiRealtime.eventBuffer` | int | 128 | 否 | Provider 与统一 session 的事件缓冲。 |
| `asr.providers.geminiRealtime.allowInsecureWebSocket` | bool | false | 否 | 允许非 loopback `ws`，仅用于受控测试。 |
| `asr.providers.elevenLabsRealtime.endpoint` | string | `wss://api.elevenlabs.io/v1/speech-to-text/realtime` | 否 | ElevenLabs Realtime STT WebSocket URL；可切换到官方区域驻留 endpoint。 |
| `asr.providers.elevenLabsRealtime.model` | string | `scribe_v2_realtime` | 否 | Realtime STT 模型 ID。 |
| `asr.providers.elevenLabsRealtime.apiKey` | string | 无 | elevenLabsRealtime 启用时必填 | 必须具备 `speech_to_text` 权限；只放入 `xi-api-key` 握手 header，不写入 URL、事件或日志。 |
| `asr.providers.elevenLabsRealtime.commitStrategy` | string | `vad` | 否 | `vad` 由供应商自动提交语音段；`manual` 由 SDK 周期提交。 |
| `asr.providers.elevenLabsRealtime.vadSilenceThresholdMs` | duration/ms | 300ms | 否 | VAD 提交所需静音，范围 300ms–3s。低延迟默认使用允许的最小值。 |
| `asr.providers.elevenLabsRealtime.vadThreshold` | float | 0.4 | 否 | VAD 阈值，范围 0.1–0.9。 |
| `asr.providers.elevenLabsRealtime.minSpeechDurationMs` | duration/ms | 100ms | 否 | 最短语音活动，范围 50ms–2s。 |
| `asr.providers.elevenLabsRealtime.minSilenceDurationMs` | duration/ms | 100ms | 否 | 最短静音活动，范围 50ms–2s。 |
| `asr.providers.elevenLabsRealtime.manualCommitIntervalMs` | duration/ms | 20s | 否 | manual 模式的周期提交间隔，范围 1s–30s；VAD 模式不使用。 |
| `asr.providers.elevenLabsRealtime.includeTimestamps` | bool | true | 否 | 是否请求 word-level timestamps。 |
| `asr.providers.elevenLabsRealtime.includeLanguageDetection` | bool | true | 否 | 是否在 timestamp committed event 中请求检测语言；timestamps 关闭时 SDK 自动关闭此项。 |
| `asr.providers.elevenLabsRealtime.noVerbatim` | bool | false | 否 | 是否移除填充词、口误和不流畅表达。严格逐字稿应保持 false。 |
| `asr.providers.elevenLabsRealtime.filterBackgroundAudio` | bool | false | 否 | 是否过滤背景说话和环境声；供应商不允许与 timestamps 同时启用，因此启用时必须同时设置 `includeTimestamps=false`。 |
| `asr.providers.elevenLabsRealtime.enableLogging` | bool | true | 否 | 是否允许供应商记录请求；false 的零保留模式仅适用于具备权限的企业账户。 |
| `asr.providers.elevenLabsRealtime.emitPartials` | bool | false | 否 | 是否把 `partial_transcript` 发布为 preview。ElevenLabs partial 不含 words/置信度，默认关闭以避免显示随后被 final 撤回的猜测。 |
| `asr.providers.elevenLabsRealtime.minTranscriptLogProb` | float | -5.0 | 否 | 带 timestamps final 的最小 lexical word 平均 logprob；低于阈值的低置信度尾部幻觉会被丢弃。范围 -20–0，越接近 0 越严格。 |
| `asr.providers.elevenLabsRealtime.audioChunkMs` | duration/ms | 100ms | 否 | PCM chunk 时长；官方建议 100ms–1s。 |
| `asr.providers.elevenLabsRealtime.handshakeTimeout` | duration | 10s | 否 | 等待 `session_started` 的上限。 |
| `asr.providers.elevenLabsRealtime.writeTimeout` | duration | 5s | 否 | 单次 WebSocket 写入上限。 |
| `asr.providers.elevenLabsRealtime.finishTimeout` | duration | 20s | 否 | 尾部强制 commit 后等待 committed transcript 的硬上限。 |
| `asr.providers.elevenLabsRealtime.eventBuffer` | int | 128 | 否 | Provider 与统一 session 的事件缓冲。 |
| `asr.providers.elevenLabsRealtime.allowInsecureWebSocket` | bool | false | 否 | 允许非 loopback `ws`，仅用于受控测试。 |
| `asr.providers.inworldRealtime.endpoint` | string | `wss://api.inworld.ai/stt/v1/transcribe:streamBidirectional` | 否 | Inworld bidirectional STT WebSocket URL。 |
| `asr.providers.inworldRealtime.model` | string | `inworld/inworld-stt-1` | 否 | `provider/model` 格式的 Inworld STT model ID。 |
| `asr.providers.inworldRealtime.apiKey` | string | 无 | inworldRealtime 启用时必填 | SDK 只将它写入 Basic Authorization header。 |
| `asr.providers.inworldRealtime.serverVADEnabled` | bool | true | 否 | 是否使用 Inworld STT 1 服务端 turn detection；false 时发送 `vadThreshold=0`。 |
| `asr.providers.inworldRealtime.vadThreshold` | float | 0.5 | 否 | Inworld STT 1 VAD 阈值，范围 0–1。 |
| `asr.providers.inworldRealtime.minEndOfTurnSilenceMs` | duration/ms | 供应商默认 | 否 | 高置信度时确认 turn end 的最小静音毫秒数。 |
| `asr.providers.inworldRealtime.endOfTurnConfidenceThreshold` | float | 0.5（供应商默认） | 否 | end-of-turn 预测置信度阈值，范围 0–1；越高越不易误断句。 |
| `asr.providers.inworldRealtime.inactivityTimeoutSeconds` | int | 供应商默认 | 否 | 输入静默多少秒后停止转写；整数按秒解析。 |
| `asr.providers.inworldRealtime.includeWordTimestamps` | bool | false | 否 | 请求 word timestamps；`inworld/inworld-stt-1` 暂不支持，仅在所选模型支持时启用。 |
| `asr.providers.inworldRealtime.emitPartials` | bool | true | 否 | 是否把 interim transcription 映射为 preview。 |
| `asr.providers.inworldRealtime.audioChunkMs` | duration/ms | 100ms | 否 | 应用聚合后交给 Inworld adapter 的 PCM chunk 时长。 |
| `asr.providers.inworldRealtime.handshakeTimeout` | duration | 10s | 否 | WebSocket HTTP upgrade 的超时上限。 |
| `asr.providers.inworldRealtime.writeTimeout` | duration | 5s | 否 | 单次 WebSocket 配置/音频/结束事件写入上限。 |
| `asr.providers.inworldRealtime.finishTimeout` | duration | 20s | 否 | `endTurn` 后等待尾部 final 的硬上限；final 后 SDK 发送 `closeStream` 并主动关闭。 |
| `asr.providers.inworldRealtime.eventBuffer` | int | 128 | 否 | Provider 与统一 session 的事件缓冲。 |
| `asr.providers.inworldRealtime.allowInsecureWebSocket` | bool | false | 否 | 允许非 loopback `ws`，仅用于受控测试。 |
| `asr.providers.vllmRealtime.endpoint` | string | `ws://127.0.0.1:8000/v1/realtime` | 否 | vLLM Realtime STT WebSocket URL。非 loopback 生产服务应使用 `wss`。 |
| `asr.providers.vllmRealtime.model` | string | `mistralai/Voxtral-Mini-4B-Realtime-2602` | 否 | `session.update` 中校验的 served model 名称；使用 `--served-model-name` 时需保持一致。 |
| `asr.providers.vllmRealtime.apiKey` | string | 空 | 否 | vLLM 使用 `--api-key` 时发送为 Bearer header；未启用鉴权时留空。 |
| `asr.providers.vllmRealtime.audioChunkMs` | duration/ms | 100ms | 否 | 聚合后发送给 vLLM 的 PCM chunk 时长，范围 20ms–1s。 |
| `asr.providers.vllmRealtime.handshakeTimeout` | duration | 10s | 否 | 等待服务端 `session.created` 的上限。 |
| `asr.providers.vllmRealtime.writeTimeout` | duration | 5s | 否 | 单次 session/update/commit/audio WebSocket 写入上限。 |
| `asr.providers.vllmRealtime.finishTimeout` | duration | 20s | 否 | final commit 后等待 `transcription.done` 的硬上限。 |
| `asr.providers.vllmRealtime.eventBuffer` | int | 128 | 否 | Provider 与统一 session 的事件缓冲。 |
| `asr.providers.vllmRealtime.allowInsecureWebSocket` | bool | false | 否 | 允许非 loopback `ws`，仅用于受控内网；loopback 默认允许。 |

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

## `MicrosoftHTTPConfig`

| 字段 | 默认值 | 作用 |
|---|---:|---|
| `Name` | `microsoft` | Provider 稳定名称。 |
| `Model` | `speech-recognition-conversation` | 结果元数据模型名，不发送给 Microsoft。 |
| `Endpoint` | 无 | 区域根地址、Azure resource 根地址或完整 REST conversation recognition URL，必填；SDK 自动补齐对应路径。 |
| `APIKey` | 无 | subscription key 或 Bearer token，必填。 |
| `AuthMode` | `auto` | `auto`、`subscription_key`、`bearer`。 |
| `DefaultLanguage` | `en-US` | 请求 language 为 `auto` 时使用的 locale。 |
| `ResponseBodyLimit` | 2 MiB | 响应体大小上限。 |
| `AllowInsecureHTTP` | false | 允许非 loopback HTTP；生产环境应保持 false。 |

Provider 只接受 16kHz mono WAV PCM16，并拒绝超过 60 秒的 payload。请求使用实际采样率构造 `Content-Type`，发送 `format=detailed` 和规范化 locale。`DisplayText` 优先，随后回退到第一个 `NBest.Display` 或 `NBest.Lexical`；Microsoft 100ns tick word timestamps 会转换成秒。`NoMatch`、initial silence、babble timeout 和 end-of-dictation 空结果统一分类为 `ErrNoSpeech`。该 provider 不发送 `RecognitionContext` 中的 prompt、terms、extra fields 或 language hints。

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
| `SegmentStrategy` | `contextual` | `asr.segmentStrategy` | `contextual` 或 `single_segment`。single 模式禁用短边界识别和邻接窗口，只保留单段正式任务；底层 HTTP 尝试次数仍由 `RetryCount` 控制。 |
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

## `QwenRealtimeConfig` 与实时会话

Qwen adapter 使用 `StreamingRequest` 和 `StreamingCapabilities`。`Endpoint`、`APIKey`、`Model`、server VAD、握手/写入/结束超时属于 adapter 配置，不与 HTTP `ClientConfig` 或 `SegmentedSessionConfig` 混用。`RealtimeSession` 串行写入每个 PCM chunk；前一次写入未完成时后续 `Push` 会背压，绝不覆盖或跳过音频。

实时会话只支持单声道 raw PCM16，Qwen adapter 接受 8kHz 或 16kHz。`RecognitionContext.Prompt` 和去重后的 `Terms` 按行组成 `input_audio_transcription.corpus.text`；语言不是 `auto` 时发送 `language`，否则可取第一个 language hint。partial 的已确认 `text` 与可变 `stash` 原样拼接，最终 `transcript` 产生 `provider_final` stable 结果。

## `DashScopeInferenceRealtimeConfig`

DashScope Inference adapter 实现 `/api-ws/v1/inference` 的 `run-task` 协议，默认模型为 `qwen-audio-3.0-asr-flash-streaming`。`Endpoint` 和 `APIKey` 必填；远程 endpoint 必须使用 `wss`、固定以 `/api-ws/v1/inference` 结尾且不能携带 query。`WorkspaceID` 与 `UserAgent` 分别映射到 `X-DashScope-WorkSpace` 和 `User-Agent` 请求头。API key 只写入 Bearer Authorization header。

Provider 只接受单声道 raw PCM16；采样率在 `run-task.payload.parameters.sample_rate` 中声明，PCM 数据直接使用 WebSocket Binary Message 发送，不做 Base64 编码或无意义重采样。`MaxSentenceSilence` 默认 1300ms、允许 200ms–6s；`SemanticPunctuationEnabled`、`MultiThresholdModeEnabled`、`Heartbeat`、`SpeechNoiseThreshold` 和 `SpecialWordFilter` 原样映射为服务端断句参数。`VocabularyID` 配置预编译热词；会话 `RecognitionContext.Terms` 映射为即时 vocabulary，`VocabularyWeight` 支持 1–5 或 50，权重 50 时最多发送 50 个词。

显式语言及 language hints 会转换为供应商支持的主语言代码，最多四种；显式语言优先。`RecognitionContext.Prompt` 映射为单条 user/input_text context，并按供应商限制截断至 400 个 Unicode 字符。`result-generated` 中同一个 `sentence_id` 的非句尾结果产生 provisional 修订，`sentence_end=true` 产生 stable；heartbeat 不会输出文本。CloseInput 发送 `finish-task`，会话只在收到 `task-finished` 后完成，或由 `FinishTimeout` 终止。

Provider 同时实现 `ProviderContextUpdater.UpdateContext`，将统一的 `StreamingContextUpdate` 映射为 `continue-task`。`RealtimeSession` 默认自动保存最近 4 条 provider-final stable 结果、最多 400 个 Unicode 字符；下一块 PCM 写入前若上下文有更新，会先同步写入 `continue-task`。供应商没有 `task-continued` 响应事件，因此 SDK 只等待 WebSocket 写入完成，不增加虚假的确认等待。可通过 `RealtimeSessionConfig.ContextUpdate` 禁用或调整有界历史；partial/provisional、discarded 和错误结果不会进入上下文。应用也可通过 `AudioSessionContextUpdater.UpdateContext` 主动替换当前 Prompt；显式更新不受 `DisableAutomatic` 影响，Terms 仍只能在 `run-task` 时设置。

## `QwenOmniRealtimeConfig`

Omni adapter 与普通 Qwen realtime adapter 共享连接、串行 PCM writer、事件和错误分类，但协议策略不同：只接受 16kHz 单声道 raw PCM16；默认 `semantic_vad`；读取 `input_audio_transcription.delta/completed`；默认取消自动创建的模型回答；输入结束时不发送普通 Qwen ASR 协议的 `session.finish`。

`TurnDetectionType` 可设为 `semantic_vad` 或 `server_vad`，`VADThreshold` 使用指针区分“未设置”和显式 `0`。`DisableServerVAD=true` 时由 `CloseInput` 发送 `input_audio_buffer.commit`。`KeepModelResponses=true` 只会停止自动 cancel，SDK 仍不会把 Omni 回答作为 ASR 文本事件输出。

Adapter 使用空对象 `input_audio_transcription: {}` 启用输入转写，不发送其 `model` 字段，由服务端选择默认实现。`Instructions` 只影响 Omni 主模型，不会成为转写 prompt；`StreamingCapabilities` 因此明确声明不支持 prompt、terms 和 language hints。

## `OpenAIRealtimeConfig`

OpenAI adapter 直接使用 GA Realtime transcription 协议。`Endpoint` 和 `Model` 分别默认采用 `wss://api.openai.com/v1/realtime` 与 `gpt-4o-mini-transcribe`；API key 只放在 Bearer Authorization header，不写入 URL、事件或日志。Adapter 默认使用 `semantic_vad` 和 `eagerness=auto`；需要严格按静音断句时可切换为 `server_vad`，需要应用自行控制边界时可关闭 turn detection。显式选择 `gpt-realtime-whisper` 时 SDK 自动关闭 VAD，并且只有该模型会收到 `Delay`。

Provider 接受 8kHz、16kHz 或 24kHz 单声道 raw PCM16，并在 adapter 内连续重采样为 OpenAI 要求的 24kHz。启用 VAD 时不运行固定周期 commit，服务端自动 commit 的每个 item 都会被跟踪；CloseInput 会补尾部静音并手动 commit，只在全部已知 item 返回 completed/failed 后完成。自动 commit 与尾部 commit 竞态造成的空 buffer 错误属于正常收尾，不作为识别错误上报。服务端 delta 按 `item_id` 累积为 provisional，completed 输出 stable。当前 adapter 未暴露 prompt steering，因此 capability 明确为 false；该模式也不进入 HTTP 相邻窗口对齐。

## `GeminiRealtimeConfig`

Gemini adapter 直接实现 Live API 的 `BidiGenerateContent` raw WebSocket 协议。连接后首包发送 `setup`，并等待 `setupComplete` 后才发送音频；setup 固定启用 `inputAudioTranscription`、`AUDIO` response modality、自动 activity detection 和 `START_OF_ACTIVITY_INTERRUPTS`。模型输出音频不进入 ASR 结果，`interrupted` 只描述上一轮模型输出被打断，不会被错误地当作当前输入语音的结束边界。

Provider 接受常见单声道 raw PCM16 采样率；16kHz 直接发送，其他采样率连续重采样为 Gemini 要求的 16kHz。输入以 40ms chunk 发送；`inputTranscription.text` 同时兼容增量和累积更新，先输出 provisional。由于 Gemini 明确不保证 input transcription 与 `serverContent` 的到达顺序，generation/turn complete 后保留短 drain 窗口，再输出 stable。CloseInput 发送 `audioStreamEnd=true`，明确边界缺失时使用可配置 idle 回退，并始终受 `FinishTimeout` 硬上限保护。

SDK 默认采用高起音灵敏度、高结束灵敏度、300ms prefix padding 和 300ms silence duration。长会话默认启用 sliding-window context compression；连续 15s 无自然边界时使用可恢复的 `audioStreamEnd` 刷新。当前 adapter 不把 `RecognitionContext` 声明为 Gemini input transcription 的准确率提示能力，因此 prompt、terms 和 language hints capability 均为 false。

## `ElevenLabsRealtimeConfig`

ElevenLabs adapter 直接连接 `/v1/speech-to-text/realtime`，握手使用 `xi-api-key` header，默认模型为 `scribe_v2_realtime`。它支持供应商列出的 8kHz、16kHz、22.05kHz、24kHz、44.1kHz 和 48kHz 单声道 PCM16，并根据输入采样率设置对应 `pcm_*` 格式，不执行无意义的同采样率转换。

默认使用 VAD commit、0.4 阈值、300ms VAD silence、100ms 最短语音和静音。manual 模式按 `ManualCommitInterval` 周期把当前音频块标记为 commit；无论哪种模式，CloseInput 都追加 100ms 尾部静音并强制 commit，等待 committed event 或受 `FinishTimeout` 限制。ElevenLabs partial 不含 words 或置信度，因此默认不发布；显式启用 `EmitPartials` 后才映射为 preview。timestamps 关闭时由 `committed_transcript` 产生 stable；timestamps 开启时，该事件会被忽略，随后到达的 `committed_transcript_with_timestamps` 才产生一次 stable，防止同一提交重复输出。final 是权威结果：空 final 或只包含标点/符号的 final 都表示不能确认当前 partial，绝不会回退到旧 partial；lexical word 平均 logprob 低于 `MinTranscriptLogProb` 的 final 也会被拒绝。若该 turn 已输出 preview，SDK 会发送 `discarded` 修订，让消费者删除该不稳定结果。

当前 adapter 忽略 `RecognitionContext.Prompt`，不发送 `previous_text`，并将 `SupportsPrompt` 声明为 false。去重后的 terms 仍转为握手 query 中的 keyterms；受供应商限制最多发送 50 个、每个最多 20 个 Unicode 字符，超限项会被忽略。明确语言转换为 ISO 主语言代码，`auto` 不发送 `language_code`。`FilterBackgroundAudio` 与 timestamps 互斥，配置同时启用会在启动时返回 `ErrInvalidConfig`。

## `InworldRealtimeConfig`

Inworld adapter 首包发送 `transcribeConfig`，然后按序发送 base64 `audioChunk`。它接受常见 8kHz–48kHz 单声道 raw PCM16，并向服务端明确声明原始 sample rate，不做无意义的 16kHz 到 16kHz 重采样。网关响应使用 `result.transcription`：interim 作为 preview，final 作为 stable；空 final 会撤回已发布 preview。CloseInput 先发送 `endTurn`，收到尾部 final 后再发送 `closeStream`、设置成功结果并主动关闭连接，不依赖服务端 WebSocket close frame。

默认模型使用 `inworldSttV1Config.vadThreshold=0.5`；`DisableServerVAD` 只适用于 `inworld/inworld-stt-1`，SDK 会发送阈值 0 改用 manual `endTurn`。Inworld `prompts` 的语义是专有名词/关键词偏置，SDK 因此忽略 `RecognitionContext.Prompt`，只发送去重 terms；含有网关明确拒绝的 `#`/`/`/`@`/`|` 或控制字符时会在连接前失败。只有明确设置的 language 会转成 ISO 主语言代码；`auto` 不发送 language，language hints 也不会被升级为单一语言约束。

## `VLLMRealtimeConfig`

vLLM adapter 收到 `session.created` 后发送 `session.update(model)`，然后发送一次不带 `final` 的 `input_audio_buffer.commit` 启动实时生成。`WriteAudio` 严格校验 sequence/sample 连续性，并发送 base64 PCM16 append；`CloseInput` 只发送一次 `final=true` commit。只有收到 `transcription.done` 才成功完成，连接提前关闭、服务端 error 或 finish timeout 都会分类为可通过 `errors.Is` 判断的 SDK sentinel error。

当前 vLLM Realtime STT 协议固定 16kHz、16-bit、单声道 PCM，不支持 language、language hints、prompt、terms 或 server VAD。delta 是 vLLM 生成器的追加式 token，SDK 累计为 provisional；done 的完整文本覆盖并确认为 stable。API key 可选且只进入 `Authorization: Bearer` header；`ws` 默认仅允许 loopback，远端明文连接必须显式开启 `AllowInsecureWebSocket`。

当前 vLLM 的 `Qwen3ASRRealtimeGeneration` 存在上游已知限制（vllm-project/vllm#35767）：内部音频片段可能重复，且 `language ...<asr_text>` 原始模型协议可能直接进入 delta/done。SDK 不做无边界、无 rollback 元数据的启发式去重，避免误删说话者真实重复的文本。生产实时识别应使用 Voxtral；Qwen3-ASR 应暂用带后处理的 HTTP transcription endpoint，待上游提供规范化 delta 或片段元数据后再在 Provider adapter 边界接入。

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
