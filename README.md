# ASR SDK for Go

`github.com/caitunai/asr` 是一个不依赖 Gin、Viper、输入侧 WebSocket 或具体 VAD 实现的 ASR Go SDK。调用方输入单声道 `float32 PCM`；SDK 负责 PCM16/WAV 编码、供应商调用、相邻双片段窗口、无时间戳 Unicode 对齐、修订事件和尾段确认。

SDK 也提供面向连续 PCM 的 `AudioSession`、`SegmentedSession` 和 `RealtimeSession`。输入可以来自 WebSocket、命令行文件或麦克风；VAD 通过绝对 sample index 的通用 `SpeechBoundary` 接入，SDK 不导入具体 audio/VAD package。供应商实时 WebSocket 使用独立 `StreamingProvider`/`ProviderStream` 接口，不经过 HTTP 窗口调度，也不要求本地 VAD。

需要在同一应用中按输入会话选择多个后端时，可实现 `AudioSessionFactoryCatalog`。`Providers` 只返回 Provider 名称和 `segmented_http`/`realtime_websocket` 模式，不应包含 endpoint 或 credential；`Resolve` 在会话开始时返回固定 factory，同一会话内不应按 Segment 改换供应商。

HTTP 分段会话支持两种策略：默认 `contextual` 会使用短静音 preview、相邻双 Segment window、对齐和 tail fallback 来提高准确率；`single_segment` 只在正式 VAD END 或超长语音安全边界形成 Segment 后提交一个 standalone 识别任务，并直接发布 stable，不产生短静音请求、不等待邻段，也不执行 tail-anchor/fallback。后者适用于按请求计费或并发额度较低的服务。`ClientConfig.RetryCount` 仍独立生效；要求一次逻辑任务严格只产生一次 HTTP 尝试时应设为 `0`。

完整的应用配置、`GenericHTTPConfig`、`MicrosoftHTTPConfig`、`ClientConfig`、`SessionConfig`、`AlignmentConfig` 和 scheduler 语义见 [CONFIGURATION.md](CONFIGURATION.md)。

## 通用 HTTP Provider

通用实现发送 `multipart/form-data`，默认字段为 `file`、`model`、`language`、`prompt`、`hotwords` 和 `language_hints`，并期望响应至少包含：

```json
{"text":"recognized text","language":"en"}
```

可选 `words` 或 `segments` 数组使用 `word`/`text`、`start`、`end`、`confidence` 字段。所有字段名均可通过 `GenericHTTPConfig` 调整，因此可以直接连接常见 OpenAI 风格文件转写接口。非兼容接口（例如具有专用鉴权或响应协议的供应商）只需实现 `Provider`，无需修改会话算法。

SDK 的 `GenericHTTPConfig` 保留字段映射能力，供独立 SDK 使用者适配其他 multipart 协议。本项目的 `services/asr` 已固定成精简的 OpenAI 风格预设：上传 WAV 到 `file`、发送 `response_format=json`、使用 Bearer API key，不发送 model/language/hotwords/language_hints；会话提供非空白 prompt 时才发送去除首尾空白后的 `prompt`。因此应用配置只需要 `baseURL`、`path` 和 `apiKey`。

```toml
[asr.providers.generic]
baseURL="https://asr.example.com"
path="/v1/audio/transcriptions"
apiKey="..."
```

远程服务必须使用 HTTPS，本地 `localhost`、`127.0.0.0/8` 和 `::1` 允许使用 HTTP。对于会在识别文本前返回 `[English]` 或 `[日本語]` 标记的服务，应用预设会移除该标记，避免干扰后续窗口对齐。

```go
provider, err := asr.NewGenericHTTPProvider(asr.GenericHTTPConfig{
    Name:        "generic",
    Model:       "multilingual-asr",
    BaseURL:     "https://asr.example.com",
    Path:        "/v1/audio/transcriptions",
    APIKey:      os.Getenv("ASR_API_KEY"),
    AudioFormat: asr.AudioFormatWAVPCM16,
})
if err != nil {
    return err
}

recognizer, err := asr.NewClient(provider, asr.ClientConfig{
    RequestTimeout: 8 * time.Second,
    RetryCount:     1,
    MaxConcurrency: 16,
    AudioFormat:    asr.AudioFormatWAVPCM16,
})
```

## Microsoft Speech HTTP Provider

`MicrosoftHTTPProvider` 实现 Microsoft Speech REST Conversation API。它属于分段 HTTP provider：调用方仍把连续 PCM 和 VAD boundary 交给 `SegmentedSession`，SDK 对短静音预览、正式 VAD 片段及相邻双窗口编码 WAV 后请求 Microsoft。它不是 Microsoft WebSocket streaming adapter，因此不会绕过本地 VAD 或相邻窗口确认。

```go
provider, err := asr.NewMicrosoftHTTPProvider(asr.MicrosoftHTTPConfig{
    Endpoint:        "https://eastus.stt.speech.microsoft.com",
    APIKey:          os.Getenv("MICROSOFT_SPEECH_API_KEY"),
    DefaultLanguage: "en-US",
})
if err != nil {
    return err
}

recognizer, err := asr.NewClient(provider, asr.ClientConfig{
    RequestTimeout: 20 * time.Second,
    RetryCount:     1,
    MaxConcurrency: 16,
    AudioFormat:    asr.AudioFormatWAVPCM16,
})
```

`Endpoint` 可以是区域根地址、Azure resource 根地址，也可以是完整的 conversation recognition 地址；SDK 会分别补齐 `/speech/...` 或 `/stt/speech/...` 路径。订阅密钥使用 `Ocp-Apim-Subscription-Key`，带 `Bearer ` 前缀或显式 `AuthMode: "bearer"` 的 token 使用 Authorization。每次请求发送 `format=detailed` 和 Microsoft locale；会话语言是 `auto` 时采用 `DefaultLanguage`，默认 `en-US`。返回的 `DisplayText` 优先成为文本，`NBest` 用作回退并提供可选 word times。Microsoft short-audio REST 只接受 16kHz mono PCM WAV、单请求最多 60 秒且只返回 final；应用集成默认把最大 window 设为 55 秒。该 REST 协议没有 prompt、terms、language hints 或自动语言检测能力。

## Qwen Realtime Provider

`QwenRealtimeProvider` 实现千问实时 ASR 的供应商侧 WebSocket 协议。它在 `Start` 内完成鉴权、`session.update` 和 `session.updated` 握手；`RealtimeSession` 把连续 float32 PCM 转换为 16-bit little-endian PCM，默认聚合成 100ms chunk 后按顺序发送 `input_audio_buffer.append`。Provider 的 `text + stash` 映射成不稳定结果，`completed.transcript` 映射成 stable。

```go
provider, err := asr.NewQwenRealtimeProvider(asr.QwenRealtimeConfig{
    Endpoint: "wss://WORKSPACE_ID.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime",
    APIKey:   os.Getenv("DASHSCOPE_API_KEY"),
    Model:    "qwen3-asr-flash-realtime",
})
if err != nil {
    return err
}

session, err := asr.NewRealtimeSession(ctx, provider, asr.RealtimeSessionConfig{
    Request: asr.StreamingRequest{
        Language:   "auto",
        Context:    asr.RecognitionContext{Prompt: "产品发布会", Terms: []string{"星河系统"}},
        SampleRate: 16000,
        Channels:   1,
        Format:     asr.AudioFormatRawPCM16,
    },
})
```

Qwen adapter 默认启用 server VAD，阈值为 `0.0`，静音确认时长为 `400ms`。一般无需配置；需要覆盖时设置 `ServerVADThreshold`、`ServerVADSilenceDuration`，需要关闭时设置 `DisableServerVAD=true`。启用 server VAD 时，`Finish` 发送 `session.finish`；关闭时先发送 `input_audio_buffer.commit` 再结束会话。`Prompt` 和去重后的 `Terms` 以换行拼成 Qwen `corpus.text`。调用方仍需遵守供应商的 10,000-token corpus 上限。

## DashScope Inference Realtime Provider

`DashScopeInferenceRealtimeProvider` 实现百炼 `/api-ws/v1/inference` 的 `run-task` 协议，默认模型为 `qwen-audio-3.0-asr-flash-streaming`。该协议与上面的 OpenAI Realtime 风格 Qwen Provider 不兼容，因此使用独立 adapter：连接后发送 `run-task` 并等待 `task-started`，音频使用 WebSocket Binary Message 直接发送 PCM16，`result-generated` 的 `sentence_end=false/true` 分别映射 provisional/stable，Finish 发送 `finish-task` 并等待 `task-finished`。

```go
provider, err := asr.NewDashScopeInferenceRealtimeProvider(asr.DashScopeInferenceRealtimeConfig{
    Endpoint: "wss://WORKSPACE_ID.cn-beijing.maas.aliyuncs.com/api-ws/v1/inference",
    APIKey:   os.Getenv("DASHSCOPE_API_KEY"),
    Model:    "qwen-audio-3.0-asr-flash-streaming",
})
if err != nil {
    return err
}

session, err := asr.NewRealtimeSession(ctx, provider, asr.RealtimeSessionConfig{
    Request: asr.StreamingRequest{
        LanguageHints: []string{"zh", "en"},
        Context: asr.RecognitionContext{
            Prompt: "产品发布会",
            Terms:  []string{"星河系统", "通义千问"},
        },
        SampleRate: 16000,
        Channels:   1,
        Format:     asr.AudioFormatRawPCM16,
    },
})
```

`Prompt` 作为一条 `user/input_text` 上下文发送，按协议限制为最多 400 个 Unicode 字符；`Terms` 去重后映射成即时 `vocabulary`，权重默认 5，可配置为 1–5 或超级热词权重 50。明确 `Language` 时优先发送它，否则发送最多四个 `LanguageHints`，区域 tag 会转换为主语言代码。心跳 sentence 被忽略；同一 `sentence_id` 的 partial 会修订同一 provisional，空 final 会撤回已有 partial。Adapter 不发送数据合规检测 header，也不执行自动断线重连。

该 Provider 实现可选的 `ProviderContextUpdater`。`RealtimeSession` 默认把最近 4 条 provider-final stable 文本保存在最多 400 个 Unicode 字符的有界历史中；收到 stable 后不会阻塞结果输出，而是在发送下一块音频前先发送 `continue-task`，上下文包含初始 `Prompt` 和最近识别结果。同一连接的写锁保证 context update 先于后续 PCM，协议没有不存在的确认事件等待。可通过 `RealtimeSessionConfig.ContextUpdate.DisableAutomatic=true` 关闭，或使用 `MaxHistoryItems`、`MaxHistoryRunes` 调整 SDK 历史上限；DashScope 最终仍按最多 5 条 input_text、总计 400 字符收口。需要由应用主动替换 Prompt 时，可对 `RealtimeSession`（或 `AudioSessionContextUpdater`）调用 `UpdateContext`；显式更新即使关闭自动历史也仍然可用。

## Qwen Omni Realtime Provider

`QwenOmniRealtimeProvider` 把 Qwen-Omni-Realtime 会话限制为 ASR-only：只申请 `text` modality，持续发送 16kHz 单声道 PCM，消费 `conversation.item.input_audio_transcription.delta/completed`，并把 partial 映射为 provisional、completed 映射为 stable。server VAD 会自动触发 Omni 的对话响应；Provider 默认在 `response.created` 后立即发送 `response.cancel`，避免把回答混入 ASR 输出。输入结束时会补一小段静音推动尾句完成，并用 `FinishTimeout` 兜底。

```go
provider, err := asr.NewQwenOmniRealtimeProvider(asr.QwenOmniRealtimeConfig{
    Endpoint: "wss://WORKSPACE_ID.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime",
    APIKey:   os.Getenv("DASHSCOPE_API_KEY"),
    Model:    "qwen3.5-omni-plus-realtime",
})
if err != nil {
    return err
}

session, err := asr.NewRealtimeSession(ctx, provider, asr.RealtimeSessionConfig{
    Request: asr.StreamingRequest{
        Language:   "auto",
        SampleRate: 16000,
        Channels:   1,
        Format:     asr.AudioFormatRawPCM16,
        ServerVAD:  provider.ServerVADEnabled(),
    },
})
```

SDK 默认模型为 `qwen3.5-omni-plus-realtime`，默认采用 `semantic_vad`、阈值 `0.5`、静音确认 `800ms`；这些参数可覆盖但一般不必写进应用配置。Adapter 发送空的 `input_audio_transcription: {}` 来启用输入转写，不指定其 `model`，由服务端选择默认实现。该通道当前不发送 prompt、terms 或 language hint。如果目标只是低成本、纯语音转文字，优先使用 `QwenRealtimeProvider`；当同一会话后续还要扩展 Omni 音视频理解时再使用本 Provider。

Omni 在已建立会话中返回的单次事件错误不等于 WebSocket 已失效。Adapter 会忽略“回答已经结束、无法 cancel”的预期竞态错误；其他握手后的事件错误会输出非 final `asr.error`，但继续接收音频。只有握手失败、传输断开或输入结束超时才终止整个实时 session。

## OpenAI Realtime Provider

`OpenAIRealtimeProvider` 直接连接 OpenAI GA Realtime transcription WebSocket 服务，也不依赖输入侧 WebSocket。Provider 默认使用 `gpt-4o-mini-transcribe`，连接 `wss://api.openai.com/v1/realtime?intent=transcription`，使用 Bearer API key，并创建 `type=transcription` 的会话。

```go
provider, err := asr.NewOpenAIRealtimeProvider(asr.OpenAIRealtimeConfig{
    APIKey: os.Getenv("OPENAI_API_KEY"),
})
if err != nil {
    return err
}

session, err := asr.NewRealtimeSession(ctx, provider, asr.RealtimeSessionConfig{
    Request: asr.StreamingRequest{
        Language:   "zh-CN",
        SampleRate: 16000,
        Channels:   1,
        Format:     asr.AudioFormatRawPCM16,
    },
})
```

OpenAI 要求 24kHz mono PCM16；Adapter 接受应用常见的 8kHz、16kHz 或 24kHz PCM16，并使用跨 chunk 连续的线性重采样统一转换成 24kHz。SDK 默认启用 `semantic_vad`，`eagerness=auto`，由服务端根据语义完整性自动形成转写 item，避免固定周期在句中断开。也可切换为 `server_vad`，或显式关闭 turn detection；关闭后才按 `CommitInterval`（默认 `3s`）手动 commit。

Finish 会提交剩余 PCM、补充一小段尾部静音，并等待已经自动或手动 commit 的所有 item 返回 completed/failed。自动 VAD 与尾部手动 commit 竞态产生的“空 buffer”错误会被作为已完成收尾处理，不向上报成会话失败。`gpt-realtime-whisper` 不支持转写会话 VAD，因此 SDK 自动使用周期 commit；此模型可设置 `Delay`（`minimal/low/medium/high/xhigh`，默认 `medium`），默认的 `gpt-4o-mini-transcribe` 不发送该专属字段。

当前 Provider 不发送 `RecognitionContext.Prompt` 或 terms；明确语言会转换成 ISO-639-1 主语言代码，`auto` 不发送 language。服务端 delta 累积为 provisional，completed 直接成为 stable。

## Gemini Realtime Provider

`GeminiRealtimeProvider` 直接实现 Gemini Live API 的 raw WebSocket 协议，使用 `inputAudioTranscription` 获得用户输入语音文本。它不依赖浏览器 WebSocket，可由服务端、命令行音频读取程序或其他 Go package 直接创建。

```go
provider, err := asr.NewGeminiRealtimeProvider(asr.GeminiRealtimeConfig{
    APIKey: os.Getenv("GEMINI_API_KEY"),
})
if err != nil {
    return err
}

session, err := asr.NewRealtimeSession(ctx, provider, asr.RealtimeSessionConfig{
    Request: asr.StreamingRequest{
        Language:   "auto",
        SampleRate: 16000,
        Channels:   1,
        Format:     asr.AudioFormatRawPCM16,
    },
    ChunkDuration: 40 * time.Millisecond,
})
```

SDK 默认连接 Gemini v1beta `BidiGenerateContent` endpoint，使用 `gemini-3.1-flash-live-preview`。API key 按官方 raw WebSocket 协议放入认证 query，SDK 不会把完整 URL 或 key 写入事件和日志。首包发送 setup 并等待 `setupComplete`；16kHz mono PCM16 直接发送，其他受支持采样率才连续重采样为 16kHz，默认使用 40ms chunk。自动 VAD 默认使用 300ms prefix padding、300ms silence duration、高起音灵敏度和高结束灵敏度，使长音频中的短停顿能及时形成输入 turn；这是偏低延迟的 ASR 策略，若慢速口述被切得过碎，可增加静音时长或降低结束灵敏度。

新闻播报、配乐节目等音频可能长期保持 activity，单靠 Server VAD 无法给出确定的出字上限。SDK 默认统计发送到 Gemini 的 16kHz sample clock：若连续 15 秒仍未收到自然 turn boundary，会在下一块音频前发送一次 `audioStreamEnd`，随后在同一 WebSocket 和上下文中继续发送。这个刷新不会结束 ASR session；最终 Finish 仍只负责剩余尾段。可通过 `MaxContinuousTurn` 调整上限，或设置 `DisableContinuousTurnFlush=true` 关闭。

`inputTranscription` 先产生 provisional；收到 generation/turn complete 后，SDK 继续等待短暂 drain 再产生 stable，以容纳 Gemini 明确不保证顺序的转写事件。`interrupted=true` 只表示旧的模型输出被新语音打断，不会确认当前输入文本。Finish 发送 `audioStreamEnd=true`；若供应商没有返回明确边界，则使用有界 idle 回退确认尾段，最终仍由硬超时终止。默认开启 sliding-window context compression，避免长会话音频 token 持续累积。

## ElevenLabs Realtime Provider

`ElevenLabsRealtimeProvider` 实现 Scribe v2 Realtime WebSocket 协议。最小配置只需要 API key：

```go
provider, err := asr.NewElevenLabsRealtimeProvider(asr.ElevenLabsRealtimeConfig{
    APIKey: os.Getenv("ELEVENLABS_API_KEY"),
})
if err != nil {
    return err
}

session, err := asr.NewRealtimeSession(ctx, provider, asr.RealtimeSessionConfig{
    Request: asr.StreamingRequest{
        Language:   "auto",
        SampleRate: 16000,
        Channels:   1,
        Format:     asr.AudioFormatRawPCM16,
        Context: asr.RecognitionContext{
            Terms: []string{"ElevenLabs", "Scribe"},
        },
    },
    ChunkDuration: 100 * time.Millisecond,
})
```

SDK 默认连接 `wss://api.elevenlabs.io/v1/speech-to-text/realtime`，模型为 `scribe_v2_realtime`，使用 VAD commit、300ms 静音确认、word timestamps 和语言检测。API key 只放入 `xi-api-key` header；受限 key 必须在 ElevenLabs 控制台启用 Speech to Text（`speech_to_text`）权限。Provider 支持官方 PCM16 采样率并直接声明相应 `pcm_*` 格式；应用默认的 16kHz 不经过重采样。

committed transcript 映射为 stable。ElevenLabs partial 不带 words 或置信度，无法可靠区分临时猜测与有效文本，所以 adapter 默认不发布 partial；需要最低延迟时可显式设置 `EmitPartials=true` 映射为 preview。启用 timestamps 时，普通 committed 事件会被忽略，紧随其后的 timestamp committed 才产生一次 stable，避免同一句重复；空 final 或纯标点 final 表示供应商撤回当前 partial，绝不会回退到旧 partial，默认也会拒绝 lexical word 平均 logprob 低于 -5.0 的低置信度尾部幻觉。启用 partial 后，被拒绝 turn 已经产生的 preview 会收到 `discarded` 修订，消费者应删除对应 segment；真实的高置信度短词仍可正常 stable。当前 adapter 暂不发送 `RecognitionContext.Prompt` 或 `previous_text`，capability 明确为 false；符合限制的 terms 仍转成最多 50 个 keyterms。manual commit 模式默认每 20 秒提交一次，避免长音频积压；Finish 追加 100ms 静音并强制提交尾段。完整参数和限制见 [CONFIGURATION.md](CONFIGURATION.md)。

## Inworld Realtime Provider

`InworldRealtimeProvider` 实现 Inworld bidirectional streaming STT WebSocket 协议，默认使用 `inworld/inworld-stt-1`：

```go
provider, err := asr.NewInworldRealtimeProvider(asr.InworldRealtimeConfig{
    APIKey: os.Getenv("INWORLD_API_KEY"),
})
if err != nil {
    return err
}

session, err := asr.NewRealtimeSession(ctx, provider, asr.RealtimeSessionConfig{
    Request: asr.StreamingRequest{
        Language:   "zh-CN",
        SampleRate: 16000,
        Channels:   1,
        Format:     asr.AudioFormatRawPCM16,
        Context: asr.RecognitionContext{
            Terms:  []string{"中央气象台", "Inworld"},
        },
    },
    ChunkDuration: 100 * time.Millisecond,
})
```

SDK 连接 `wss://api.inworld.ai/stt/v1/transcribe:streamBidirectional`，使用 `Authorization: Basic <API key>` 握手；API key 不写入 URL、事件或日志。首包是 `transcribeConfig`，音频以 base64 `audioChunk` 持续发送。SDK 解析网关返回的 `result.transcription`：interim 映射 preview，final 映射 stable，同一 turn 复用 result ID；空 final 会撤回该 turn 已发布的 preview。Finish 先发送 `endTurn`，收到尾部 final 后再发送 `closeStream` 并由客户端关闭 WebSocket；`FinishTimeout` 只用于尾部 final 确实没有到达的情况。

默认启用 Inworld server VAD，阈值为官方默认 `0.5`；可设置 `DisableServerVAD=true` 以便仅由 `endTurn` 确认轮次。只有明确设置的 language 会转成 ISO 主语言代码；`auto` 始终省略 language，不会把 language hints 升级为输出语言约束。Inworld `prompts` 是专有名词/关键词偏置，因此 SDK 只发送去重后的 `RecognitionContext.Terms`，不发送通用 `Prompt`，避免中文上下文把英文语音偏置为中文输出。Terms 包含协议明确不接受的 `#`、`/`、`@`、`|` 或控制字符时，SDK 会在连接前返回 `ErrInvalidRequest`。

## vLLM Realtime Provider

`VLLMRealtimeProvider` 实现 vLLM Realtime STT `/v1/realtime` 协议，默认连接本机 vLLM 并使用 Voxtral Mini Realtime：

```go
provider, err := asr.NewVLLMRealtimeProvider(asr.VLLMRealtimeConfig{
    Endpoint: "ws://127.0.0.1:8000/v1/realtime",
    Model:    "mistralai/Voxtral-Mini-4B-Realtime-2602",
    // APIKey: os.Getenv("VLLM_API_KEY"), // vLLM 启用 --api-key 时设置
})
```

Provider 等待 `session.created` 后发送 `session.update`，再以非 final commit 启动实时生成。输入必须是 16kHz mono PCM16；音频以 base64 append 连续发送。vLLM 的 delta 是追加式 token，SDK 将累计文本映射为 provisional；`transcription.done` 的完整文本映射 stable。Finish 发送一次 `commit(final=true)` 并等待 done，不会提前关闭连接或重复 final commit。该协议当前没有 server VAD、language、prompt 或 terms 能力，API key 可选。

已知限制：vLLM 的 Qwen3-ASR realtime 实现目前可能重复内部音频片段并泄漏 `language ...<asr_text>` 原始标记（vllm-project/vllm#35767）。由于协议没有片段 ID 或 rollback 元数据，SDK 不执行可能误删真实重复语句的猜测式去重；实时模式应使用 Voxtral，Qwen3-ASR 暂用 HTTP transcription endpoint。

## 单会话请求调度

需要同时提交高频中间结果和不可丢失正式结果时，用 `ScheduledRecognizer` 包装供应商客户端，再把包装后的 `Recognizer` 交给 `Session` 和中间识别调用方：

```go
scheduled, err := asr.NewScheduledRecognizer(ctx, recognizer)
if err != nil {
    return err
}
defer scheduled.Close()

result, err := scheduled.Transcribe(ctx, asr.TranscriptionRequest{
    RequestID:     requestID,
    Samples:       samples,
    SampleRate:    16000,
    Channels:      1,
    AudioEndAt:    endAt,
    Authoritative: false, // 中间预览；正式 VAD/尾段请求设为 true
})
```

调度策略固定为 `1 active + authoritative FIFO + 1 latest preview`：

- `Authoritative=true` 的请求按 `AudioEndAt` 排序并无损执行，永远不会被后续请求覆盖。
- `Authoritative=false` 最多保留一个等待中的最新请求；旧请求返回 `ErrRequestSuperseded`。
- authoritative 到达时会清除等待中的 preview，并取消正在执行的 preview，使正式证据尽快执行；正在执行的 authoritative 不会被取消。
- `Stats()` 可读取 authoritative 的等待/未完成数量、preview pending 状态和被覆盖计数，供调用方计算停止阶段的 drain timeout 和监控积压。

调度器只依赖通用 `Recognizer` 和 `TranscriptionRequest.Authoritative`，不导入任何 VAD/audio 包。直接使用低层 `Session` 时，调用方自行创建请求；使用高层 `SegmentedSession` 时，SDK 会把通用 speech boundary 映射为 preview、完整 Segment、相邻窗口和 tail anchor。

## 连续 PCM 与通用边界

`SegmentedSession` 适合 WebSocket 实时输入、麦克风或分块读取的音频文件：

```go
session, err := asr.NewSegmentedSession(ctx, recognizer, asr.SegmentedSessionConfig{
    Session: asr.SessionConfig{
        Language:   "auto",
        SampleRate: 16000,
        Channels:   1,
        Context: asr.RecognitionContext{
            Prompt: "产品发布会",
            Terms:  []string{"Qwen ASR"},
        },
    },
    // 连续语音达到 15s 后，把当前起点后前 5s 内最后一个短静音升级为正式段。
    LongSpeechCommitAfter: 15 * time.Second,
    LongSpeechCommitPrefix: 5 * time.Second,
})
if err != nil {
    return err
}
defer session.Close()

go func() {
    for event := range session.Events() {
        handle(event)
    }
}()

err = session.Push(ctx, asr.AudioChunk{
    Samples: pcm,
    Boundaries: []asr.SpeechBoundary{{
        Type:               asr.SpeechBoundaryStart,
        SourceSegmentIndex: 0,
        StartSample:        0,
    }},
})
```

边界使用输入流绝对 sample index。`Finish` 只接收结束阶段首次产生的 PCM/边界增量；不要传入调用方保存的完整 VAD 历史。`Done`/`Wait` 由 SDK 内部完成状态驱动，与事件最终写入浏览器、终端或文件是否成功无关。

如果命令行程序希望把整个音频文件当成单个请求，不需要构造 VAD 边界，可以直接调用 `recognizer.Transcribe`。

## 滚动会话

```go
session, err := asr.NewSession(scheduled, nil, asr.SessionConfig{
    SessionID:  sessionID,
    Language:   "auto",
    Context: asr.RecognitionContext{
        Prompt: "产品发布会",
        Terms:  []string{"星河系统", "Qwen ASR"},
    },
    SampleRate:        16000,
    Channels:          1,
    TailAnchorEnabled: true,
    // 短 VAD 等待下一次 SpeechStarted，以获得相邻窗口上下文；长 VAD 快速独立封口。
    ShortSegmentMaxDuration:  6 * time.Second,
    ShortSegmentNeighborWait: 3 * time.Second,
})
if err != nil {
    return err
}
defer session.Close()

go func() {
    for event := range session.Events() {
        handle(event)
    }
}()

if err := session.AddSegment(ctx, segment); err != nil {
    return err
}
```

一个 `Session` 在创建时绑定一个 `Recognizer`，整个证据链不会切换供应商或模型。`prompt`、专有名词和语言提示会复制到该会话的每个 standalone、pair 和 tail-anchor 请求。`SpeechStarted` 会根据上一段长度决定是继续证据链还是立即封口；音频结束时调用 `Stop`，并继续消费事件直到收到 `asr.session_completed`。

启用 `ShortSegmentMaxDuration` 和 `ShortSegmentNeighborWait` 后，短于门槛的 VAD 会立即提交 ASR 并输出不稳定结果，但在 neighbor wait 到期前不会仅凭 standalone 结果定稿。如果下一次 `SpeechStarted` 在期限内到达，会保留同一 evidence chain，下一次 `AddSegment` 自动提交相邻双片段窗口；没有后续语音时仍会在期限后封口。达到门槛的长 VAD 沿用较短的 `TailFinalizeSilence`，不必等待下一个 VAD；若较短尾段计时尚未到期就出现下一次 `SpeechStarted`，SDK 会立即以 `long_segment_boundary` 请求上一链独立封口，并让新 Segment 进入新链。旧链的 HTTP 请求即使仍在执行也不会被取消或丢弃，两条链可同时等待结果。两个参数必须同时为零（关闭此策略）或同时大于零，且 neighbor wait 不应短于 tail silence。

对于上游 VAD 长时间不产生 `speech_end` 的连续语音，建议由上游在较短静音处产生不结束物理 VAD 的 soft-boundary，用于提前请求中间识别。正式 `speech_end` 到达后仍将完整 VAD segment 交给 SDK 的 rolling session；超过上游最长语音时，应将最近 soft-boundary 升级为真正分段。相邻 segment 时间连续时，SDK 不会在拼接窗口中额外插入静音。

`SegmentedSession` 还会保留已经产生 Preview 的短静音候选。未正式提交的语音达到 `LongSpeechCommitAfter` 后，SDK 在 `[currentStart, currentStart+LongSpeechCommitPrefix]` 内选择最后一个候选，将其作为正式 `speech_end`，并从同一 sample 位置开启新的内部 `speech_start`。该切分不受短片段 neighbor wait 影响，会以 `long_speech_commit` 立即请求独立封口。这允许 15 秒连续语音先确认大约前 5 秒，同时继续缓存后 10 秒。Preview 只提供低延迟显示，升级时仍重新执行正式识别，成功后才发布 stable/degraded。

`preview` 和 `provisional` 是不稳定结果；`stable` 是由相邻窗口交叉证据或明确尾段策略确认的结果；`degraded` 表示超时后用现有最佳证据封口。调用方按 `segment_index` 排序，并只用不小于当前 `revision` 的更新覆盖旧结果。
