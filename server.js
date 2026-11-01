const DEFAULT_GEMINI_WS_URL =
  "wss://generativelanguage.googleapis.com/ws/" +
  "google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";

const DEFAULT_MODEL = "gemini-3.8-live";
const SETUP_TIMEOUT_MS = 15_000;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // 密码访问验证：配置了 ACCESS_PASSWORD 时生效
    let authedToken = "";
    if (env.ACCESS_PASSWORD) {
      const expectedPassword = String(env.ACCESS_PASSWORD).trim();
      const cookieAuth = request.headers.get("Cookie")?.match(/(?:^|;\s*)auth=([^;]+)/)?.[1];
      const queryPwd = url.searchParams.get("pwd") || url.searchParams.get("auth");
      const authHeader = request.headers.get("Authorization");

      let isAuthed = false;
      if (queryPwd && queryPwd === expectedPassword) {
        isAuthed = true;
      } else if (cookieAuth && decodeURIComponent(cookieAuth) === expectedPassword) {
        isAuthed = true;
      } else if (authHeader && authHeader.startsWith("Basic ")) {
        try {
          const decoded = atob(authHeader.slice(6));
          const inputPwd = decoded.includes(":") ? decoded.split(":")[1] : decoded;
          if (inputPwd === expectedPassword || decoded === expectedPassword) {
            isAuthed = true;
          }
        } catch {}
      }

      if (!isAuthed) {
        return new Response("Unauthorized", {
          status: 401,
          headers: {
            "WWW-Authenticate": 'Basic realm="Gemini Live Voice"',
            "content-type": "text/plain; charset=UTF-8",
          },
        });
      }
      authedToken = expectedPassword;
    }

    if (url.pathname === "/ws") {
      return handleBrowserWebSocket(request, env, ctx);
    }

    if (url.pathname !== "/" || request.method !== "GET") {
      return new Response("Not Found", { status: 404 });
    }

    const headers = {
      "content-type": "text/html; charset=UTF-8",
      "cache-control": "no-store",
      "x-content-type-options": "nosniff",
      "referrer-policy": "no-referrer",
      "permissions-policy": "microphone=(self)",
      "content-security-policy": [
        "default-src 'self'",
        "style-src 'unsafe-inline'",
        "script-src 'unsafe-inline'",
        "connect-src 'self' wss:",
        "media-src 'self' blob:",
        "img-src 'self' data:",
        "base-uri 'none'",
        "form-action 'self'",
        "frame-ancestors 'none'",
      ].join("; "),
    };

    if (authedToken) {
      headers["set-cookie"] = `auth=${encodeURIComponent(authedToken)}; Path=/; Max-Age=2592000; Secure; SameSite=Lax`;
    }

    const renderedHtml = HTML.replace(
      "__AUTH_TOKEN_PLACEHOLDER__",
      JSON.stringify(authedToken)
    );

    return new Response(renderedHtml, { headers });
  },
};

function handleBrowserWebSocket(request, env, ctx) {
  const upgrade = request.headers.get("Upgrade");

  if (!upgrade || upgrade.toLowerCase() !== "websocket") {
    return new Response("Expected Upgrade: websocket", { status: 426 });
  }

  /*
   * WebSocket 并不受普通 CORS 保护。
   * 默认只允许当前 Worker 页面所在 Origin 发起连接。
   *
   * 如使用自定义域名，可显式设置：
   * ALLOWED_ORIGIN=https://voice.example.com
   */
  const requestOrigin = request.headers.get("Origin");
  const allowedOrigin = String(
    env.ALLOWED_ORIGIN || new URL(request.url).origin,
  ).replace(/\/$/, "");

  if (!requestOrigin || requestOrigin !== allowedOrigin) {
    return new Response("Forbidden WebSocket origin", { status: 403 });
  }

  const pair = new WebSocketPair();
  const [client, browser] = Object.values(pair);

  /*
   * allowHalfOpen 对代理场景更合适：
   * 浏览器或 Gemini 任一端关闭时，由我们协调另一端。
   */
  browser.accept({ allowHalfOpen: true });

  const bridge = createGeminiBridge(browser, env);
  bridge.install();

  /*
   * 不阻塞浏览器 WebSocket 升级；
   * 上游握手和 Gemini setup 在后台进行。
   */
  ctx.waitUntil(bridge.connect());

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

function createGeminiBridge(browser, env) {
  let upstream = null;
  let phase = "connecting"; // connecting | setting-up | ready | closed
  let closed = false;
  let setupTimer = null;

  function sendToBrowser(payload) {
    if (!closed && browser.readyState === WebSocket.OPEN) {
      browser.send(JSON.stringify(payload));
    }
  }

  function clearSetupTimer() {
    if (setupTimer) {
      clearTimeout(setupTimer);
      setupTimer = null;
    }
  }

  function closeSocket(socket, code = 1000, reason = "") {
    if (!socket) return;

    try {
      if (
        socket.readyState === WebSocket.CONNECTING ||
        socket.readyState === WebSocket.OPEN ||
        socket.readyState === WebSocket.CLOSING
      ) {
        socket.close(code, reason.slice(0, 120));
      }
    } catch {
      // 连接关闭阶段忽略异常。
    }
  }

  function closeEverything(code = 1000, reason = "closed") {
    if (closed) return;

    closed = true;
    phase = "closed";
    clearSetupTimer();

    closeSocket(upstream, code, reason);
    closeSocket(browser, code, reason);
  }

  function fail(message) {
    if (closed) return;

    console.error("[Gemini Live Worker]", message);

    sendToBrowser({
      type: "error",
      message,
    });

    /*
     * 先 send 错误帧，再 close。
     * 浏览器可以显示出错信息，随后允许用户重新拨号。
     */
    closeEverything(1011, "Gemini Live connection failed");
  }

  async function connect() {
    let config;

    try {
      config = getGeminiConfig(env);
    } catch (error) {
      fail(errorMessage(error));
      return;
    }

    sendToBrowser({
      type: "status",
      state: "connecting",
      message: "正在连接 Gemini…",
    });

    let response;

    try {
      /*
       * Cloudflare Worker 的出站 WebSocket：
       * 用 https + Upgrade: websocket 发起，再从 response.webSocket 获取 socket。
       */
      response = await fetch(config.fetchUrl, {
        headers: {
          Upgrade: "websocket",
        },
      });
    } catch (error) {
      fail(
        "无法建立到 Gemini 的 WebSocket 连接：" +
          errorMessage(error),
      );
      return;
    }

    if (!response.webSocket) {
      const detail = await readHandshakeError(response);

      fail(
        "Gemini WebSocket 握手失败（HTTP " +
          response.status +
          "）" +
          (detail ? "：" + detail : ""),
      );
      return;
    }

    if (closed) {
      try {
        response.webSocket.close(1000, "browser already closed");
      } catch {}
      return;
    }

    upstream = response.webSocket;

    /*
     * 新兼容日期下，二进制 WS 消息默认可能是 Blob。
     * 明确设为 ArrayBuffer，避免旧代码出现 "[object Blob]"。
     */
    upstream.binaryType = "arraybuffer";

    upstream.addEventListener("message", (event) => {
      void handleGeminiMessage(event);
    });

    upstream.addEventListener("error", () => {
      if (!closed) {
        fail(
          "Gemini 上游 WebSocket 出错。请检查 API Key、模型权限、Endpoint 和网络。",
        );
      }
    });

    upstream.addEventListener("close", (event) => {
      if (closed) return;

      const reason = event.reason
        ? "Gemini 已关闭连接：" + event.reason
        : "Gemini 已关闭连接（代码 " + event.code + "）";

      sendToBrowser({
        type: "closed",
        code: event.code,
        message: reason,
      });

      closeEverything(
        event.code === 1000 ? 1000 : 1011,
        "Gemini upstream closed",
      );
    });

    upstream.accept({ allowHalfOpen: true });

    phase = "setting-up";

    sendToBrowser({
      type: "status",
      state: "setting-up",
      message: "Gemini 已连接，正在初始化会话…",
    });

    try {
      upstream.send(JSON.stringify(createSetupMessage(config)));
    } catch (error) {
      fail("无法发送 Gemini setup 消息：" + errorMessage(error));
      return;
    }

    setupTimer = setTimeout(() => {
      if (phase === "setting-up" && !closed) {
        fail(
          "等待 Gemini setupComplete 超时（15 秒）。请检查 GEMINI_API_KEY、GEMINI_WS_URL、模型权限或 Worker 日志。",
        );
      }
    }, SETUP_TIMEOUT_MS);
  }

  function install() {
    browser.addEventListener("message", (event) => {
      void handleBrowserMessage(event);
    });

    browser.addEventListener("error", () => {
      closeEverything(1011, "browser websocket error");
    });

    browser.addEventListener("close", () => {
      closeEverything(1000, "browser closed");
    });
  }

  async function handleBrowserMessage(event) {
    if (closed) return;

    let message;

    try {
      message = JSON.parse(await wsDataToText(event.data));
    } catch {
      sendToBrowser({
        type: "error",
        message: "浏览器发送了无效的 JSON 消息。",
      });
      return;
    }

    if (phase !== "ready" || !upstream) {
      sendToBrowser({
        type: "error",
        message: "Gemini 尚未完成初始化，请稍候。",
      });
      return;
    }

    if (upstream.readyState !== WebSocket.OPEN) {
      sendToBrowser({
        type: "error",
        message: "Gemini 连接当前不可用。",
      });
      return;
    }

    if (message.type === "audio") {
      const audioData = typeof message.data === "string" ? message.data : "";

      /*
       * 浏览器每次发的是几十毫秒 PCM，正常远小于这个限制。
       * 防止异常客户端向 Worker / 上游推送超大消息。
       */
      if (
        !audioData ||
        audioData.length > 128 * 1024 ||
        !/^[A-Za-z0-9+/=]+$/.test(audioData)
      ) {
        sendToBrowser({
          type: "error",
          message: "麦克风音频数据格式不正确。",
        });
        return;
      }

      upstream.send(
        JSON.stringify({
          realtimeInput: {
            audio: {
              data: audioData,
              mimeType: "audio/pcm;rate=16000",
            },
          },
        }),
      );

      return;
    }

    if (message.type === "audioEnd") {
      upstream.send(
        JSON.stringify({
          realtimeInput: {
            audioStreamEnd: true,
          },
        }),
      );

      return;
    }

    sendToBrowser({
      type: "error",
      message: "未知浏览器消息类型。",
    });
  }

  async function handleGeminiMessage(event) {
    if (closed) return;

    let raw;

    try {
      raw = await wsDataToText(event.data);
    } catch (error) {
      fail("无法读取 Gemini 上游消息：" + errorMessage(error));
      return;
    }

    let packet;

    try {
      packet = JSON.parse(raw);
    } catch {
      fail("Gemini 返回了无法解析的非 JSON 消息。");
      return;
    }

    /*
     * setupComplete 是真正可开始传输音频的信号。
     */
    if (
      packet &&
      Object.prototype.hasOwnProperty.call(packet, "setupComplete")
    ) {
      clearSetupTimer();
      phase = "ready";

      sendToBrowser({
        type: "ready",
        model: getSafeModelName(env),
      });
    }

    /*
     * 让页面直接显示 Gemini 返回的顶层错误，
     * 不再像旧版那样静默卡在“初始化中”。
     */
    if (packet && packet.error) {
      const message =
        packet.error.message ||
        packet.error.status ||
        JSON.stringify(packet.error).slice(0, 500);

      fail("Gemini 返回错误：" + message);
      return;
    }

    sendToBrowser({
      type: "gemini",
      data: raw,
    });
  }

  return {
    install,
    connect,
  };
}

function getGeminiConfig(env) {
  const apiKey = requiredEnv(env, "GEMINI_API_KEY");
  const systemPrompt = requiredEnv(env, "GEMINI_SYSTEM_PROMPT");

  const rawEndpoint = String(
    env.GEMINI_WS_URL || DEFAULT_GEMINI_WS_URL,
  ).trim();

  const endpoint = new URL(rawEndpoint);

  /*
   * 环境变量允许填官方文档中的 wss:// 地址；
   * Worker 内部 fetch 升级 WebSocket 时改为 https://。
   */
  if (endpoint.protocol === "wss:") {
    endpoint.protocol = "https:";
  }

  if (endpoint.protocol !== "https:") {
    throw new Error(
      "GEMINI_WS_URL 必须是 https:// 或 wss:// 的 WebSocket Endpoint。",
    );
  }

  endpoint.searchParams.set("key", apiKey);

  const model = getSafeModelName(env);

  const voice = String(env.GEMINI_VOICE || "Kore").trim();

  if (voice && !/^[A-Za-z][A-Za-z0-9_-]*$/.test(voice)) {
    throw new Error("GEMINI_VOICE 格式不正确。");
  }

  return {
    fetchUrl: endpoint.toString(),
    model,
    voice,
    systemPrompt,
  };
}

function getSafeModelName(env) {
  const model = String(env.GEMINI_MODEL || DEFAULT_MODEL)
    .trim()
    .replace(/^models\//, "");

  if (!/^[A-Za-z0-9._-]+$/.test(model)) {
    throw new Error("GEMINI_MODEL 格式不正确。");
  }

  return model;
}

function createSetupMessage(config) {
  const generationConfig = {
    responseModalities: ["AUDIO"],
  };

  if (config.voice) {
    generationConfig.speechConfig = {
      voiceConfig: {
        prebuiltVoiceConfig: {
          voiceName: config.voice,
        },
      },
    };
  }

  return {
    setup: {
      model: "models/" + config.model,

      /*
       * 注意：responseModalities 与 speechConfig 属于 generationConfig。
       */
      generationConfig,

      systemInstruction: {
        parts: [
          {
            text: config.systemPrompt,
          },
        ],
      },

      /*
       * 电话界面中的实时字幕：
       * - inputAudioTranscription：用户说话字幕
       * - outputAudioTranscription：Gemini 回答字幕
       */
      inputAudioTranscription: {},
      outputAudioTranscription: {},
    },
  };
}

function requiredEnv(env, name) {
  const value = typeof env[name] === "string" ? env[name].trim() : "";

  if (!value) {
    throw new Error("缺少 Cloudflare 环境变量：" + name);
  }

  return value;
}

async function readHandshakeError(response) {
  try {
    const text = (await response.text()).replace(/\s+/g, " ").trim();

    if (!text) return "";

    try {
      const json = JSON.parse(text);

      if (json && json.error && json.error.message) {
        return String(json.error.message).slice(0, 500);
      }
    } catch {
      // 非 JSON 时回退为普通文本。
    }

    return text.slice(0, 500);
  } catch {
    return "";
  }
}

async function wsDataToText(data) {
  if (typeof data === "string") return data;

  if (data instanceof ArrayBuffer) {
    return new TextDecoder().decode(data);
  }

  if (ArrayBuffer.isView(data)) {
    return new TextDecoder().decode(
      data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength),
    );
  }

  if (typeof Blob !== "undefined" && data instanceof Blob) {
    return await data.text();
  }

  return String(data);
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

const HTML = String.raw`<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="theme-color" content="#090d18">
  <title>Gemini Live Call</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #090d18;
      --panel: rgba(22, 30, 51, .8);
      --panel-border: rgba(177, 199, 255, .13);
      --text: #f6f8ff;
      --muted: #aab4cf;
      --green: #48d597;
      --blue: #8da8ff;
      --danger: #ff637b;
      --energy: 0;
    }

    * {
      box-sizing: border-box;
      -webkit-tap-highlight-color: transparent;
    }

    body {
      min-height: 100svh;
      margin: 0;
      overflow: hidden;
      color: var(--text);
      background:
        radial-gradient(circle at 50% -10%, #253975 0%, transparent 42%),
        radial-gradient(circle at 0% 100%, #133d47 0%, transparent 40%),
        var(--bg);
      font-family:
        Inter, ui-sans-serif, system-ui, -apple-system,
        BlinkMacSystemFont, "Segoe UI", "PingFang SC",
        "Microsoft YaHei", sans-serif;
    }

    .shell {
      width: min(100%, 520px);
      min-height: 100svh;
      margin: 0 auto;
      padding: max(18px, env(safe-area-inset-top))
               20px
               max(24px, env(safe-area-inset-bottom));
      display: flex;
      flex-direction: column;
    }

    .topbar {
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 45px;
      text-align: center;
    }

    .brand {
      display: grid;
      gap: 2px;
    }

    .brand strong {
      letter-spacing: .02em;
      font-size: 15px;
    }

    .brand span {
      color: var(--muted);
      font-size: 11px;
    }

    .stage {
      flex: 0 0 auto;
      padding-top: clamp(30px, 8vh, 78px);
      display: grid;
      justify-items: center;
      text-align: center;
    }

    .orb-wrap {
      position: relative;
      width: 174px;
      height: 174px;
      display: grid;
      place-items: center;
    }

    .ring,
    .ring::before,
    .ring::after {
      position: absolute;
      inset: 0;
      border: 1px solid rgba(143, 172, 255, .28);
      border-radius: 50%;
      content: "";
    }

    .ring::before {
      inset: -14px;
      opacity: .55;
    }

    .ring::after {
      inset: -31px;
      opacity: .18;
    }

    .orb {
      width: 142px;
      height: 142px;
      border-radius: 50%;
      display: grid;
      place-items: center;
      color: white;
      font-size: 53px;
      font-weight: 750;
      background:
        radial-gradient(circle at 31% 25%, #dce6ff 0%, #9cb5ff 15%, transparent 16%),
        radial-gradient(circle at 38% 28%, #7793ff 0%, #425fc8 43%, #263d93 100%);
      box-shadow:
        0 0 calc(22px + var(--energy) * 58px) rgba(106, 143, 255, .56),
        0 18px 45px rgba(0, 0, 0, .34);
      transform: scale(calc(1 + var(--energy) * .11));
      transition: transform .1s linear, box-shadow .12s linear;
      z-index: 1;
    }

    body[data-phase="dialing"] .orb {
      animation: breathe 1.15s ease-in-out infinite alternate;
    }

    body[data-phase="connected"] .ring {
      animation: ripple 1.8s ease-out infinite;
    }

    body[data-speaking="true"] .orb {
      background:
        radial-gradient(circle at 31% 25%, #e2fff1 0%, #8cf0be 15%, transparent 16%),
        radial-gradient(circle at 38% 28%, #6be4a2 0%, #258b66 43%, #164b42 100%);
    }

    @keyframes breathe {
      from { transform: scale(.96); opacity: .75; }
      to { transform: scale(1.06); opacity: 1; }
    }

    @keyframes ripple {
      0% { transform: scale(.86); opacity: .75; }
      100% { transform: scale(1.27); opacity: 0; }
    }

    .status {
      margin-top: 42px;
      min-height: 46px;
    }

    .status-main {
      font-size: 21px;
      font-weight: 720;
      letter-spacing: .01em;
    }

    .status-sub {
      margin-top: 7px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.4;
    }

    .timer {
      margin-top: 10px;
      min-height: 19px;
      color: #d7e0ff;
      font-variant-numeric: tabular-nums;
      font-size: 14px;
      letter-spacing: .08em;
    }

    .captions {
      flex: 1 1 auto;
      min-height: 170px;
      max-height: 34svh;
      margin-top: 35px;
      padding: 15px;
      overflow-y: auto;
      border: 1px solid var(--panel-border);
      border-radius: 20px;
      background: linear-gradient(145deg, rgba(20, 28, 49, .77), rgba(11, 16, 30, .5));
      scrollbar-width: thin;
    }

    .caption-empty {
      min-height: 120px;
      display: grid;
      place-items: center;
      color: #8490b2;
      text-align: center;
      font-size: 13px;
      line-height: 1.65;
    }

    .line {
      margin: 0 0 13px;
      line-height: 1.55;
      font-size: 14px;
      overflow-wrap: anywhere;
    }

    .line:last-child {
      margin-bottom: 0;
    }

    .line .who {
      display: block;
      margin-bottom: 2px;
      color: #97a8db;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: .06em;
      text-transform: uppercase;
    }

    .line.me .who {
      color: #7ee7b6;
    }

    .line.note {
      color: #8e9abb;
      font-size: 12px;
    }

    .line.live .text::after {
      content: " ···";
      color: #9eb1ee;
      animation: dots 1s infinite step-end;
    }

    @keyframes dots {
      0%, 100% { opacity: .25; }
      50% { opacity: 1; }
    }

    .controls {
      flex: 0 0 auto;
      margin-top: 23px;
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 14px;
      align-items: center;
    }

    button {
      border: 0;
      color: white;
      font: inherit;
      cursor: pointer;
    }

    button:disabled {
      cursor: not-allowed;
      opacity: .45;
    }

    .call-btn {
      grid-column: 1 / -1;
      min-height: 68px;
      border-radius: 22px;
      background: linear-gradient(135deg, #3ece8a, #23a970);
      box-shadow: 0 13px 30px rgba(31, 189, 120, .25);
      font-size: 18px;
      font-weight: 760;
      transition: transform .15s ease, filter .15s ease;
    }

    .call-btn:hover {
      filter: brightness(1.08);
      transform: translateY(-1px);
    }

    .round-btn {
      min-height: 63px;
      border-radius: 20px;
      background: rgba(62, 77, 117, .8);
      border: 1px solid rgba(177, 199, 255, .11);
      color: #eff3ff;
      font-size: 15px;
      font-weight: 700;
    }

    .round-btn.muted {
      color: #ffdce3;
      background: rgba(133, 54, 74, .92);
    }

    .round-btn.hangup {
      background: linear-gradient(135deg, #f2687f, #c63e57);
      box-shadow: 0 10px 26px rgba(232, 74, 103, .2);
    }

    .hint {
      flex: 0 0 auto;
      margin: 16px 5px 0;
      color: #7f8aaa;
      text-align: center;
      font-size: 11px;
      line-height: 1.55;
    }

    [hidden] {
      display: none !important;
    }

    @media (max-height: 720px) {
      .stage {
        padding-top: 28px;
      }

      .orb-wrap {
        transform: scale(.82);
        margin-bottom: -17px;
      }

      .status {
        margin-top: 20px;
      }

      .captions {
        margin-top: 20px;
        max-height: 26svh;
      }

      .controls {
        margin-top: 16px;
      }
    }
  </style>
</head>
<body data-phase="idle" data-speaking="false">
  <main class="shell">
    <header class="topbar">
      <div class="brand">
        <strong id="modelName">Gemini Live</strong>
        <span>端到端加密连接由 Worker 代理</span>
      </div>
    </header>

    <section class="stage" aria-live="polite">
      <div class="orb-wrap" aria-hidden="true">
        <div class="ring"></div>
        <div id="orb" class="orb">G</div>
      </div>

      <div class="status">
        <div id="statusMain" class="status-main">准备通话</div>
        <div id="statusSub" class="status-sub">点击开始通话，然后允许麦克风权限。</div>
        <div id="timer" class="timer"></div>
      </div>
    </section>

    <section id="captions" class="captions" aria-label="实时字幕">
      <div id="captionEmpty" class="caption-empty">
        通话接通后，这里会显示你和 Gemini 的实时字幕。
      </div>
    </section>

    <section class="controls">
      <button id="callButton" class="call-btn" type="button">
        ☎　开始通话
      </button>

      <button id="muteButton" class="round-btn" type="button" disabled>
        🎙　静音
      </button>

      <button id="endButton" class="round-btn hangup" type="button" disabled>
        ☎　挂断
      </button>
    </section>

    <p class="hint">
      说话时可以随时打断 Gemini。建议使用耳机，以降低扬声器回声被麦克风再次采集的概率。
    </p>
  </main>

  <script>
    const AUTH_TOKEN = __AUTH_TOKEN_PLACEHOLDER__;
    const callButton = document.getElementById('callButton');
    const muteButton = document.getElementById('muteButton');
    const endButton = document.getElementById('endButton');

    const orb = document.getElementById('orb');
    const statusMain = document.getElementById('statusMain');
    const statusSub = document.getElementById('statusSub');
    const timerEl = document.getElementById('timer');
    const modelName = document.getElementById('modelName');

    const captions = document.getElementById('captions');
    const captionEmpty = document.getElementById('captionEmpty');

    let socket = null;
    let sessionReady = false;
    let muted = false;
    let manuallyEnded = false;
    let lastFailure = '';

    let audioContext = null;
    let mediaStream = null;
    let micSource = null;
    let micProcessor = null;
    let silentMicSink = null;

    let connectTimer = null;
    let durationTimer = null;
    let callStartedAt = 0;

    let nextPlayTime = 0;
    const playingSources = new Set();

    let activeInputLine = null;
    let activeOutputLine = null;

    function workerWebSocketUrl() {
      const scheme = location.protocol === 'https:' ? 'wss://' : 'ws://';
      let wsUrl = scheme + location.host + '/ws';
      if (AUTH_TOKEN) {
        wsUrl += '?auth=' + encodeURIComponent(AUTH_TOKEN);
      }
      return wsUrl;
    }

    function setPhase(phase) {
      document.body.dataset.phase = phase;
    }

    function setSpeaking(value) {
      document.body.dataset.speaking = value ? 'true' : 'false';
    }

    function setEnergy(value) {
      const safe = Math.max(0, Math.min(1, value || 0));
      document.documentElement.style.setProperty('--energy', String(safe));
    }

    function setStatus(main, sub, isError) {
      statusMain.textContent = main;
      statusSub.textContent = sub || '';
      statusMain.style.color = isError ? '#ff94a4' : '';
    }

    function refreshButtons() {
      const active = !!socket;

      callButton.hidden = active;
      endButton.disabled = !active;
      muteButton.disabled = !sessionReady;

      muteButton.classList.toggle('muted', muted);
      muteButton.textContent = muted ? '🔇　取消静音' : '🎙　静音';
    }

    function clearConnectTimer() {
      if (connectTimer) {
        clearTimeout(connectTimer);
        connectTimer = null;
      }
    }

    function startDurationTimer() {
      stopDurationTimer();

      callStartedAt = Date.now();

      durationTimer = setInterval(function () {
        const seconds = Math.floor((Date.now() - callStartedAt) / 1000);
        const minutes = String(Math.floor(seconds / 60)).padStart(2, '0');
        const rest = String(seconds % 60).padStart(2, '0');
        timerEl.textContent = minutes + ':' + rest;
      }, 1000);

      timerEl.textContent = '00:00';
    }

    function stopDurationTimer() {
      if (durationTimer) {
        clearInterval(durationTimer);
        durationTimer = null;
      }

      timerEl.textContent = '';
    }

    function clearCaptions() {
      captions.replaceChildren();
      activeInputLine = null;
      activeOutputLine = null;
    }

    function scrollCaptionsToBottom() {
      captions.scrollTop = captions.scrollHeight;
    }

    function addNote(text) {
      captionEmpty.hidden = true;

      const line = document.createElement('div');
      line.className = 'line note';
      line.textContent = text;

      captions.appendChild(line);
      scrollCaptionsToBottom();
    }

    function createCaptionLine(kind, label) {
      captionEmpty.hidden = true;

      const line = document.createElement('div');
      line.className = 'line ' + kind;

      const who = document.createElement('span');
      who.className = 'who';
      who.textContent = label;

      const text = document.createElement('span');
      text.className = 'text';

      line.appendChild(who);
      line.appendChild(text);

      captions.appendChild(line);
      scrollCaptionsToBottom();

      return line;
    }

    function updateInputCaption(text, interim) {
      if (!text) return;

      if (!activeInputLine) {
        activeInputLine = createCaptionLine('me', '你');
      }

      activeInputLine.querySelector('.text').textContent = text;
      activeInputLine.classList.toggle('live', interim);

      if (!interim) {
        activeInputLine = null;
      }

      scrollCaptionsToBottom();
    }

    function appendOutputCaption(text) {
      if (!text) return;

      if (!activeOutputLine) {
        activeOutputLine = createCaptionLine('assistant', 'Gemini');
      }

      activeOutputLine.querySelector('.text').textContent += text;
      scrollCaptionsToBottom();
    }

    async function ensureAudioContext() {
      if (!audioContext) {
        const AudioContextClass =
          window.AudioContext || window.webkitAudioContext;

        if (!AudioContextClass) {
          throw new Error('当前浏览器不支持 Web Audio API。');
        }

        audioContext = new AudioContextClass();
      }

      if (audioContext.state === 'suspended') {
        await audioContext.resume();
      }

      return audioContext;
    }

    async function startMicrophoneCapture() {
      const ctx = await ensureAudioContext();

      const getMicStream = async (constraints) => {
        if (navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
          return navigator.mediaDevices.getUserMedia(constraints);
        }

        const legacyGetUserMedia =
          navigator.getUserMedia ||
          navigator.webkitGetUserMedia ||
          navigator.mozGetUserMedia ||
          navigator.msGetUserMedia;

        if (legacyGetUserMedia) {
          return new Promise((resolve, reject) => {
            legacyGetUserMedia.call(navigator, constraints, resolve, reject);
          });
        }

        if (window.isSecureContext === false || location.protocol !== 'https:') {
          throw new Error('手机端调用麦克风必须在 HTTPS 安全环境下运行，请使用 https:// 域名访问。');
        }

        throw new Error('当前浏览器不支持获取麦克风音频。');
      };

      mediaStream = await getMicStream({
        audio: {
          channelCount: 1,
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
        },
      });

      micSource = ctx.createMediaStreamSource(mediaStream);

      /*
       * 缓冲区从 4096 减小为 2048，直接降低近一半的声音采集延迟（约 40ms）。
       */
      micProcessor = ctx.createScriptProcessor(2048, 1, 1);
      silentMicSink = ctx.createGain();
      silentMicSink.gain.value = 0;

      micProcessor.onaudioprocess = function (event) {
        const input = event.inputBuffer.getChannelData(0);
        const level = rms(input);

        if (!muted) {
          setEnergy(Math.min(0.72, level * 9));
        }

        if (
          !sessionReady ||
          muted ||
          !socket ||
          socket.readyState !== WebSocket.OPEN
        ) {
          return;
        }

        /*
         * 防止网络暂时堵塞时积压大量实时音频。
         */
        if (socket.bufferedAmount > 512 * 1024) {
          return;
        }

        const pcm16 = downsampleFloat32ToPcm16(
          input,
          ctx.sampleRate,
          16000,
        );

        sendToWorker({
          type: 'audio',
          data: int16ArrayToBase64(pcm16),
        });
      };

      micSource.connect(micProcessor);
      micProcessor.connect(silentMicSink);
      silentMicSink.connect(ctx.destination);
    }

    function stopMicrophoneCapture() {
      if (micProcessor) {
        micProcessor.onaudioprocess = null;
        try {
          micProcessor.disconnect();
        } catch {}
      }

      if (micSource) {
        try {
          micSource.disconnect();
        } catch {}
      }

      if (silentMicSink) {
        try {
          silentMicSink.disconnect();
        } catch {}
      }

      if (mediaStream) {
        for (const track of mediaStream.getTracks()) {
          track.stop();
        }
      }

      micProcessor = null;
      micSource = null;
      silentMicSink = null;
      mediaStream = null;
    }

    function stopPlayback() {
      for (const source of playingSources) {
        try {
          source.stop();
        } catch {}
      }

      playingSources.clear();
      setSpeaking(false);
      setEnergy(0);

      if (audioContext) {
        nextPlayTime = audioContext.currentTime;
      } else {
        nextPlayTime = 0;
      }
    }

    async function closeAudio() {
      stopPlayback();
      stopMicrophoneCapture();

      if (audioContext) {
        try {
          await audioContext.close();
        } catch {}
      }

      audioContext = null;
    }

    async function startCall() {
      if (socket) return;

      manuallyEnded = false;
      lastFailure = '';
      muted = false;
      sessionReady = false;

      clearCaptions();
      addNote('正在请求麦克风权限并呼叫 Gemini…');

      setPhase('dialing');
      setStatus('正在拨号…', '正在连接 Worker 和 Gemini。');
      refreshButtons();

      try {
        /*
         * 麦克风权限必须尽可能在用户点击事件中请求，
         * 否则部分浏览器会拒绝权限请求。
         */
        await startMicrophoneCapture();
      } catch (error) {
        setPhase('idle');
        setStatus(
          '无法使用麦克风',
          error instanceof Error ? error.message : String(error),
          true,
        );
        addNote('麦克风启动失败。请检查浏览器权限。');
        await closeAudio();
        refreshButtons();
        return;
      }

      const ws = new WebSocket(workerWebSocketUrl());
      socket = ws;
      refreshButtons();

      connectTimer = setTimeout(function () {
        if (socket === ws && !sessionReady) {
          lastFailure =
            '连接等待超过 20 秒。请检查 Worker 环境变量或查看 wrangler tail 日志。';
          ws.close(1011, 'connection timeout');
        }
      }, 20_000);

      ws.addEventListener('open', function () {
        if (socket !== ws) return;

        setStatus(
          '正在接通…',
          'Worker 已连接，正在创建 Gemini Live 会话。',
        );
      });

      ws.addEventListener('message', function (event) {
        if (socket !== ws) return;

        let message;

        try {
          message = JSON.parse(event.data);
        } catch {
          lastFailure = 'Worker 返回了无法解析的消息。';
          return;
        }

        handleWorkerMessage(message);
      });

      ws.addEventListener('error', function () {
        if (socket !== ws) return;

        lastFailure =
          '浏览器与 Worker 的 WebSocket 连接发生错误。';
      });

      ws.addEventListener('close', function (event) {
        if (socket !== ws) return;

        clearConnectTimer();

        const endedByUser = manuallyEnded;
        socket = null;
        sessionReady = false;

        void closeAudio();

        stopDurationTimer();
        setPhase('idle');
        refreshButtons();

        if (endedByUser) {
          setStatus('通话已结束', '点击“开始通话”可重新连接。');
          return;
        }

        const detail =
          lastFailure ||
          (event.reason
            ? event.reason
            : '连接已关闭（代码 ' + event.code + '）。');

        setStatus('通话未能建立', detail, true);
        addNote('连接结束：' + detail);
      });
    }

    function endCall() {
      manuallyEnded = true;
      clearConnectTimer();

      if (socket) {
        const ws = socket;
        socket = null;

        try {
          ws.close(1000, 'user ended call');
        } catch {}
      }

      sessionReady = false;
      stopDurationTimer();
      setPhase('idle');

      void closeAudio();

      setStatus('通话已结束', '点击“开始通话”可重新连接。');
      refreshButtons();
    }

    function toggleMute() {
      if (!sessionReady) return;

      muted = !muted;

      if (muted) {
        sendToWorker({ type: 'audioEnd' });
        setStatus('已静音', 'Gemini 仍可继续说话。');
      } else {
        setStatus('通话中', '正在聆听。');
      }

      refreshButtons();
    }

    function sendToWorker(payload) {
      if (!socket || socket.readyState !== WebSocket.OPEN) {
        return false;
      }

      socket.send(JSON.stringify(payload));
      return true;
    }

    function handleWorkerMessage(message) {
      if (message.type === 'status') {
        setStatus(
          message.state === 'setting-up'
            ? '正在接通…'
            : '正在拨号…',
          message.message || '',
        );
        return;
      }

      if (message.type === 'ready') {
        clearConnectTimer();

        sessionReady = true;
        muted = false;

        const model = message.model || 'Gemini Live';
        modelName.textContent = model;

        setPhase('connected');
        setStatus('通话中', '正在聆听。');
        addNote('已接通。现在可以自然说话。');

        startDurationTimer();
        refreshButtons();
        return;
      }

      if (message.type === 'error') {
        lastFailure = message.message || 'Gemini 连接失败。';
        setStatus('连接错误', lastFailure, true);
        addNote('错误：' + lastFailure);
        return;
      }

      if (message.type === 'closed') {
        lastFailure = message.message || 'Gemini 已关闭连接。';
        return;
      }

      if (message.type === 'gemini') {
        handleGeminiPayload(message.data);
      }
    }

    function handleGeminiPayload(raw) {
      let packet;

      try {
        packet = JSON.parse(raw);
      } catch {
        return;
      }

      if (packet.error) {
        lastFailure =
          packet.error.message ||
          JSON.stringify(packet.error).slice(0, 400);
        return;
      }

      const serverContent = packet.serverContent;
      if (!serverContent) return;

      if (serverContent.interrupted) {
        stopPlayback();
        activeOutputLine = null;

        if (!muted) {
          setStatus('通话中', '检测到你开始说话，已打断 Gemini。');
        }
      }

      if (
        serverContent.interimInputTranscription &&
        typeof serverContent.interimInputTranscription.text === 'string'
      ) {
        updateInputCaption(
          serverContent.interimInputTranscription.text,
          true,
        );
      }

      if (
        serverContent.inputTranscription &&
        typeof serverContent.inputTranscription.text === 'string'
      ) {
        updateInputCaption(
          serverContent.inputTranscription.text,
          false,
        );
      }

      if (
        serverContent.outputTranscription &&
        typeof serverContent.outputTranscription.text === 'string'
      ) {
        appendOutputCaption(serverContent.outputTranscription.text);

        if (!muted) {
          setStatus('Gemini 正在说话', '你可以直接开口打断它。');
        }
      }

      const parts =
        serverContent.modelTurn &&
        Array.isArray(serverContent.modelTurn.parts)
          ? serverContent.modelTurn.parts
          : [];

      for (const part of parts) {
        if (typeof part.text === 'string' && part.text) {
          appendOutputCaption(part.text);
        }

        if (
          part.inlineData &&
          typeof part.inlineData.data === 'string' &&
          typeof part.inlineData.mimeType === 'string' &&
          part.inlineData.mimeType.indexOf('audio/pcm') !== -1
        ) {
          void playPcm24k(part.inlineData.data);
        }
      }

      if (serverContent.waitingForInput && !muted) {
        setStatus('通话中', '正在聆听。');
      }

      if (serverContent.turnComplete) {
        activeOutputLine = null;

        if (!muted && playingSources.size === 0) {
          setStatus('通话中', '正在聆听。');
        }
      }
    }

    async function playPcm24k(base64) {
      try {
        const ctx = await ensureAudioContext();
        const bytes = base64ToBytes(base64);

        if (bytes.byteLength < 2) return;

        const count = Math.floor(bytes.byteLength / 2);
        const buffer = ctx.createBuffer(1, count, 24000);
        const channel = buffer.getChannelData(0);
        const view = new DataView(
          bytes.buffer,
          bytes.byteOffset,
          bytes.byteLength,
        );

        let sum = 0;

        for (let i = 0; i < count; i++) {
          const value = view.getInt16(i * 2, true) / 32768;
          channel[i] = value;
          sum += value * value;
        }

        const source = ctx.createBufferSource();
        source.buffer = buffer;
        source.connect(ctx.destination);

        const now = ctx.currentTime;
        if (nextPlayTime < now) {
          nextPlayTime = now;
        }
        const startAt = Math.max(now + 0.008, nextPlayTime);

        source.start(startAt);
        nextPlayTime = startAt + buffer.duration;

        playingSources.add(source);
        setSpeaking(true);
        setEnergy(Math.min(0.9, Math.sqrt(sum / count) * 10));

        source.addEventListener('ended', function () {
          playingSources.delete(source);

          if (playingSources.size === 0) {
            setSpeaking(false);

            if (!muted) {
              setEnergy(0);

              if (sessionReady) {
                setStatus('通话中', '正在聆听。');
              }
            }
          }
        });
      } catch (error) {
        console.warn('PCM 播放失败：', error);
      }
    }

    function rms(samples) {
      let sum = 0;

      for (let i = 0; i < samples.length; i++) {
        sum += samples[i] * samples[i];
      }

      return Math.sqrt(sum / Math.max(samples.length, 1));
    }

    function downsampleFloat32ToPcm16(input, inputRate, outputRate) {
      if (inputRate === outputRate) {
        const direct = new Int16Array(input.length);

        for (let i = 0; i < input.length; i++) {
          const sample = Math.max(-1, Math.min(1, input[i]));
          direct[i] =
            sample < 0
              ? Math.round(sample * 32768)
              : Math.round(sample * 32767);
        }

        return direct;
      }

      const ratio = inputRate / outputRate;
      const outputLength = Math.round(input.length / ratio);
      const output = new Int16Array(outputLength);

      let inputOffset = 0;

      for (let outputOffset = 0; outputOffset < outputLength; outputOffset++) {
        const nextInputOffset = Math.min(
          input.length,
          Math.round((outputOffset + 1) * ratio),
        );

        let sum = 0;
        let count = 0;

        for (let i = inputOffset; i < nextInputOffset; i++) {
          sum += input[i];
          count++;
        }

        const sample = Math.max(
          -1,
          Math.min(1, count ? sum / count : 0),
        );

        output[outputOffset] =
          sample < 0
            ? Math.round(sample * 32768)
            : Math.round(sample * 32767);

        inputOffset = nextInputOffset;
      }

      return output;
    }

    function int16ArrayToBase64(samples) {
      const bytes = new Uint8Array(
        samples.buffer,
        samples.byteOffset,
        samples.byteLength,
      );

      let binary = '';

      for (let i = 0; i < bytes.length; i++) {
        binary += String.fromCharCode(bytes[i]);
      }

      return btoa(binary);
    }

    function base64ToBytes(base64) {
      const binary = atob(base64.replace(/\s/g, ''));
      const bytes = new Uint8Array(binary.length);

      for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
      }

      return bytes;
    }

    callButton.addEventListener('click', function () {
      void startCall();
    });

    muteButton.addEventListener('click', toggleMute);
    endButton.addEventListener('click', endCall);

    window.addEventListener('beforeunload', function () {
      manuallyEnded = true;

      if (socket) {
        try {
          socket.close(1000, 'page unload');
        } catch {}
      }
    });

    refreshButtons();
  </script>
</body>
</html>`;
