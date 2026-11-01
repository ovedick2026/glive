import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import { URL } from "url";

const PORT = process.env.PORT || 3000;
const DEFAULT_GEMINI_WS_URL =
  "wss://generativelanguage.googleapis.com/ws/" +
  "google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";

const DEFAULT_MODEL = "gemini-2.0-flash-exp";
const SETUP_TIMEOUT_MS = 15_000;

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  // 密码访问验证：配置了 ACCESS_PASSWORD 时生效
  if (process.env.ACCESS_PASSWORD) {
    const expectedPassword = String(process.env.ACCESS_PASSWORD).trim();
    const cookieAuth = req.headers.cookie?.match(/(?:^|;\s*)auth=([^;]+)/)?.[1];
    const queryPwd = url.searchParams.get("pwd");
    const authHeader = req.headers.authorization;

    let isAuthed = false;
    if (queryPwd && queryPwd === expectedPassword) {
      isAuthed = true;
    } else if (cookieAuth && decodeURIComponent(cookieAuth) === expectedPassword) {
      isAuthed = true;
    } else if (authHeader && authHeader.startsWith("Basic ")) {
      try {
        const decoded = Buffer.from(authHeader.slice(6), "base64").toString("utf-8");
        const inputPwd = decoded.includes(":") ? decoded.split(":")[1] : decoded;
        if (inputPwd === expectedPassword || decoded === expectedPassword) {
          isAuthed = true;
        }
      } catch {}
    }

    if (!isAuthed) {
      res.writeHead(401, {
        "WWW-Authenticate": 'Basic realm="Gemini Live Voice"',
        "Content-Type": "text/plain; charset=UTF-8",
      });
      return res.end("Unauthorized");
    }
  }

  if (url.pathname !== "/" || req.method !== "GET") {
    res.writeHead(404, { "Content-Type": "text/plain" });
    return res.end("Not Found");
  }

  const headers = {
    "content-type": "text/html; charset=UTF-8",
    "cache-control": "no-store",
    "x-content-type-options": "nosniff",
    "referrer-policy": "no-referrer",
    "permissions-policy": "microphone=(self)",
  };

  if (process.env.ACCESS_PASSWORD && url.searchParams.get("pwd") === String(process.env.ACCESS_PASSWORD).trim()) {
    headers["set-cookie"] = `auth=${encodeURIComponent(process.env.ACCESS_PASSWORD)}; Path=/; Max-Age=2592000; HttpOnly; SameSite=Lax`;
  }

  res.writeHead(200, headers);
  return res.end(HTML);
});

// WebSocket 服务
const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname !== "/ws") {
    socket.destroy();
    return;
  }

  if (process.env.ACCESS_PASSWORD) {
    const expectedPassword = String(process.env.ACCESS_PASSWORD).trim();
    const cookieAuth = req.headers.cookie?.match(/(?:^|;\s*)auth=([^;]+)/)?.[1];
    const queryPwd = url.searchParams.get("pwd");

    let isAuthed = false;
    if (queryPwd && queryPwd === expectedPassword) isAuthed = true;
    if (cookieAuth && decodeURIComponent(cookieAuth) === expectedPassword) isAuthed = true;

    if (!isAuthed) {
      socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
      socket.destroy();
      return;
    }
  }

  wss.handleUpgrade(req, socket, head, (browser) => {
    wss.emit("connection", browser, req);
  });
});

wss.on("connection", (browser) => {
  const bridge = createGeminiBridge(browser);
  bridge.install();
  bridge.connect();
});

server.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});

function createGeminiBridge(browser) {
  let upstream = null;
  let phase = "connecting";
  let closed = false;
  let setupTimer = null;
  let customVoice = null;

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
    } catch {}
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
    console.error("[Gemini Live]", message);
    sendToBrowser({ type: "error", message });
    closeEverything(1011, "Gemini Live connection failed");
  }

  async function connect() {
    let config;
    try {
      config = getGeminiConfig(customVoice);
    } catch (error) {
      fail(errorMessage(error));
      return;
    }

    sendToBrowser({
      type: "status",
      state: "connecting",
      message: "正在连接 Gemini…",
    });

    try {
      upstream = new WebSocket(config.fetchUrl);
    } catch (error) {
      fail("无法建立到 Gemini 的 WebSocket 连接：" + errorMessage(error));
      return;
    }

    upstream.on("open", () => {
      if (closed) return;
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
          fail("等待 Gemini setupComplete 超时（15 秒）。请检查 API 权限与配置。");
        }
      }, SETUP_TIMEOUT_MS);
    });

    upstream.on("message", (data) => {
      handleGeminiMessage(data);
    });

    upstream.on("error", (err) => {
      if (!closed) {
        fail("Gemini 上游 WebSocket 出错: " + errorMessage(err));
      }
    });

    upstream.on("close", (code, reason) => {
      if (closed) return;
      const rText = reason.toString() || `代码 ${code}`;
      sendToBrowser({
        type: "closed",
        code,
        message: "Gemini 已关闭连接：" + rText,
      });
      closeEverything(code === 1000 ? 1000 : 1011, "Gemini upstream closed");
    });
  }

  function install() {
    browser.on("message", (data) => {
      handleBrowserMessage(data);
    });

    browser.on("error", () => {
      closeEverything(1011, "browser websocket error");
    });

    browser.on("close", () => {
      closeEverything(1000, "browser closed");
    });
  }

  function handleBrowserMessage(data) {
    if (closed) return;

    let message;
    try {
      message = JSON.parse(data.toString());
    } catch {
      sendToBrowser({ type: "error", message: "浏览器发送了无效的 JSON 消息。" });
      return;
    }

    // 客户端指定音色配置（在通话建立前）
    if (message.type === "setVoice") {
      customVoice = message.voice;
      return;
    }

    if (phase !== "ready" || !upstream) {
      sendToBrowser({ type: "error", message: "Gemini 尚未完成初始化，请稍候。" });
      return;
    }

    if (upstream.readyState !== WebSocket.OPEN) {
      sendToBrowser({ type: "error", message: "Gemini 连接当前不可用。" });
      return;
    }

    if (message.type === "audio") {
      const audioData = typeof message.data === "string" ? message.data : "";
      if (!audioData || audioData.length > 128 * 1024 || !/^[A-Za-z0-9+/=]+$/.test(audioData)) {
        sendToBrowser({ type: "error", message: "麦克风音频数据格式不正确。" });
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
        })
      );
      return;
    }

    if (message.type === "audioEnd") {
      upstream.send(JSON.stringify({ realtimeInput: { audioStreamEnd: true } }));
      return;
    }

    sendToBrowser({ type: "error", message: "未知浏览器消息类型。" });
  }

  function handleGeminiMessage(data) {
    if (closed) return;

    const raw = data.toString();
    let packet;

    try {
      packet = JSON.parse(raw);
    } catch {
      fail("Gemini 返回了无法解析的非 JSON 消息。");
      return;
    }

    if (packet && Object.prototype.hasOwnProperty.call(packet, "setupComplete")) {
      clearSetupTimer();
      phase = "ready";
      sendToBrowser({
        type: "ready",
        model: getSafeModelName(),
      });
    }

    if (packet && packet.error) {
      const message = packet.error.message || packet.error.status || JSON.stringify(packet.error).slice(0, 500);
      fail("Gemini 返回错误：" + message);
      return;
    }

    sendToBrowser({
      type: "gemini",
      data: raw,
    });
  }

  return { install, connect };
}

function getGeminiConfig(customVoice) {
  const apiKey = requiredEnv("GEMINI_API_KEY");
  const systemPrompt = process.env.GEMINI_SYSTEM_PROMPT || "You are a friendly and helpful conversational voice AI.";
  const rawEndpoint = String(process.env.GEMINI_WS_URL || DEFAULT_GEMINI_WS_URL).trim();
  const endpoint = new URL(rawEndpoint);

  endpoint.searchParams.set("key", apiKey);

  const model = getSafeModelName();
  const voice = String(customVoice || process.env.GEMINI_VOICE || "Aoede").trim();

  return {
    fetchUrl: endpoint.toString(),
    model,
    voice,
    systemPrompt,
  };
}

function getSafeModelName() {
  const model = String(process.env.GEMINI_MODEL || DEFAULT_MODEL)
    .trim()
    .replace(/^models\//, "");
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
      generationConfig,
      systemInstruction: {
        parts: [{ text: config.systemPrompt }],
      },
      inputAudioTranscription: {},
      outputAudioTranscription: {},
    },
  };
}

function requiredEnv(name) {
  const value = typeof process.env[name] === "string" ? process.env[name].trim() : "";
  if (!value) throw new Error("缺少环境变量：" + name);
  return value;
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

const HTML = String.raw`<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">
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

    /* 完美适配移动端与 iPad，解决高度溢出遮挡底部按钮 */
    html, body {
      height: 100%;
      height: 100dvh;
      margin: 0;
      padding: 0;
      overflow: hidden;
    }

    body {
      color: var(--text);
      background:
        radial-gradient(circle at 50% -10%, #253975 0%, transparent 42%),
        radial-gradient(circle at 0% 100%, #133d47 0%, transparent 40%),
        var(--bg);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", PingFang SC, sans-serif;
    }

    .shell {
      width: min(100%, 520px);
      height: 100%;
      height: 100dvh;
      margin: 0 auto;
      padding: max(12px, env(safe-area-inset-top))
               18px
               max(20px, env(safe-area-inset-bottom));
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }

    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      min-height: 40px;
    }

    .brand strong {
      letter-spacing: .02em;
      font-size: 15px;
    }

    /* 音色选择下拉框 */
    .voice-wrapper {
      display: flex;
      align-items: center;
      gap: 6px;
      background: rgba(255, 255, 255, .08);
      border: 1px solid var(--panel-border);
      border-radius: 12px;
      padding: 4px 8px;
    }

    .voice-wrapper span {
      font-size: 11px;
      color: var(--muted);
    }

    select {
      background: transparent;
      color: var(--text);
      border: none;
      font-size: 13px;
      font-weight: 600;
      outline: none;
      cursor: pointer;
    }

    select option {
      background: #151b2e;
      color: #fff;
    }

    .stage {
      flex: 0 0 auto;
      padding-top: clamp(10px, 3.5vh, 36px);
      display: grid;
      justify-items: center;
      text-align: center;
    }

    .orb-wrap {
      position: relative;
      width: 150px;
      height: 150px;
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

    .ring::before { inset: -12px; opacity: .55; }
    .ring::after { inset: -25px; opacity: .18; }

    .orb {
      width: 125px;
      height: 125px;
      border-radius: 50%;
      display: grid;
      place-items: center;
      color: white;
      font-size: 46px;
      font-weight: 750;
      background:
        radial-gradient(circle at 31% 25%, #dce6ff 0%, #9cb5ff 15%, transparent 16%),
        radial-gradient(circle at 38% 28%, #7793ff 0%, #425fc8 43%, #263d93 100%);
      box-shadow:
        0 0 calc(20px + var(--energy) * 50px) rgba(106, 143, 255, .56),
        0 14px 35px rgba(0, 0, 0, .34);
      transform: scale(calc(1 + var(--energy) * .11));
      transition: transform .08s linear, box-shadow .1s linear;
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
      margin-top: 24px;
      min-height: 42px;
    }

    .status-main {
      font-size: 20px;
      font-weight: 720;
    }

    .status-sub {
      margin-top: 5px;
      color: var(--muted);
      font-size: 13px;
    }

    .timer {
      margin-top: 6px;
      min-height: 18px;
      color: #d7e0ff;
      font-variant-numeric: tabular-nums;
      font-size: 14px;
      letter-spacing: .08em;
    }

    .captions {
      flex: 1 1 auto;
      min-height: 100px;
      max-height: 32svh;
      margin: 15px 0 10px;
      padding: 12px;
      overflow-y: auto;
      border: 1px solid var(--panel-border);
      border-radius: 18px;
      background: linear-gradient(145deg, rgba(20, 28, 49, .77), rgba(11, 16, 30, .5));
      scrollbar-width: thin;
      -webkit-overflow-scrolling: touch;
    }

    .caption-empty {
      height: 100%;
      display: grid;
      place-items: center;
      color: #8490b2;
      text-align: center;
      font-size: 13px;
    }

    .line {
      margin: 0 0 11px;
      line-height: 1.5;
      font-size: 13.5px;
      overflow-wrap: anywhere;
    }

    .line:last-child { margin-bottom: 0; }

    .line .who {
      display: block;
      margin-bottom: 2px;
      color: #97a8db;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: .06em;
      text-transform: uppercase;
    }

    .line.me .who { color: #7ee7b6; }
    .line.note { color: #8e9abb; font-size: 12px; }

    .line.live .text::after {
      content: " ···";
      color: #9eb1ee;
      animation: dots 1s infinite step-end;
    }

    @keyframes dots { 0%, 100% { opacity: .25; } 50% { opacity: 1; } }

    .controls {
      flex: 0 0 auto;
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
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
      height: 58px;
      border-radius: 20px;
      background: linear-gradient(135deg, #3ece8a, #23a970);
      box-shadow: 0 10px 25px rgba(31, 189, 120, .25);
      font-size: 17px;
      font-weight: 760;
    }

    .round-btn {
      height: 52px;
      border-radius: 18px;
      background: rgba(62, 77, 117, .8);
      border: 1px solid rgba(177, 199, 255, .11);
      color: #eff3ff;
      font-size: 14.5px;
      font-weight: 700;
    }

    .round-btn.muted {
      color: #ffdce3;
      background: rgba(133, 54, 74, .92);
    }

    .round-btn.hangup {
      background: linear-gradient(135deg, #f2687f, #c63e57);
      box-shadow: 0 8px 20px rgba(232, 74, 103, .2);
    }

    .hint {
      flex: 0 0 auto;
      margin: 10px 0 0;
      color: #7f8aaa;
      text-align: center;
      font-size: 11px;
    }

    [hidden] { display: none !important; }
  </style>
</head>
<body data-phase="idle" data-speaking="false">
  <main class="shell">
    <header class="topbar">
      <div class="brand">
        <strong id="modelName">Gemini Live</strong>
      </div>
      <div class="voice-wrapper">
        <span>音色:</span>
        <select id="voiceSelector">
          <option value="Aoede" selected>Aoede (空灵女声)</option>
          <option value="Kore">Kore (亲切女声)</option>
          <option value="Puck">Puck (活泼男声)</option>
          <option value="Fenrir">Fenrir (磁性男声)</option>
          <option value="Charon">Charon (低沉男声)</option>
        </select>
      </div>
    </header>

    <section class="stage">
      <div class="orb-wrap">
        <div class="ring"></div>
        <div id="orb" class="orb">G</div>
      </div>

      <div class="status">
        <div id="statusMain" class="status-main">准备通话</div>
        <div id="statusSub" class="status-sub">点击开始通话，允许麦克风权限。</div>
        <div id="timer" class="timer"></div>
      </div>
    </section>

    <section id="captions" class="captions">
      <div id="captionEmpty" class="caption-empty">接通后，这里将显示实时对话字幕。</div>
    </section>

    <section class="controls">
      <button id="callButton" class="call-btn" type="button">☎　开始通话</button>
      <button id="muteButton" class="round-btn" type="button" disabled>🎙　静音</button>
      <button id="endButton" class="round-btn hangup" type="button" disabled>☎　挂断</button>
    </section>

    <p class="hint">随时开口可打断 Gemini。推荐佩戴耳机通话。</p>
  </main>

  <script>
    const callButton = document.getElementById('callButton');
    const muteButton = document.getElementById('muteButton');
    const endButton = document.getElementById('endButton');
    const voiceSelector = document.getElementById('voiceSelector');

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

    // 核心改进：消除吞吞吐吐的自适应平滑排队播放队列
    let nextPlayTime = 0;
    const playingSources = new Set();
    const JITTER_BUFFER = 0.05; // 50ms 缓冲缝隙，确保网络波动时无缝拼接 PCM

    let activeInputLine = null;
    let activeOutputLine = null;

    function getWsUrl() {
      const scheme = location.protocol === 'https:' ? 'wss://' : 'ws://';
      return scheme + location.host + '/ws';
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
      voiceSelector.disabled = active;

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
        const AudioContextClass = window.AudioContext || window.webkitAudioContext;
        if (!AudioContextClass) throw new Error('当前浏览器不支持 Web Audio API。');
        audioContext = new AudioContextClass({ latencyHint: 'interactive' });
      }
      if (audioContext.state === 'suspended') {
        await audioContext.resume();
      }
      return audioContext;
    }

    async function startMicrophoneCapture() {
      const ctx = await ensureAudioContext();

      mediaStream = await navigator.mediaDevices.getUserMedia({
        audio: {
          channelCount: 1,
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
        },
      });

      micSource = ctx.createMediaStreamSource(mediaStream);
      micProcessor = ctx.createScriptProcessor(2048, 1, 1);
      silentMicSink = ctx.createGain();
      silentMicSink.gain.value = 0;

      micProcessor.onaudioprocess = function (event) {
        const input = event.inputBuffer.getChannelData(0);
        const level = rms(input);

        if (!muted) {
          setEnergy(Math.min(0.72, level * 9));
        }

        if (!sessionReady || muted || !socket || socket.readyState !== WebSocket.OPEN) {
          return;
        }

        if (socket.bufferedAmount > 512 * 1024) return;

        // 完整还原高精度的音频重采样逻辑
        const pcm16 = downsampleFloat32ToPcm16(input, ctx.sampleRate, 16000);

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
        try { micProcessor.disconnect(); } catch {}
      }
      if (micSource) {
        try { micSource.disconnect(); } catch {}
      }
      if (silentMicSink) {
        try { silentMicSink.disconnect(); } catch {}
      }
      if (mediaStream) {
        for (const track of mediaStream.getTracks()) track.stop();
      }
      micProcessor = null;
      micSource = null;
      silentMicSink = null;
      mediaStream = null;
    }

    function stopPlayback() {
      for (const source of playingSources) {
        try { source.stop(); } catch {}
      }
      playingSources.clear();
      setSpeaking(false);
      setEnergy(0);
      nextPlayTime = 0;
    }

    async function closeAudio() {
      stopPlayback();
      stopMicrophoneCapture();
      if (audioContext) {
        try { await audioContext.close(); } catch {}
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
      addNote('正在请求麦克风权限并建立通道…');
      setPhase('dialing');
      setStatus('正在拨号…', '连接服务器中');
      refreshButtons();

      try {
        await startMicrophoneCapture();
      } catch (error) {
        setPhase('idle');
        setStatus('无法使用麦克风', error.message || String(error), true);
        await closeAudio();
        refreshButtons();
        return;
      }

      const ws = new WebSocket(getWsUrl());
      socket = ws;
      refreshButtons();

      connectTimer = setTimeout(function () {
        if (socket === ws && !sessionReady) {
          lastFailure = '连接等待超时（20 秒）。请检查 Render 环境变量与服务状态。';
          ws.close(1011, 'connection timeout');
        }
      }, 20_000);

      ws.addEventListener('open', function () {
        if (socket !== ws) return;
        setStatus('正在接通…', '发送配置参数');
        ws.send(JSON.stringify({
          type: 'setVoice',
          voice: voiceSelector.value
        }));
      });

      ws.addEventListener('message', function (event) {
        if (socket !== ws) return;
        let message;
        try {
          message = JSON.parse(event.data);
        } catch {
          lastFailure = '服务器返回了无效的数据。';
          return;
        }
        handleWorkerMessage(message);
      });

      ws.addEventListener('error', function () {
        if (socket !== ws) return;
        lastFailure = '与服务端的 WebSocket 连接发生错误。';
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

        const detail = lastFailure || (event.reason ? event.reason : '连接关闭（' + event.code + '）');
        setStatus('通话中断', detail, true);
        addNote('连接断开：' + detail);
      });
    }

    function endCall() {
      manuallyEnded = true;
      clearConnectTimer();
      if (socket) {
        const ws = socket;
        socket = null;
        try { ws.close(1000, 'user ended call'); } catch {}
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
      if (!socket || socket.readyState !== WebSocket.OPEN) return false;
      socket.send(JSON.stringify(payload));
      return true;
    }

    function handleWorkerMessage(message) {
      if (message.type === 'status') {
        setStatus(message.state === 'setting-up' ? '正在接通…' : '正在拨号…', message.message || '');
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
        addNote('通话已建立，现在可以自然说话。');

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
      try { packet = JSON.parse(raw); } catch { return; }

      if (packet.error) {
        lastFailure = packet.error.message || JSON.stringify(packet.error).slice(0, 400);
        return;
      }

      const serverContent = packet.serverContent;
      if (!serverContent) return;

      // 打断逻辑：当模型检测到用户发声，停止正在播放的所有语音
      if (serverContent.interrupted) {
        stopPlayback();
        activeOutputLine = null;
        if (!muted) setStatus('通话中', '已打断回复。');
      }

      // 实时字幕流式拼接
      if (serverContent.interimInputTranscription?.text) {
        updateInputCaption(serverContent.interimInputTranscription.text, true);
      }
      if (serverContent.inputTranscription?.text) {
        updateInputCaption(serverContent.inputTranscription.text, false);
      }
      if (serverContent.outputTranscription?.text) {
        appendOutputCaption(serverContent.outputTranscription.text);
        if (!muted) setStatus('Gemini 正在说话', '可随时开口打断它。');
      }

      const parts = serverContent.modelTurn?.parts || [];
      for (const part of parts) {
        if (part.text) appendOutputCaption(part.text);
        if (part.inlineData?.mimeType?.includes('audio/pcm')) {
          playPcm24k(part.inlineData.data);
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

    // 核心优化函数：带有平滑排队的 24kHz PCM 播放调度器（彻底消灭打嗝和断断续续）
    async function playPcm24k(base64) {
      try {
        const ctx = await ensureAudioContext();
        const bytes = base64ToBytes(base64);
        if (bytes.byteLength < 2) return;

        const count = Math.floor(bytes.byteLength / 2);
        const buffer = ctx.createBuffer(1, count, 24000);
        const channel = buffer.getChannelData(0);
        const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

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
        // 如果当前时间已超过下一片段排队时间，说明由于网络有轻微卡顿，追加平滑缓冲时间，不立即截断播放
        if (nextPlayTime < now) {
          nextPlayTime = now + JITTER_BUFFER;
        }

        source.start(nextPlayTime);
        nextPlayTime += buffer.duration;

        playingSources.add(source);
        setSpeaking(true);
        setEnergy(Math.min(0.9, Math.sqrt(sum / count) * 10));

        source.addEventListener('ended', function () {
          playingSources.delete(source);
          if (playingSources.size === 0) {
            setSpeaking(false);
            if (!muted) {
              setEnergy(0);
              if (sessionReady) setStatus('通话中', '正在聆听。');
            }
          }
        });
      } catch (error) {
        console.warn('PCM 播放失败：', error);
      }
    }

    function rms(samples) {
      let sum = 0;
      for (let i = 0; i < samples.length; i++) sum += samples[i] * samples[i];
      return Math.sqrt(sum / Math.max(samples.length, 1));
    }

    // 完整的下采样滤波器逻辑
    function downsampleFloat32ToPcm16(input, inputRate, outputRate) {
      if (inputRate === outputRate) {
        const direct = new Int16Array(input.length);
        for (let i = 0; i < input.length; i++) {
          const sample = Math.max(-1, Math.min(1, input[i]));
          direct[i] = sample < 0 ? Math.round(sample * 32768) : Math.round(sample * 32767);
        }
        return direct;
      }

      const ratio = inputRate / outputRate;
      const outputLength = Math.round(input.length / ratio);
      const output = new Int16Array(outputLength);
      let inputOffset = 0;

      for (let outputOffset = 0; outputOffset < outputLength; outputOffset++) {
        const nextInputOffset = Math.min(input.length, Math.round((outputOffset + 1) * ratio));
        let sum = 0;
        let count = 0;

        for (let i = inputOffset; i < nextInputOffset; i++) {
          sum += input[i];
          count++;
        }

        const sample = Math.max(-1, Math.min(1, count ? sum / count : 0));
        output[outputOffset] = sample < 0 ? Math.round(sample * 32768) : Math.round(sample * 32767);
        inputOffset = nextInputOffset;
      }
      return output;
    }

    function int16ArrayToBase64(samples) {
      const bytes = new Uint8Array(samples.buffer, samples.byteOffset, samples.byteLength);
      let binary = '';
      for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
      return btoa(binary);
    }

    function base64ToBytes(base64) {
      const binary = atob(base64.replace(/\s/g, ''));
      const bytes = new Uint8Array(binary.length);
      for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
      return bytes;
    }

    callButton.addEventListener('click', () => { void startCall(); });
    muteButton.addEventListener('click', toggleMute);
    endButton.addEventListener('click', endCall);

    window.addEventListener('beforeunload', () => {
      manuallyEnded = true;
      if (socket) {
        try { socket.close(1000, 'page unload'); } catch {}
      }
    });

    refreshButtons();
  </script>
</body>
</html>`;
