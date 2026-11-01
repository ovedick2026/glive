import "dotenv/config";

import express from "express";
import { timingSafeEqual } from "node:crypto";
import { createServer } from "node:http";
import { WebSocket, WebSocketServer } from "ws";

const DEFAULT_GEMINI_WS_URL =
  "wss://generativelanguage.googleapis.com/ws/" +
  "google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";

const DEFAULT_MODEL = "gemini-3.8-live";
const SETUP_TIMEOUT_MS = 15_000;
const HEARTBEAT_INTERVAL_MS = 30_000;

let apiKeyRoundRobinIndex = 0;

const VOICE_CATALOG = [
  { name: "Kore", label: "Kore · 坚定 (极速)" },
  { name: "Puck", label: "Puck · 活泼 (极速)" },
  { name: "Leda", label: "Leda · 年轻 (极速)" },
  { name: "Aoede", label: "Aoede · 轻快" },
  { name: "Charon", label: "Charon · 知性" },
  { name: "Fenrir", label: "Fenrir · 热情" },
  { name: "Orus", label: "Orus · 稳重" },
  { name: "Zephyr", label: "Zephyr · 明亮" },
  { name: "Callirrhoe", label: "Callirrhoe · 随和" },
  { name: "Autonoe", label: "Autonoe · 明亮" },
  { name: "Enceladus", label: "Enceladus · 气声" },
  { name: "Iapetus", label: "Iapetus · 清晰" },
  { name: "Umbriel", label: "Umbriel · 随和" },
  { name: "Algieba", label: "Algieba · 柔和" },
  { name: "Despina", label: "Despina · 柔和" },
  { name: "Erinome", label: "Erinome · 清晰" },
  { name: "Algenib", label: "Algenib · 沙哑" },
  { name: "Rasalgethi", label: "Rasalgethi · 知性" },
  { name: "Laomedeia", label: "Laomedeia · 活泼" },
  { name: "Achernar", label: "Achernar · 柔和" },
  { name: "Alnilam", label: "Alnilam · 坚定" },
  { name: "Schedar", label: "Schedar · 平稳" },
  { name: "Gacrux", label: "Gacrux · 成熟" },
  { name: "Pulcherrima", label: "Pulcherrima · 直接" },
  { name: "Achird", label: "Achird · 友好" },
  { name: "Zubenelgenubi", label: "Zubenelgenubi · 随和" },
  { name: "Vindemiatrix", label: "Vindemiatrix · 温和" },
  { name: "Sadachbia", label: "Sadachbia · 活泼" },
  { name: "Sadaltager", label: "Sadaltager · 博学" },
  { name: "Sulafat", label: "Sulafat · 温暖" },
];

const voiceLabelMap = new Map(
  VOICE_CATALOG.map((voice) => [voice.name, voice.label]),
);

const app = express();
const server = createServer(app);

app.disable("x-powered-by");

app.use((req, res, next) => {
  res.set({
    "cache-control": "no-store",
    "x-content-type-options": "nosniff",
    "referrer-policy": "no-referrer",
    "permissions-policy": "microphone=(self)",
    "content-security-policy": [
      "default-src 'self'",
      "style-src 'unsafe-inline'",
      "script-src 'unsafe-inline'",
      "connect-src 'self' ws: wss:",
      "media-src 'self' blob:",
      "img-src 'self' data:",
      "base-uri 'none'",
      "form-action 'self'",
      "frame-ancestors 'none'",
    ].join("; "),
  });

  next();
});

/*
 * Render 健康检查不需要密码，否则新实例会因为 401 无法通过健康检查。
 */
app.get("/healthz", (_req, res) => {
  res.type("text/plain; charset=UTF-8").send("ok");
});

/*
 * 前端读取允许选择的声音列表。
 * 不返回 GEMINI_API_KEY、系统提示词等任何敏感配置。
 */
app.get("/config", requireHttpAuth, (_req, res) => {
  res.json({
    defaultVoice: getDefaultVoice(),
    voices: getVoiceOptions(),
  });
});

/*
 * 主页：保留原来的 ?pwd= 访问方式。
 * 访问成功时写入 HttpOnly Cookie，后续 WebSocket 会自动携带。
 */
app.get("/", requireHttpAuth, (req, res) => {
  const expectedPassword = getAccessPassword();

  if (expectedPassword && queryPasswordMatches(req, expectedPassword)) {
    const secure = isHttpsRequest(req) ? "; Secure" : "";

    res.append(
      "set-cookie",
      `auth=${encodeURIComponent(expectedPassword)}; Path=/; Max-Age=2592000; HttpOnly; SameSite=Lax${secure}`,
    );
  }

  res.type("text/html; charset=UTF-8").send(HTML);
});

app.use((req, res) => {
  res.status(404).type("text/plain; charset=UTF-8").send("Not Found");
});

app.use((error, _req, res, _next) => {
  console.error("[HTTP]", error);

  if (res.headersSent) {
    return;
  }

  res.status(500).type("text/plain; charset=UTF-8").send("Internal Server Error");
});

const wss = new WebSocketServer({
  noServer: true,
  clientTracking: true,
  maxPayload: 256 * 1024,
  perMessageDeflate: false, // 禁用压缩，降低实时语音 CPU 延迟
});

/*
 * 浏览器端 WS 已连接后的处理。
 */
wss.on("connection", (browser, request) => {
  browser.isAlive = true;

  // 禁用 Nagle 算法，让每一帧小音频包立即发出，降低网络抖动延迟
  if (browser._socket && typeof browser._socket.setNoDelay === "function") {
    browser._socket.setNoDelay(true);
  }

  browser.on("pong", function heartbeat() {
    this.isAlive = true;
  });

  const url = getRequestUrl(request);
  const selectedVoice = url.searchParams.get("voice") || "";
  const clientMemory = url.searchParams.get("memory") || ""; // <-- 新增这一行

  const bridge = createGeminiBridge(browser, selectedVoice, clientMemory); // <-- 传入 clientMemory
  bridge.install();
  void bridge.connect();
});

/*
 * Render / WebSocket 升级入口。
 */
server.on("upgrade", (request, socket, head) => {
  socket.setNoDelay(true);

  const url = getRequestUrl(request);

  if (url.pathname !== "/ws") {
    rejectUpgrade(socket, 404, "Not Found");
    return;
  }

  if (!isRequestAuthorized(request)) {
    rejectUpgrade(socket, 401, "Unauthorized", {
      "WWW-Authenticate": 'Basic realm="Gemini Live Voice"',
    });
    return;
  }

  if (!isAllowedWebSocketOrigin(request)) {
    rejectUpgrade(socket, 403, "Forbidden WebSocket origin");
    return;
  }

  wss.handleUpgrade(request, socket, head, (browser) => {
    wss.emit("connection", browser, request);
  });
});

/*
 * Render 官方建议对长连接做 ping/pong。
 * 这里会主动清理已经断开的浏览器 WebSocket。
 */
const heartbeatTimer = setInterval(() => {
  for (const client of wss.clients) {
    if (client.isAlive === false) {
      client.terminate();
      continue;
    }

    client.isAlive = false;

    try {
      client.ping();
    } catch {
      client.terminate();
    }
  }
}, HEARTBEAT_INTERVAL_MS);

heartbeatTimer.unref();

wss.on("close", () => {
  clearInterval(heartbeatTimer);
});

function createGeminiBridge(browser, selectedVoice, clientMemory = "") {
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
      if (socket.readyState === WebSocket.OPEN) {
        socket.close(code, reason.slice(0, 120));
      } else if (socket.readyState === WebSocket.CONNECTING) {
        socket.terminate();
      }
    } catch {
      // 关闭阶段忽略异常。
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

    console.error("[Gemini Live Render]", message);

    sendToBrowser({
      type: "error",
      message,
    });

    closeEverything(1011, "Gemini Live connection failed");
  }

  async function connect() {
    let config;

    try {
      config = getGeminiConfig(selectedVoice);
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
      upstream = new WebSocket(config.webSocketUrl, {
        handshakeTimeout: SETUP_TIMEOUT_MS,
        maxPayload: 2 * 1024 * 1024,
        perMessageDeflate: false,
      });
    } catch (error) {
      fail("无法建立到 Gemini 的 WebSocket 连接：" + errorMessage(error));
      return;
    }

    upstream.binaryType = "arraybuffer";

    upstream.once("open", () => {
      if (closed) {
        closeSocket(upstream, 1000, "browser already closed");
        return;
      }

      // 禁用上游 TCP Nagle 缓冲，降低往返延迟
      if (upstream._socket && typeof upstream._socket.setNoDelay === "function") {
        upstream._socket.setNoDelay(true);
      }

      phase = "setting-up";

      sendToBrowser({
        type: "status",
        state: "setting-up",
        message: "Gemini 已连接，正在初始化会话…",
      });

      try {
        upstream.send(JSON.stringify(createSetupMessage(config, clientMemory)));
      } catch (error) {
        fail("无法发送 Gemini setup 消息：" + errorMessage(error));
        return;
      }

      setupTimer = setTimeout(() => {
        if (phase === "setting-up" && !closed) {
          fail(
            "等待 Gemini setupComplete 超时（15 秒）。请检查 GEMINI_API_KEY、GEMINI_WS_URL、模型权限或 Render 日志。",
          );
        }
      }, SETUP_TIMEOUT_MS);
    });

    upstream.on("message", (data) => {
      void handleGeminiMessage(data);
    });

    upstream.on("error", (error) => {
      if (!closed) {
        fail(
          "Gemini 上游 WebSocket 出错：" +
            errorMessage(error) +
            "。请检查 API Key、模型权限、Endpoint 和网络。",
        );
      }
    });

    upstream.on("unexpected-response", (_request, response) => {
      let detail = "";

      response.on("data", (chunk) => {
        if (detail.length < 500) {
          detail += Buffer.from(chunk).toString("utf8");
        }
      });

      response.on("end", () => {
        const cleanDetail = detail.replace(/\s+/g, " ").trim().slice(0, 500);

        fail(
          "Gemini WebSocket 握手失败（HTTP " +
            response.statusCode +
            "）" +
            (cleanDetail ? "：" + cleanDetail : ""),
        );
      });
    });

    upstream.on("close", (code, reasonBuffer) => {
      if (closed) return;

      const reason = Buffer.isBuffer(reasonBuffer)
        ? reasonBuffer.toString("utf8")
        : String(reasonBuffer || "");

      const message = reason
        ? "Gemini 已关闭连接：" + reason
        : "Gemini 已关闭连接（代码 " + code + "）";

      sendToBrowser({
        type: "closed",
        code,
        message,
      });

      closeEverything(
        code === 1000 ? 1000 : 1011,
        "Gemini upstream closed",
      );
    });
  }

  function install() {
    browser.on("message", (data) => {
      void handleBrowserMessage(data);
    });

    browser.on("error", () => {
      closeEverything(1011, "browser websocket error");
    });

    browser.on("close", () => {
      closeEverything(1000, "browser closed");
    });
  }

  async function handleBrowserMessage(data) {
    if (closed) return;

    let message;

    try {
      message = JSON.parse(await wsDataToText(data));
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

      /*
       * 网络短暂堵塞时直接丢弃过旧的实时音频，
       * 不让 Node 进程和 Gemini 上游不断积压。
       */
      if (upstream.bufferedAmount > 256 * 1024) {
        return;
      }

      try {
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
      } catch (error) {
        fail("无法转发麦克风音频：" + errorMessage(error));
      }

      return;
    }

    if (message.type === "audioEnd") {
      try {
        upstream.send(
          JSON.stringify({
            realtimeInput: {
              audioStreamEnd: true,
            },
          }),
        );
      } catch (error) {
        fail("无法发送音频结束信号：" + errorMessage(error));
      }

      return;
    }

    sendToBrowser({
      type: "error",
      message: "未知浏览器消息类型。",
    });
  }

  async function handleGeminiMessage(data) {
    if (closed) return;

    let raw;

    try {
      raw = await wsDataToText(data);
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

    if (
      packet &&
      Object.prototype.hasOwnProperty.call(packet, "setupComplete")
    ) {
      clearSetupTimer();
      phase = "ready";

      sendToBrowser({
        type: "ready",
        model: getSafeModelName(),
      });
    }

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

/*
 * 支持在 GEMINI_API_KEY 中用英文逗号配置多个 Key（如 key1,key2,key3），
 * 多台设备同时通话时自动轮询分配，突破免费单 Key 并发上限。
 */
function pickGeminiApiKey() {
  const raw = requiredEnv("GEMINI_API_KEY");
  const keys = raw
    .split(",")
    .map((k) => k.trim())
    .filter(Boolean);

  if (keys.length === 0) {
    throw new Error("缺少 Render 环境变量：GEMINI_API_KEY");
  }

  const key = keys[apiKeyRoundRobinIndex % keys.length];
  apiKeyRoundRobinIndex = (apiKeyRoundRobinIndex + 1) % keys.length;
  return key;
}

function getGeminiConfig(selectedVoice) {
  const apiKey = pickGeminiApiKey();
  const systemPrompt = requiredEnv("GEMINI_SYSTEM_PROMPT");

  const rawEndpoint = String(
    process.env.GEMINI_WS_URL || DEFAULT_GEMINI_WS_URL,
  ).trim();

  const endpoint = new URL(rawEndpoint);

  if (endpoint.protocol === "https:") {
    endpoint.protocol = "wss:";
  }

  if (endpoint.protocol !== "wss:") {
    throw new Error(
      "GEMINI_WS_URL 必须是 https:// 或 wss:// 的 WebSocket Endpoint。",
    );
  }

  endpoint.searchParams.set("key", apiKey);

  const model = getSafeModelName();
  const defaultVoice = getDefaultVoice();
  const voice = String(selectedVoice || defaultVoice).trim();

  if (!getAllowedVoiceNames().includes(voice)) {
    throw new Error("当前选择的语音不在 GEMINI_ALLOWED_VOICES 允许列表中。");
  }

  if (!/^[A-Za-z][A-Za-z0-9_-]*$/.test(voice)) {
    throw new Error("GEMINI_VOICE 格式不正确。");
  }

  return {
    webSocketUrl: endpoint.toString(),
    model,
    voice,
    systemPrompt,
  };
}

function getSafeModelName() {
  const model = String(process.env.GEMINI_MODEL || DEFAULT_MODEL)
    .trim()
    .replace(/^models\//, "");

  if (!/^[A-Za-z0-9._-]+$/.test(model)) {
    throw new Error("GEMINI_MODEL 格式不正确。");
  }

  return model;
}

function getDefaultVoice() {
  const voice = String(process.env.GEMINI_VOICE || "Kore").trim();

  if (!/^[A-Za-z][A-Za-z0-9_-]*$/.test(voice)) {
    throw new Error("GEMINI_VOICE 格式不正确。");
  }

  return voice;
}

function getAllowedVoiceNames() {
  const raw = String(process.env.GEMINI_ALLOWED_VOICES || "").trim();

  const configuredVoices = raw
    ? raw
        .split(",")
        .map((voice) => voice.trim())
        .filter(Boolean)
    : VOICE_CATALOG.map((voice) => voice.name);

  const defaultVoice = getDefaultVoice();
  const names = [...configuredVoices, defaultVoice];

  const uniqueNames = [...new Set(names)];

  for (const voice of uniqueNames) {
    if (!/^[A-Za-z][A-Za-z0-9_-]*$/.test(voice)) {
      throw new Error(
        "GEMINI_ALLOWED_VOICES 中存在格式不正确的语音名称。",
      );
    }
  }

  return uniqueNames;
}

function getVoiceOptions() {
  return getAllowedVoiceNames().map((name) => ({
    name,
    label: voiceLabelMap.get(name) || name,
  }));
}

function createSetupMessage(config, clientMemory = "") {
  const generationConfig = {
    responseModalities: ["AUDIO"],
  };

  if (config.voice) {
    generationConfig.speechConfig = {
      languageCode: "en-US",
      voiceConfig: {
        prebuiltVoiceConfig: {
          voiceName: config.voice,
        },
      },
    };
  }

  let finalPrompt = config.systemPrompt;
  const cleanMemory = String(clientMemory || "").trim().slice(0, 1500);
  if (cleanMemory) {
    finalPrompt +=
      "\n\n[PAST MEMORY LOG (Recent conversation history with this child from previous calls — use this to remember their level, interests, and progress, and pick a fresh topic today)]:\n" +
      cleanMemory;
  }

  return {
    setup: {
      model: "models/" + config.model,
      generationConfig,
      systemInstruction: {
        parts: [
          {
            text: finalPrompt,
          },
        ],
      },
      inputAudioTranscription: {},
      outputAudioTranscription: {},
    },
  };
}

function requiredEnv(name) {
  const value =
    typeof process.env[name] === "string"
      ? process.env[name].trim()
      : "";

  if (!value) {
    throw new Error("缺少 Render 环境变量：" + name);
  }

  return value;
}

function getAccessPassword() {
  return typeof process.env.ACCESS_PASSWORD === "string"
    ? process.env.ACCESS_PASSWORD.trim()
    : "";
}

function requireHttpAuth(req, res, next) {
  if (isRequestAuthorized(req)) {
    next();
    return;
  }

  res.status(401).set({
    "WWW-Authenticate": 'Basic realm="Gemini Live Voice"',
    "content-type": "text/plain; charset=UTF-8",
  });

  res.send("Unauthorized");
}

function isRequestAuthorized(req) {
  const expectedPassword = getAccessPassword();

  if (!expectedPassword) {
    return true;
  }

  const url = getRequestUrl(req);
  const queryPassword = url.searchParams.get("pwd");

  if (queryPassword && secretEquals(queryPassword, expectedPassword)) {
    return true;
  }

  const cookiePassword = getCookie(req, "auth");

  if (cookiePassword && secretEquals(cookiePassword, expectedPassword)) {
    return true;
  }

  const basicPassword = getBasicPassword(req);

  if (basicPassword && secretEquals(basicPassword, expectedPassword)) {
    return true;
  }

  return false;
}

function queryPasswordMatches(req, expectedPassword) {
  const value = getRequestUrl(req).searchParams.get("pwd");
  return Boolean(value && secretEquals(value, expectedPassword));
}

function getCookie(req, name) {
  const header = String(req.headers.cookie || "");

  for (const item of header.split(";")) {
    const index = item.indexOf("=");

    if (index < 0) continue;

    const key = item.slice(0, index).trim();
    const value = item.slice(index + 1).trim();

    if (key !== name) continue;

    try {
      return decodeURIComponent(value);
    } catch {
      return "";
    }
  }

  return "";
}

function getBasicPassword(req) {
  const authHeader = String(req.headers.authorization || "");

  if (!authHeader.startsWith("Basic ")) {
    return "";
  }

  try {
    const decoded = Buffer.from(authHeader.slice(6), "base64").toString("utf8");
    const separator = decoded.indexOf(":");

    return separator >= 0 ? decoded.slice(separator + 1) : decoded;
  } catch {
    return "";
  }
}

function secretEquals(input, expected) {
  const left = Buffer.from(String(input));
  const right = Buffer.from(String(expected));

  return (
    left.length === right.length &&
    timingSafeEqual(left, right)
  );
}

function getRequestUrl(req) {
  return new URL(req.url || "/", getRequestOrigin(req));
}

function getRequestOrigin(req) {
  const forwardedProtocol = String(req.headers["x-forwarded-proto"] || "")
    .split(",")[0]
    .trim();

  const protocol =
    forwardedProtocol ||
    (req.socket.encrypted ? "https" : "http");

  const host = String(req.headers.host || "localhost");

  return protocol + "://" + host;
}

function isHttpsRequest(req) {
  return getRequestOrigin(req).startsWith("https://");
}

function isAllowedWebSocketOrigin(req) {
  const requestOrigin = String(req.headers.origin || "").replace(/\/$/, "");

  const allowedOrigin = String(
    process.env.ALLOWED_ORIGIN || getRequestOrigin(req),
  ).replace(/\/$/, "");

  return Boolean(requestOrigin && requestOrigin === allowedOrigin);
}

function rejectUpgrade(socket, statusCode, message, extraHeaders = {}) {
  const body = message + "\n";

  const statusText =
    statusCode === 401
      ? "Unauthorized"
      : statusCode === 403
        ? "Forbidden"
        : statusCode === 404
          ? "Not Found"
          : "Bad Request";

  const headers = {
    Connection: "close",
    "Content-Type": "text/plain; charset=UTF-8",
    "Content-Length": Buffer.byteLength(body),
    ...extraHeaders,
  };

  const rawHeaders = Object.entries(headers)
    .map(([name, value]) => name + ": " + value)
    .join("\r\n");

  socket.write(
    "HTTP/1.1 " +
      statusCode +
      " " +
      statusText +
      "\r\n" +
      rawHeaders +
      "\r\n\r\n" +
      body,
  );

  socket.destroy();
}

async function wsDataToText(data) {
  if (typeof data === "string") {
    return data;
  }

  if (Buffer.isBuffer(data)) {
    return data.toString("utf8");
  }

  if (data instanceof ArrayBuffer) {
    return new TextDecoder().decode(data);
  }

  if (ArrayBuffer.isView(data)) {
    return new TextDecoder().decode(
      data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength),
    );
  }

  return String(data);
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

function gracefulShutdown(signal) {
  console.log("[Render] 收到 " + signal + "，正在关闭 WebSocket 会话…");

  clearInterval(heartbeatTimer);

  for (const client of wss.clients) {
    try {
      if (client.readyState === WebSocket.OPEN) {
        client.send(
          JSON.stringify({
            type: "closed",
            code: 1012,
            message: "服务正在重启，请重新拨号。",
          }),
        );

        client.close(1012, "service restarting");
      } else {
        client.terminate();
      }
    } catch {
      client.terminate();
    }
  }

  server.close(() => {
    console.log("[Render] HTTP 服务已关闭。");
  });

  const forceExitTimer = setTimeout(() => {
    process.exit(0);
  }, 27_000);

  forceExitTimer.unref();
}

process.once("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.once("SIGINT", () => gracefulShutdown("SIGINT"));

const HTML = String.raw`<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
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

    html, body {
      height: 100%;
      margin: 0;
      padding: 0;
    }

    body {
      height: 100svh;
      height: 100dvh;
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
      width: min(100%, 540px);
      height: 100%;
      margin: 0 auto;
      padding: max(12px, env(safe-area-inset-top))
               18px
               max(16px, env(safe-area-inset-bottom));
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }

    .topbar {
      flex: 0 0 auto;
      display: flex;
      align-items: center;
      justify-content: space-between;
      min-height: 42px;
      gap: 10px;
    }

    .brand {
      display: grid;
      gap: 2px;
      text-align: left;
    }

    .brand strong {
      letter-spacing: .02em;
      font-size: 15px;
    }

    .brand span {
      color: var(--muted);
      font-size: 11px;
    }

    .voice-picker {
      display: flex;
      align-items: center;
      gap: 6px;
      background: rgba(22, 30, 51, .78);
      border: 1px solid var(--panel-border);
      border-radius: 12px;
      padding: 6px 10px;
      font-size: 12px;
      color: var(--muted);
    }

    .voice-picker select {
      background: transparent;
      border: 0;
      color: var(--text);
      font: inherit;
      font-weight: 600;
      outline: none;
      cursor: pointer;
      max-width: 145px;
      text-overflow: ellipsis;
    }

    .voice-picker select option {
      background: #12182b;
      color: var(--text);
    }

    .voice-picker select:disabled {
      opacity: .5;
      cursor: not-allowed;
    }

    .stage {
      flex: 1 1 auto;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      text-align: center;
      min-height: 0;
    }

    .orb-wrap {
      position: relative;
      width: 174px;
      height: 174px;
      display: grid;
      place-items: center;
      transition: transform .25s ease;
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
      margin-top: clamp(14px, 3vh, 34px);
      min-height: 44px;
    }

    .status-main {
      font-size: 21px;
      font-weight: 720;
      letter-spacing: .01em;
    }

    .status-sub {
      margin-top: 5px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.4;
    }

    .timer {
      margin-top: 6px;
      min-height: 19px;
      color: #d7e0ff;
      font-variant-numeric: tabular-nums;
      font-size: 14px;
      letter-spacing: .08em;
    }

    /* 折叠/展开字幕控制栏 */
    .caption-drawer {
      flex: 0 1 auto;
      display: flex;
      flex-direction: column;
      min-height: 0;
      margin-top: 8px;
    }

    .caption-toggle {
      align-self: center;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 14px;
      border-radius: 999px;
      background: rgba(28, 39, 66, .72);
      border: 1px solid var(--panel-border);
      color: var(--muted);
      font-size: 12px;
      font-weight: 600;
      transition: background .15s ease, color .15s ease;
    }

    .caption-toggle:hover {
      background: rgba(42, 57, 94, .85);
      color: var(--text);
    }

    .captions {
      height: 22svh;
      min-height: 110px;
      max-height: 220px;
      margin-top: 10px;
      padding: 13px 15px;
      overflow-y: auto;
      border: 1px solid var(--panel-border);
      border-radius: 18px;
      background: linear-gradient(145deg, rgba(20, 28, 49, .82), rgba(11, 16, 30, .58));
      scrollbar-width: thin;
    }

    .caption-empty {
      height: 100%;
      display: grid;
      place-items: center;
      color: #8490b2;
      text-align: center;
      font-size: 13px;
      line-height: 1.6;
    }

    .line {
      margin: 0 0 11px;
      line-height: 1.5;
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
      margin-top: 12px;
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
      min-height: 58px;
      border-radius: 20px;
      background: linear-gradient(135deg, #3ece8a, #23a970);
      box-shadow: 0 13px 30px rgba(31, 189, 120, .25);
      font-size: 17px;
      font-weight: 760;
      transition: transform .15s ease, filter .15s ease;
    }

    .call-btn:hover {
      filter: brightness(1.08);
      transform: translateY(-1px);
    }

    .round-btn {
      min-height: 54px;
      border-radius: 18px;
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
      margin: 10px 5px 0;
      color: #7f8aaa;
      text-align: center;
      font-size: 11px;
      line-height: 1.45;
    }

    [hidden] {
      display: none !important;
    }

    /* 当字幕展开且屏幕较矮时自动缩小光球，绝不挤压底部按钮 */
    @media (max-height: 760px) {
      body[data-captions="open"] .orb-wrap {
        transform: scale(.74);
        margin: -22px 0;
      }

      body[data-captions="open"] .status {
        margin-top: 8px;
      }

      .call-btn {
        min-height: 52px;
      }

      .round-btn {
        min-height: 48px;
      }
    }

    @media (max-height: 640px) {
      .orb-wrap {
        transform: scale(.72);
        margin: -24px 0;
      }

      .captions {
        height: 18svh;
        min-height: 90px;
      }

      .hint {
        display: none;
      }
    }

    @media (min-width: 768px) and (min-height: 820px) {
      .shell {
        width: min(100%, 600px);
        padding-top: 28px;
        padding-bottom: 28px;
      }

      .captions {
        max-height: 260px;
      }
    }
  </style>
</head>
<body data-phase="idle" data-speaking="false" data-captions="closed">
  <main class="shell">
    <header class="topbar">
      <div class="brand">
        <strong id="modelName">Gemini Live</strong>
        <span>端到端加密连接由代理转发</span>
      </div>

      <label class="voice-picker" for="voiceSelect">
        <span>音色</span>
        <select id="voiceSelect" aria-label="选择语音音色">
          <option value="Kore">Kore · 坚定</option>
        </select>
      </label>
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

    <section class="caption-drawer">
      <button id="captionToggle" class="caption-toggle" type="button" aria-expanded="false">
        💬 展开实时字幕
      </button>

      <div id="captions" class="captions" aria-label="实时字幕" hidden>
        <div id="captionEmpty" class="caption-empty">
          通话接通后，这里会显示你和 Gemini 的实时字幕。
        </div>
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
    const callButton = document.getElementById('callButton');
    const muteButton = document.getElementById('muteButton');
    const endButton = document.getElementById('endButton');
    const voiceSelect = document.getElementById('voiceSelect');
    const captionToggle = document.getElementById('captionToggle');

    const orb = document.getElementById('orb');
    const statusMain = document.getElementById('statusMain');
    const statusSub = document.getElementById('statusSub');
    const timerEl = document.getElementById('timer');
    const modelName = document.getElementById('modelName');

    const captions = document.getElementById('captions');
    const captionEmpty = document.getElementById('captionEmpty');

    // 字幕折叠与展开切换（默认收起）
    let captionsExpanded = false;
    captionToggle.addEventListener('click', function () {
      captionsExpanded = !captionsExpanded;
      captions.hidden = !captionsExpanded;
      document.body.dataset.captions = captionsExpanded ? 'open' : 'closed';
      captionToggle.setAttribute('aria-expanded', String(captionsExpanded));
      captionToggle.textContent = captionsExpanded ? '🔽 收起实时字幕' : '💬 展开实时字幕';
      if (captionsExpanded) {
        scrollCaptionsToBottom();
      }
    });

    // 从后端 /config 加载完整音色列表
    async function loadVoiceConfig() {
      try {
        const res = await fetch('/config');
        if (!res.ok) return;
        const data = await res.json();
        const voices = Array.isArray(data.voices) ? data.voices : [];
        const savedVoice = localStorage.getItem('gemini_live_voice') || data.defaultVoice || 'Kore';

        if (voices.length > 0) {
          voiceSelect.replaceChildren();
          for (const item of voices) {
            const opt = document.createElement('option');
            opt.value = item.name;
            opt.textContent = item.label || item.name;
            voiceSelect.appendChild(opt);
          }
        }

        voiceSelect.value = savedVoice;
        if (!voiceSelect.value && voiceSelect.options.length > 0) {
          voiceSelect.selectedIndex = 0;
        }
      } catch {}
    }

    voiceSelect.addEventListener('change', function () {
      localStorage.setItem('gemini_live_voice', voiceSelect.value);
    });

    void loadVoiceConfig();

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

    // 提取最近 16 条对话存入浏览器本地作为跨天记忆
    function saveConversationMemory() {
      try {
        const lines = captions.querySelectorAll('.line.me, .line.assistant');
        if (lines.length < 2) return;

        const recent = [];
        const startIdx = Math.max(0, lines.length - 12);
        for (let i = startIdx; i < lines.length; i++) {
          const role = lines[i].classList.contains('me') ? 'Child' : 'Tutor';
          const text = lines[i].querySelector('.text')?.textContent?.trim() || '';
          if (text) {
            recent.push(role + ': ' + text);
          }
        }

        if (recent.length > 0) {
          const prev = localStorage.getItem('gemini_child_memory') || '';
          const combined = (prev ? prev + '\n---\n' : '') + recent.join('\n');
          // 只保留最近 1200 个字符，防止 URL 过长，永远滚动更新最新记忆
          localStorage.setItem('gemini_child_memory', combined.slice(-1200));
        }
      } catch {}
    }

    function workerWebSocketUrl() {
      const scheme = location.protocol === 'https:' ? 'wss://' : 'ws://';
      const voice = encodeURIComponent(voiceSelect.value || 'Kore');
      const memory = encodeURIComponent(localStorage.getItem('gemini_child_memory') || '');
      return scheme + location.host + '/ws?voice=' + voice + '&memory=' + memory;
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
      voiceSelect.disabled = active;

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
      if (!captions.hidden) {
        captions.scrollTop = captions.scrollHeight;
      }
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

        audioContext = new AudioContextClass({
          latencyHint: 'interactive',
        });
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
       * 采集缓冲区进一步从 2048 降为 1024（约 21ms 一帧），大幅提升实时响应速度。
       */
      micProcessor = ctx.createScriptProcessor(1024, 1, 1);
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

        if (socket.bufferedAmount > 256 * 1024) {
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
      setStatus('正在拨号…', '正在连接服务器和 Gemini。');
      refreshButtons();

      try {
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
            '连接等待超过 20 秒。请检查 Render 环境变量或服务日志。';
          ws.close(1011, 'connection timeout');
        }
      }, 20_000);

      ws.addEventListener('open', function () {
        if (socket !== ws) return;

        setStatus(
          '正在接通…',
          '服务器已连接，正在创建 Gemini Live 会话。',
        );
      });

      ws.addEventListener('message', function (event) {
        if (socket !== ws) return;

        let message;

        try {
          message = JSON.parse(event.data);
        } catch {
          lastFailure = '服务器返回了无法解析的消息。';
          return;
        }

        handleWorkerMessage(message);
      });

      ws.addEventListener('error', function () {
        if (socket !== ws) return;

        lastFailure =
          '浏览器与服务器的 WebSocket 连接发生错误。';
      });

      ws.addEventListener('close', function (event) {
        saveConversationMemory();
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
      saveConversationMemory();
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
        addNote('已接通（音色：' + voiceSelect.value + '）。现在可以自然说话。');

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

        /*
         * 35ms 首包抗抖动微缓冲：
         * 当队列为空（首包到达）时预留 35ms 缓冲窗口，防止非热音色第 1 帧与第 2 帧之间断流；
         * 后续连续帧紧密无缝拼接。
         */
        const now = ctx.currentTime;
        if (nextPlayTime <= now) {
          nextPlayTime = now + 0.035;
        }
        const startAt = nextPlayTime;

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

const port = Number(process.env.PORT || 10000);

server.listen(port, "0.0.0.0", () => {
  console.log("[Render] Gemini Live 服务正在监听 0.0.0.0:" + port);
});
