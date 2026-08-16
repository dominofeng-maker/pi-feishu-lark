import { execFile } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import path from "node:path";

export type FeishuImageInput = { type: "image"; data: string; mimeType: string };

/** 飞书语音消息落盘目录（Ogg Opus 16kHz，飞书语音消息的原始格式） */
export const AUDIO_MEDIA_DIR = path.join(homedir(), ".pi", "agent", "media", "audio");

const SUPPORTED_IMAGE_MIME = new Set(["image/png", "image/jpeg", "image/webp"]);
const SUPPORTED_TEXT_EXT = new Set([
  ".txt", ".md", ".csv", ".json", ".log",
  ".js", ".mjs", ".cjs", ".ts", ".tsx", ".jsx",
  ".py", ".java", ".go", ".rs", ".rb", ".php", ".swift",
  ".kt", ".kts", ".scala", ".sh", ".bash", ".zsh",
  ".sql", ".yaml", ".yml", ".xml", ".html", ".css", ".scss",
  ".less", ".vue", ".svelte", ".toml", ".ini", ".conf", ".env",
  ".c", ".cc", ".cpp", ".h", ".hpp", ".cs",
]);
const SUPPORTED_TEXT_BASENAME = new Set(["dockerfile", "makefile", ".gitignore"]);
const MAX_TEXT_FILE_BYTES = 800_000;
const MAX_TEXT_FILE_CHARS = 16_000;

export function isSupportedImageMime(mimeType: string | undefined) {
  return Boolean(mimeType && SUPPORTED_IMAGE_MIME.has(mimeType));
}

export function isSupportedTextFile(fileName: string) {
  const lower = fileName.toLowerCase();
  if (SUPPORTED_TEXT_BASENAME.has(lower)) return true;
  const idx = lower.lastIndexOf(".");
  if (idx < 0) return false;
  return SUPPORTED_TEXT_EXT.has(lower.slice(idx));
}

export function decodeTextFile(fileName: string, bytes: Buffer): { ok: true; text: string; truncated: boolean } | { ok: false } {
  const slice = bytes.length > MAX_TEXT_FILE_BYTES ? bytes.subarray(0, MAX_TEXT_FILE_BYTES) : bytes;
  if (looksBinary(slice)) return { ok: false };
  const text = slice.toString("utf8");
  const truncated = text.length > MAX_TEXT_FILE_CHARS || bytes.length > MAX_TEXT_FILE_BYTES;
  return { ok: true, text: text.slice(0, MAX_TEXT_FILE_CHARS), truncated };
}

export function detectImageMime(bytes: Buffer, headerMime?: string) {
  if (headerMime && SUPPORTED_IMAGE_MIME.has(headerMime)) return headerMime;
  if (bytes.length >= 12) {
    if (bytes[0] === 0x89 && bytes[1] === 0x50 && bytes[2] === 0x4e && bytes[3] === 0x47) return "image/png";
    if (bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) return "image/jpeg";
    if (
      bytes[0] === 0x52 && bytes[1] === 0x49 && bytes[2] === 0x46 && bytes[3] === 0x46 &&
      bytes[8] === 0x57 && bytes[9] === 0x45 && bytes[10] === 0x42 && bytes[11] === 0x50
    ) return "image/webp";
  }
  return undefined;
}

export function detectCodeLanguage(fileName: string) {
  const lower = fileName.toLowerCase();
  const ext = lower.includes(".") ? lower.slice(lower.lastIndexOf(".")) : "";
  const mapping: Record<string, string> = {
    ".js": "javascript", ".mjs": "javascript", ".cjs": "javascript",
    ".ts": "typescript", ".tsx": "tsx", ".jsx": "jsx",
    ".py": "python", ".java": "java", ".go": "go", ".rs": "rust", ".rb": "ruby",
    ".php": "php", ".swift": "swift", ".kt": "kotlin", ".kts": "kotlin",
    ".scala": "scala", ".sh": "bash", ".bash": "bash", ".zsh": "bash",
    ".sql": "sql", ".yaml": "yaml", ".yml": "yaml", ".xml": "xml",
    ".html": "html", ".css": "css", ".scss": "scss", ".less": "less",
    ".vue": "vue", ".svelte": "svelte", ".toml": "toml", ".ini": "ini",
    ".conf": "conf", ".env": "bash", ".c": "c", ".cc": "cpp", ".cpp": "cpp",
    ".h": "c", ".hpp": "cpp", ".cs": "csharp", ".md": "markdown",
    ".json": "json", ".csv": "csv", ".txt": "text", ".log": "text",
  };
  if (lower === "dockerfile") return "dockerfile";
  if (lower === "makefile") return "makefile";
  return mapping[ext] || "text";
}

function looksBinary(bytes: Buffer) {
  if (!bytes.length) return false;
  let controlCount = 0;
  const sampleLen = Math.min(bytes.length, 4096);
  for (let i = 0; i < sampleLen; i += 1) {
    const ch = bytes[i]!;
    if (ch === 0) return true;
    if (ch < 8 || (ch > 13 && ch < 32)) controlCount += 1;
  }
  return controlCount / sampleLen > 0.08;
}

/**
 * 将飞书语音资源保存到本地（Ogg Opus），返回落盘路径。
 * 文件扩展名固定为 .ogg（飞书语音消息格式为 Ogg Opus 16kHz mono）。
 */
export function saveAudioResource(messageId: string, fileKey: string, bytes: Buffer): { path: string; size: number } {
  mkdirSync(AUDIO_MEDIA_DIR, { recursive: true });
  const safeId = fileKey.replace(/[^a-zA-Z0-9_-]/g, "_");
  const filePath = path.join(AUDIO_MEDIA_DIR, `${messageId}-${safeId}.ogg`);
  writeFileSync(filePath, bytes);
  return { path: filePath, size: bytes.length };
}

/**
 * 调用本地 FunASR（pi-local-perception stt）转写音频，返回转写文本。
 * 任何失败（服务未启动/超时/解析错误）都返回 null，由调用方降级处理。
 */
export function transcribeAudioLocal(filePath: string, timeoutMs = 25_000): Promise<string | null> {
  return new Promise((resolve) => {
    const candidates = [
      process.env.PI_LOCAL_PERCEPTION_BIN,
      path.join(homedir(), ".local", "bin", "pi-local-perception"),
      "pi-local-perception",
    ].filter((p): p is string => Boolean(p));
    let helper: string | undefined;
    for (const candidate of candidates) {
      if (candidate === "pi-local-perception" || candidate.includes("/")) {
        helper = candidate;
        break;
      }
    }
    if (!helper) {
      resolve(null);
      return;
    }
    execFile(
      helper,
      ["stt", filePath],
      { timeout: timeoutMs, maxBuffer: 4 * 1024 * 1024 },
      (error, stdout) => {
        if (error) {
          resolve(null);
          return;
        }
        try {
          const parsed = JSON.parse(stdout) as { ok?: boolean; text?: string };
          if (parsed?.ok && typeof parsed.text === "string" && parsed.text.trim()) {
            resolve(parsed.text.trim());
            return;
          }
        } catch {
          // fallthrough
        }
        resolve(null);
      },
    );
  });
}
