import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { saveAudioResource, transcribeAudioLocal } from "../.pi/extensions/feishu/attachments.ts";
import { parseMessageInput } from "../.pi/extensions/feishu/messages.ts";

test("audio message parses to kind=audio attachment with source=audio", () => {
  const parsed = parseMessageInput({
    messageId: "om_test_audio_1",
    chatId: "oc_test",
    chatMode: "p2p",
    msgType: "audio",
    senderOpenId: "ou_test",
    content: JSON.stringify({
      file_key: "file_v3_0014k_abc123-def456",
      duration: 8000,
    }),
  });
  assert.equal(parsed.source, "audio");
  assert.equal(parsed.text, "");
  assert.equal(parsed.attachments.length, 1);
  assert.deepEqual(parsed.attachments[0], { kind: "audio", fileKey: "file_v3_0014k_abc123-def456" });
});

test("audio message without file_key yields no attachment (graceful)", () => {
  const parsed = parseMessageInput({
    messageId: "om_test_audio_2",
    chatId: "oc_test",
    chatMode: "p2p",
    msgType: "audio",
    senderOpenId: "ou_test",
    content: JSON.stringify({ duration: 8000 }),
  });
  assert.equal(parsed.attachments.length, 0);
});

test("saveAudioResource writes ogg bytes to media dir and returns path", () => {
  const dir = mkdtempSync(join(tmpdir(), "pi-audio-test-"));
  try {
    const saved = saveAudioResource("om_x100b672fa556bca8de37bb6b8f146bd", "file_v3_0014k_f2a0c9d3-5373-4061-b4ce-bd23ec637f1g", Buffer.from([0x4f, 0x67, 0x67, 0x53]));
    assert.ok(saved.path.endsWith(".ogg"));
    assert.ok(saved.path.includes("om_x100b672fa556bca8de37bb6b8f146bd"));
    assert.equal(readFileSync(saved.path).length, 4);
    assert.equal(saved.size, 4);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("transcribeAudioLocal resolves null when helper is missing or fails (no crash)", async () => {
  const original = process.env.PI_LOCAL_PERCEPTION_BIN;
  process.env.PI_LOCAL_PERCEPTION_BIN = "/nonexistent/pi-local-perception";
  try {
    const result = await transcribeAudioLocal("/nonexistent/file.ogg", 3_000);
    assert.equal(result, null);
  } finally {
    if (original === undefined) delete process.env.PI_LOCAL_PERCEPTION_BIN;
    else process.env.PI_LOCAL_PERCEPTION_BIN = original;
  }
});
