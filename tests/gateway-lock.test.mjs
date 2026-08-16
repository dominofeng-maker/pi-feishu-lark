import { test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, rmSync, existsSync, readFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const lockDir = mkdtempSync(join(tmpdir(), "pifl-lock-"));
process.env.PI_FEISHU_LOCK_PATH = join(lockDir, "locks.json");

const { acquireGatewayLock, readGatewayOwner } = await import("../.pi/extensions/feishu/gateway-lock.js");

const LOCK_KEY = "pi-feishu-lark.feishu-gateway";
const LOCKS_FILE = join(lockDir, "locks.json");

function writeOwner(overrides) {
  const owner = {
    key: LOCK_KEY,
    pid: process.pid,
    token: "test-token",
    cwd: "/tmp",
    startedAt: new Date().toISOString(),
    heartbeatAt: new Date().toISOString(),
    status: "connected",
    ...overrides,
  };
  writeFileSync(LOCKS_FILE, JSON.stringify({ [LOCK_KEY]: owner }));
}

function readLocks() {
  if (!existsSync(LOCKS_FILE)) return {};
  return JSON.parse(readFileSync(LOCKS_FILE, "utf8"));
}

const DEAD_PID = 999_999_999; // 几乎不可能存在的 pid
const now = Date.now();
const iso = (msAgo) => new Date(now - msAgo).toISOString();

beforeEach(() => {
  mkdirSync(lockDir, { recursive: true });
  try { rmSync(LOCKS_FILE, { force: true }); } catch {}
  try { rmSync(`${LOCKS_FILE}.lock`, { recursive: true, force: true }); } catch {}
});

afterEach(() => {
  try { rmSync(lockDir, { recursive: true, force: true }); } catch {}
});

test("无锁时 acquire 成功", async () => {
  const result = await acquireGatewayLock("/tmp");
  assert.equal(result.status, "acquired");
  await result.handle.release();
});

test("活跃锁（进程活 + 心跳新鲜）→ busy", async () => {
  writeOwner({ heartbeatAt: iso(5_000) });
  const result = await acquireGatewayLock("/tmp");
  assert.equal(result.status, "busy");
  assert.equal(result.owner.pid, process.pid);
});

test("死进程锁 → 接管（acquired）", async () => {
  writeOwner({ pid: DEAD_PID, heartbeatAt: iso(1_000) });
  const result = await acquireGatewayLock("/tmp");
  assert.equal(result.status, "acquired");
  await result.handle.release();
});

test("进程活 + 心跳软过期（>90s 且 <300s）→ busy 不接管（核心修复）", async () => {
  writeOwner({ heartbeatAt: iso(120_000) });
  const result = await acquireGatewayLock("/tmp");
  assert.equal(result.status, "busy");
  assert.equal(result.owner.pid, process.pid);
});

test("进程活 + 心跳硬过期（>300s）→ 接管（核心修复）", async () => {
  writeOwner({ heartbeatAt: iso(301_000) });
  const result = await acquireGatewayLock("/tmp");
  assert.equal(result.status, "acquired");
  await result.handle.release();
});

test("force=true 时无条件接管", async () => {
  writeOwner({ heartbeatAt: iso(5_000) });
  const result = await acquireGatewayLock("/tmp", true);
  assert.equal(result.status, "acquired");
  await result.handle.release();
});

test("readGatewayOwner：进程活 + 心跳过期 → 返回 owner（核心修复）", () => {
  writeOwner({ heartbeatAt: iso(120_000) });
  const owner = readGatewayOwner();
  assert.ok(owner, "应返回存活网关");
  assert.equal(owner.pid, process.pid);
});

test("readGatewayOwner：进程活 + 心跳新鲜 → 返回 owner", () => {
  writeOwner({ heartbeatAt: iso(5_000) });
  assert.ok(readGatewayOwner());
});

test("readGatewayOwner：死进程 → undefined", () => {
  writeOwner({ pid: DEAD_PID, heartbeatAt: iso(5_000) });
  assert.equal(readGatewayOwner(), undefined);
});

test("release 后锁清除，readGatewayOwner 为 undefined", async () => {
  const result = await acquireGatewayLock("/tmp");
  assert.equal(result.status, "acquired");
  await result.handle.release();
  assert.equal(readGatewayOwner(), undefined);
  assert.deepEqual(Object.keys(readLocks()), []);
});
