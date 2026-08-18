export type PromptWatchOptions = {
  /** After this many ms, fire onStillRunning() once while the prompt is still pending. 0 disables. */
  notifyMs: number;
  /**
   * After this many ms of *inactivity*, the awaited prompt rejects with an
   * Error carrying hardTimeoutMessage. 0 disables.
   *
   * When `isActive` is provided this is an IDLE timeout: the clock resets on
   * every activity probe that returns true, so a long-running task that keeps
   * producing output / tool calls is never killed just because it takes long.
   * When `isActive` is omitted it behaves as a fixed wall-clock timeout.
   */
  hardMs: number;
  /** Error message used when the hard timeout fires. */
  hardTimeoutMessage: string;
  /** Fired once when notifyMs elapses and the prompt is still pending. Must never fail the prompt. */
  onStillRunning?: () => void;
  /** Called when the hard timeout fires; use it to abort the underlying run so it is not left busy. */
  onHardTimeout?: () => Promise<void> | void;
  /**
   * Optional activity probe. When provided, the hard timeout is treated as an
   * idle timeout: it only fires after hardMs of continuous silence (probe
   * returning false). Long tasks with steady output are never killed.
   */
  isActive?: () => boolean;
};

/**
 * Awaits a prompt promise with an optional "still working" notice threshold and
 * an optional hard timeout.
 *
 * The notify threshold NEVER fails the prompt — it only observes, so long-running
 * tasks are never reported as failed just because they take a while. Only the
 * hard timeout (opt-in) rejects, and the caller is expected to abort the
 * underlying run via onHardTimeout so the session does not keep running in the
 * background (which would otherwise leave it busy for follow-up messages).
 */
export async function waitForPrompt(prompt: Promise<unknown>, options: PromptWatchOptions): Promise<void> {
  const { notifyMs, hardMs, hardTimeoutMessage, onStillRunning, onHardTimeout, isActive } = options;

  if (notifyMs <= 0 && hardMs <= 0) {
    await prompt;
    return;
  }

  let notifyTimer: NodeJS.Timeout | undefined;
  let hardTimer: NodeJS.Timeout | undefined;
  let hardReject: ((error: Error) => void) | undefined;

  const fireHardTimeout = () => {
    const error = new Error(hardTimeoutMessage);
    hardReject?.(error);
    void Promise.resolve(onHardTimeout?.()).catch(() => undefined);
  };

  if (notifyMs > 0) {
    notifyTimer = setTimeout(() => {
      onStillRunning?.();
    }, notifyMs);
    notifyTimer.unref?.();
  }

  if (hardMs > 0) {
    if (isActive) {
      // Idle-style hard timeout: poll periodically, fire only after hardMs of
      // continuous silence (probe returning false). Any activity resets the clock.
      const POLL_MS = 5000;
      let silentSince = Date.now();
      hardTimer = setInterval(() => {
        let active = false;
        try {
          active = Boolean(isActive());
        } catch {}
        if (active) {
          silentSince = Date.now();
        } else if (Date.now() - silentSince >= hardMs) {
          if (hardTimer) clearInterval(hardTimer);
          fireHardTimeout();
        }
      }, POLL_MS);
      hardTimer.unref?.();
    } else {
      hardTimer = setTimeout(() => {
        fireHardTimeout();
      }, hardMs);
      hardTimer.unref?.();
    }
  }

  try {
    if (hardMs > 0) {
      await Promise.race([
        prompt,
        new Promise<never>((_, reject) => {
          hardReject = reject;
        }),
      ]);
    } else {
      await prompt;
    }
  } finally {
    if (notifyTimer) clearTimeout(notifyTimer);
    if (hardTimer) clearInterval(hardTimer);
  }
}
