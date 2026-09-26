package com.salesforce.multicloudj.pubsub.ali;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * TEMPORARY diagnostic watchdog for the pubsub-ali conformance ITs (revert once the CI hang is
 * captured). A single daemon thread watches a per-phase deadline; when the current phase overruns
 * its budget it prints a full thread dump of every live thread to {@code System.out}, which the
 * failsafe fork forwards to the CI console log (readable via {@code gh run view --log}). It then
 * re-dumps on a fixed interval so a sustained hang is unmistakable, and it is armed per test method
 * so the dump names the stuck method. It fires well before GitHub's 30-minute step kill.
 *
 * <p>Scoped to pubsub-ali test sources only: it never touches the shared {@code AbstractPubsubIT},
 * so it has no effect on the AWS or GCP pubsub suites.
 */
final class DiagnosticWatchdog {

  private static final long DUMP_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(60);
  private static final long POLL_MILLIS = 1000L;

  private static volatile String phase = "not-armed";
  private static volatile long deadlineNanos = Long.MAX_VALUE;
  private static boolean started;

  private DiagnosticWatchdog() {}

  /** Starts the daemon watchdog thread once; later calls are no-ops. */
  static synchronized void start() {
    if (started) {
      return;
    }
    started = true;
    Thread thread = new Thread(DiagnosticWatchdog::loop, "pubsub-ali-diagnostic-watchdog");
    thread.setDaemon(true);
    thread.start();
  }

  /** (Re)arms the watchdog: records the current phase and its budget in milliseconds. */
  static void arm(String newPhase, long timeoutMillis) {
    phase = newPhase;
    deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
  }

  private static void loop() {
    long lastDumpNanos = 0L;
    while (true) {
      try {
        Thread.sleep(POLL_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      long now = System.nanoTime();
      if (now > deadlineNanos
          && (lastDumpNanos == 0L || now - lastDumpNanos > DUMP_INTERVAL_NANOS)) {
        lastDumpNanos = now;
        dump(phase);
      }
    }
  }

  private static void dump(String currentPhase) {
    StringBuilder sb = new StringBuilder(4096);
    sb.append(System.lineSeparator())
        .append("==== [PUBSUB-ALI WATCHDOG] phase='")
        .append(currentPhase)
        .append("' overran its budget at ")
        .append(Instant.now())
        .append("; full thread dump follows ====")
        .append(System.lineSeparator());
    for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
      Thread thread = entry.getKey();
      sb.append('"')
          .append(thread.getName())
          .append("\" #")
          .append(thread.getId())
          .append(" state=")
          .append(thread.getState())
          .append(" daemon=")
          .append(thread.isDaemon())
          .append(System.lineSeparator());
      for (StackTraceElement frame : entry.getValue()) {
        sb.append("\tat ").append(frame).append(System.lineSeparator());
      }
      sb.append(System.lineSeparator());
    }
    sb.append("==== [PUBSUB-ALI WATCHDOG] end thread dump ====").append(System.lineSeparator());
    System.out.println(sb);
    System.out.flush();
  }
}
