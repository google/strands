/*
 * Copyright 2026 The Strands Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.async.strands;

import com.google.async.strands.Strand.State;
import com.google.errorprone.annotations.ThreadSafe;

/**
 * A listener consumes events during the course of Strands execution.
 *
 * <p>Listeners are <i>passive</i> - they cannot affect execution by throwing exceptions. Any
 * exception thrown will be caught and suppressed.
 *
 * <p>Listeners are <i>lightweight</i> - they should avoid blocking or doing too much work.
 */
@ThreadSafe
@SuppressWarnings("GoodTime") // This is a performance-sensitive API, so we use primitives.
public interface EventListener {
  /**
   * Sentinel value representing an unmeasured/untimed duration (e.g. when timing is disabled or
   * unsampled).
   */
  public static final long UNTIMED_NANOS = -1L;

  /**
   * Event that fires when a Stands scope has opened and is running its task.
   *
   * <p>Any thrown exceptions will be caught and may be logged and will not impact execution.
   */
  void scopeOpen();

  /**
   * Event that fires when a Strands scope has closed when it has returned a result or thrown an
   * exception.
   *
   * <p>Any thrown exceptions will be caught and may be logged and will not impact execution.
   *
   * @param runtimeNanos the number of nanoseconds between this scope being opened and closed
   */
  void scopeClose(long runtimeNanos);

  /**
   * Event that fires when a Strand is first created, but is has not yet been allocated a virtual
   * thread or started executing.
   *
   * <p>Any thrown exceptions will be caught and may be logged and will not impact execution.
   */
  void strandCreate();

  /**
   * Event that fires when a Strand is ready to start running, but is waiting for its virtual thread
   * to be mounted to a platform thread.
   *
   * <p>Any thrown exceptions will be caught and may be logged and will not impact execution.
   *
   * @param delayNanos the number of nanoseconds between the Strand being created and starting to
   *     run.
   */
  void strandReady(long delayNanos);

  /**
   * Event that fires when a Strand has started running, i.e. its virtual thread has been mounted to
   * a platform thread for the first time.
   *
   * <p>Any thrown exceptions will be caught and may be logged and will not impact execution.
   *
   * @param delayNanos the number of nanoseconds between the Strand becoming ready and starting to
   *     run
   */
  void strandStart(long delayNanos);

  /**
   * Event that fires when a Strand has completed, i.e. its virtual thread has been terminated and a
   * result or exception is available.
   *
   * <p>Any thrown exceptions will be caught and may be logged and will not impact execution.
   *
   * @param runtimeNanos the number of nanoseconds between the Strand starting to run and completing
   * @param state the {@link State} the Strand completed with
   */
  void strandComplete(long runtimeNanos, State state);

  /** An {@link EventListener} that does nothing. */
  static final EventListener EMPTY =
      new EventListener() {
        @Override
        public void scopeOpen() {}

        @Override
        public void scopeClose(long runtimeNanos) {}

        @Override
        public void strandCreate() {}

        @Override
        public void strandReady(long delayNanos) {}

        @Override
        public void strandStart(long delayNanos) {}

        @Override
        public void strandComplete(long runtimeNanos, State state) {}
      };

  /**
   * Returns an {@link EventListener} that forwards events it receives to multiple other listeners
   * (in order).
   */
  static EventListener multiplex(EventListener... listeners) {
    if (listeners.length == 0) {
      return EMPTY;
    }
    if (listeners.length == 1) {
      // Avoid the overhead of this if there is only one listener.
      return listeners[0];
    }
    return new EventListener() {
      @Override
      public void scopeOpen() {
        for (EventListener listener : listeners) {
          listener.scopeOpen();
        }
      }

      @Override
      public void scopeClose(long runtimeNanos) {
        for (EventListener listener : listeners) {
          listener.scopeClose(runtimeNanos);
        }
      }

      @Override
      public void strandCreate() {
        for (EventListener listener : listeners) {
          listener.strandCreate();
        }
      }

      @Override
      public void strandReady(long delayNanos) {
        for (EventListener listener : listeners) {
          listener.strandReady(delayNanos);
        }
      }

      @Override
      public void strandStart(long delayNanos) {
        for (EventListener listener : listeners) {
          listener.strandStart(delayNanos);
        }
      }

      @Override
      public void strandComplete(long runtimeNanos, State state) {
        for (EventListener listener : listeners) {
          listener.strandComplete(runtimeNanos, state);
        }
      }
    };
  }
}
