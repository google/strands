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

import com.google.errorprone.annotations.ThreadSafe;

/** Defines the timing mode used for measuring duration metrics in Strands event listener events. */
@ThreadSafe
public enum TimingMode {
  /**
   * Timing is disabled. No {@link System#nanoTime()} calls are executed and {@link
   * EventListener#UNTIMED_NANOS} (-1L) is passed for duration parameters.
   */
  OFF,

  /**
   * Samples duration timing at the root scope level for approximately 1 in 128 root scopes. When a
   * scope is sampled, all strands and sub-scopes within that tree are timed; unsampled scopes pass
   * {@link EventListener#UNTIMED_NANOS} (-1L).
   */
  SAMPLE,

  /** Measures duration timing with {@link System#nanoTime()} for 100% of strands. */
  FULL;

  /**
   * Returns {@link TimingMode#FULL} or {@link TimingMode#OFF} based on the given key.
   *
   * <p>If the timing mode is {@link TimingMode#SAMPLE}, then the key is used to probabilistically
   * select whether to return {@link TimingMode#FULL} or {@link TimingMode#OFF}. Consequently, it is
   * important that the key is a new object for each check, otherwise the selection logic will be
   * biased.
   */
  final TimingMode effectiveFor(Object key) {
    return switch (this) {
      case OFF -> TimingMode.OFF;
      case FULL -> TimingMode.FULL;
      // Note: We don't unconditionally take the hash code of the key because this incurs an initial
      // computation cost, so we only compute it if we actually need to.
      case SAMPLE ->
          (System.identityHashCode(key) & SAMPLE_MASK) == 0 ? TimingMode.FULL : TimingMode.OFF;
    };
  }

  /**
   * Mask to use for {@link #SAMPLE} timing mode.
   *
   * <p>When used as a bitmask with a uniformly distributed hash code, (i.e. from {@link
   * System#identityHashCode(Object)}), this keeps the least significant 7 bits which has 128
   * possible values. Consequently, there is a approximately 1 in 128 chance that the masked value
   * will be zero.
   *
   * <p>This is used for a very fast method of selecting a probabilistic subset of scopes to time.
   */
  private static final int SAMPLE_MASK = 0b0111_1111;
}
