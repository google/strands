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

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/**
 * An exception that is thrown when Strands encounters some unexpected internal state.
 *
 * <p>If you see this exception, you've likely found a bug in Strands! Please file a bug report at
 * https://github.com/google/strands/issues/new with the details of how you encountered this
 * exception and reproduction steps if possible.
 */
public final class StrandsInternalStateException extends IllegalStateException {

  private static final String URL = "https://github.com/google/strands/issues/new";

  @FormatMethod
  StrandsInternalStateException(@FormatString String message, Object... args) {
    String fmt =
        String.format(
            "Strands encountered an unexpected internal state (please file an issue at %s): %s",
            URL, message);
    super(String.format(fmt, args));
  }
}
