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

import org.jspecify.annotations.Nullable;

/**
 * A functional interface similar to {@link java.util.function.Function} that possibly throws a
 * specific (possibly checked) exception.
 */
@FunctionalInterface
public interface Transform<
    I extends @Nullable Object, O extends @Nullable Object, X extends Throwable> {
  /**
   * Returns the result of applying this transform to the given input.
   *
   * @param input the input to apply the transform to
   * @throws X if the transform fails
   */
  O apply(I input) throws X;
}
