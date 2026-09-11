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
import org.jspecify.annotations.Nullable;

/**
 * A closure of configuration provided by an {@link Environment} provided to {@link Scope} instances
 * and the {@link Strand} instances that it creates.
 */
@ThreadSafe
final record ExecutionContext(
    EventListener listener,
    TimingMode timingMode,
    VirtualThreadFactory taskThreadFactory,
    VirtualThreadFactory frameworkThreadFactory,
    @Nullable ContextPropagationOperator contextPropagationOperator) {}
