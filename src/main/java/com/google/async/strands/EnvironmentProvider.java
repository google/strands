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

/**
 * Supplies the {@link Environment} that Strands should use by default.
 *
 * <p>Strands discovers providers via {@link java.util.ServiceLoader}; an {@link Environment}
 * published as a service will cause loading to fail with an error. Implementations must be public,
 * must have a public no-argument constructor, and must be declared in {@code
 * META-INF/services/com.google.async.strands.EnvironmentProvider} (most easily by annotating them
 * with {@code @AutoService(EnvironmentProvider.class)}).
 *
 * <p>Publishing a provider lets a library configure Strands for an entire application just by being
 * on its classpath, without call sites having to be aware of the configuration.
 *
 * <p>The default environment is resolved once per JVM, on first use, in this order of precedence:
 *
 * <ol>
 *   <li>the provider named by the {@code com.google.async.strands.defaultEnvironmentProvider}
 *       system property, which overrides {@link #priority} entirely, or the built-in fallback if
 *       that property is set to {@code none};
 *   <li>the discovered provider with the highest {@link #priority};
 *   <li>a built-in fallback to the default implementation of {@link Environment}.
 * </ol>
 *
 * <p>Only the winning provider's {@link #get} is called, and only once, so a provider is free to do
 * work that would be wasteful to do unconditionally.
 *
 * <p>Anything that leaves the choice ambiguous is an error: two providers claiming the same
 * priority, or a service declaration that fails to load.
 */
@ThreadSafe
public interface EnvironmentProvider {

  /**
   * The role a provider fills, which determines which provider wins when several are discovered on
   * the same classpath.
   *
   * <p>Constants are declared from lowest precedence to highest, and that declaration order is the
   * precedence order.
   *
   * <p>Choose the constant that describes who you are, not how strongly you feel: each priority may
   * be claimed by at most one provider, and two providers claiming the same one is an error.
   */
  enum Priority {
    /**
     * An ambient integration with the runtime the code happens to be executing on, published by a
     * platform or base library and not requested by the application.
     */
    PLATFORM,

    /**
     * A server or application framework configuring Strands on behalf of the applications it hosts.
     */
    FRAMEWORK,

    /**
     * The application itself, which gets the final say over the frameworks and platforms it uses.
     */
    APPLICATION;
  }

  /**
   * Returns the role this provider fills.
   *
   * <p>See {@link Priority} for how to choose, and note that at most one provider may claim any
   * given priority.
   */
  Priority priority();

  /**
   * Returns the {@link Environment} to install as the default.
   *
   * <p>Called at most once per JVM, and only on the winning provider.
   */
  Environment get();
}
