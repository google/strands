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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import com.google.async.strands.EnvironmentProvider.Priority;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import org.jspecify.annotations.Nullable;

/**
 * Resolves and holds the default runtime {@link Environment} for Strands.
 *
 * <p>The default is resolved at most once per JVM, on first use, from the highest-{@linkplain
 * EnvironmentProvider#priority priority} {@link EnvironmentProvider} published to {@link
 * ServiceLoader}, or {@link #FALLBACK} if there are none.
 *
 * <p>Anything that leaves that choice ambiguous or unreliable is an error. Silently degrading to
 * the fallback would drop whatever the intended environment supplies - context propagation, in
 * particular, which can carry credentials, deadlines, and tracing - and it would do so in a way
 * that looks like it worked. {@link #PREFERRED_PROVIDER_PROPERTY} can pick a winner out of an
 * ambiguous set, but nothing overrules a classpath that could not be read.
 */
final class DefaultEnvironment {

  /**
   * System property selecting the {@link EnvironmentProvider} to use, by fully qualified class name
   * or the special value {@link #FALLBACK_PROVIDER_NAME}. The named provider must still be
   * published to {@link ServiceLoader}, except when {@link #FALLBACK_PROVIDER_NAME} is used.
   *
   * <p>If set, this names the provider to use directly and overrides {@link
   * EnvironmentProvider#priority priority} selection and collision detection.
   *
   * <p>Read once, when the default environment is first resolved. Setting it after that has no
   * effect.
   */
  private static final String PREFERRED_PROVIDER_PROPERTY =
      "com.google.async.strands.defaultEnvironmentProvider";

  /** The value of {@link #PREFERRED_PROVIDER_PROPERTY} that selects {@link #FALLBACK}. */
  private static final String FALLBACK_PROVIDER_NAME = "none";

  /**
   * The environment used when no provider is published.
   *
   * <p>Uses only default JDK capabilities and requires no external dependencies. This is not itself
   * published as a service; it is the result of finding nothing, or of asking for {@link
   * #FALLBACK_PROVIDER_NAME}.
   */
  private static final Environment FALLBACK = new Environment() {};

  /**
   * Orders discovered providers by descending priority.
   *
   * <p>Class name is used as a tiebreak purely so that the providers reported in a collision are
   * listed in a stable order; it is not a way to resolve the collision.
   */
  private static final Comparator<EnvironmentProvider> BY_PRECEDENCE =
      Comparator.comparing(EnvironmentProvider::priority)
          .reversed()
          .thenComparing(provider -> provider.getClass().getName());

  /**
   * What a {@link ServiceLoader} sweep turned up.
   *
   * <p>A failure is carried alongside the providers that did load rather than thrown here so that
   * {@link #resolve} remains the single, testable place where the policy lives, and so that the
   * diagnostic can say which providers survived.
   *
   * @param providers every provider that loaded successfully
   * @param failure why the sweep stopped early, or null if it completed
   */
  private record Discovery(
      List<EnvironmentProvider> providers, @Nullable ServiceConfigurationError failure) {}

  /**
   * Defers resolution until the first call to {@link #get}.
   *
   * <p>Class initialization gives the JVM's own locking, so resolution happens exactly once even
   * under concurrent first use, and every subsequent read is a plain field access.
   */
  private static final class Holder {
    static final @Nullable Environment INSTANCE;
    static final @Nullable RuntimeException FAILURE;

    static {
      Environment instance = null;
      RuntimeException failure = null;
      try {
        checkForMisdeclaredEnvironments();
        Discovery discovery = discover();
        instance =
            resolve(
                discovery.providers(),
                discovery.failure(),
                System.getProperty(PREFERRED_PROVIDER_PROPERTY));
      } catch (RuntimeException e) {
        failure = e;
      }
      INSTANCE = instance;
      FAILURE = failure;
    }

    private Holder() {}
  }

  /**
   * Returns the default {@link Environment}, resolving it on first use.
   *
   * <p>The same failure is reported on every call, since the classpath cannot change to fix it.
   *
   * @throws IllegalStateException if the classpath does not unambiguously determine a single
   *     environment
   */
  static Environment get() {
    RuntimeException failure = Holder.FAILURE;
    if (failure != null) {
      throw new IllegalStateException(
          "Unable to resolve the default Strands Environment.", failure);
    }
    return requireNonNull(Holder.INSTANCE);
  }

  /**
   * Selects the environment to use from the providers discovered on the classpath.
   *
   * @param discovered the providers that loaded successfully
   * @param discoveryFailure why discovery stopped early, or null if it completed
   * @param preferred the value of {@link #PREFERRED_PROVIDER_PROPERTY}, or null if unset
   * @throws IllegalStateException if {@code discoveryFailure} is non-null, if {@code preferred}
   *     names a provider that is not among {@code discovered}, or if {@code preferred} is unset and
   *     several providers claim the same priority
   */
  static Environment resolve(
      List<EnvironmentProvider> discovered,
      @Nullable ServiceConfigurationError discoveryFailure,
      @Nullable String preferred) {
    List<EnvironmentProvider> candidates = new ArrayList<>(discovered);
    candidates.sort(BY_PRECEDENCE);

    if (discoveryFailure != null) {
      throw new IllegalStateException(
          String.format(
              "Failed to load the Strands EnvironmentProviders published to ServiceLoader; %d"
                  + " loaded successfully [%s]. Fix the offending service declaration.",
              candidates.size(), names(candidates)),
          discoveryFailure);
    }
    if (preferred != null && !preferred.isEmpty()) {
      return selectPreferred(candidates, preferred);
    }
    if (candidates.isEmpty()) {
      return FALLBACK;
    }
    checkNoCollisions(candidates);
    return candidates.get(0).get();
  }

  /**
   * Returns the environment from the provider named by {@link #PREFERRED_PROVIDER_PROPERTY}.
   *
   * <p>Only reached once discovery has completed cleanly, so every published provider is among
   * {@code candidates} and an unmatched name really is a name for nothing.
   */
  private static Environment selectPreferred(
      List<EnvironmentProvider> candidates, String preferred) {
    if (preferred.equals(FALLBACK_PROVIDER_NAME)) {
      return FALLBACK;
    }
    for (EnvironmentProvider candidate : candidates) {
      if (candidate.getClass().getName().equals(preferred)) {
        return candidate.get();
      }
    }
    throw new IllegalStateException(
        String.format(
            "The %s system property names \"%s\", which is not among the Strands"
                + " EnvironmentProviders discovered on the classpath [%s]. The named provider must"
                + " also be published to ServiceLoader. Use \"%s\" to select the built-in"
                + " fallback environment.",
            PREFERRED_PROVIDER_PROPERTY, preferred, names(candidates), FALLBACK_PROVIDER_NAME));
  }

  /** Checks that no two providers claim the same {@link Priority}. */
  private static void checkNoCollisions(List<EnvironmentProvider> candidates) {
    ListMultimap<Priority, EnvironmentProvider> byPriority = ArrayListMultimap.create(3, 1);
    for (EnvironmentProvider candidate : candidates) {
      byPriority.put(candidate.priority(), candidate);
    }
    for (Priority priority : byPriority.keySet()) {
      List<EnvironmentProvider> claimants = byPriority.get(priority);
      if (claimants.size() <= 1) {
        continue;
      }
      throw new IllegalStateException(
          String.format(
              "Multiple Strands EnvironmentProviders claim Priority.%s: [%s]. At most one provider"
                  + " may claim a given priority. Change one provider's priority(), drop one from"
                  + " the classpath, or name the one to use with the %s system property.",
              priority, names(claimants), PREFERRED_PROVIDER_PROPERTY));
    }
  }

  /** Returns the class names of {@code providers}, for use in diagnostics. */
  private static String names(List<EnvironmentProvider> providers) {
    return providers.stream().map(provider -> provider.getClass().getName()).collect(joining(", "));
  }

  /**
   * Returns every {@link EnvironmentProvider} published to {@link ServiceLoader}.
   *
   * <p>The whole iteration is guarded rather than each element because {@link
   * ServiceConfigurationError} can come from {@code hasNext()} as well as {@code next()}, and
   * resuming a failed {@code hasNext()} would spin forever.
   */
  private static Discovery discover() {
    List<EnvironmentProvider> providers = new ArrayList<>();
    try {
      for (EnvironmentProvider provider :
          ServiceLoader.load(
              EnvironmentProvider.class, EnvironmentProvider.class.getClassLoader())) {
        providers.add(provider);
      }
    } catch (ServiceConfigurationError e) {
      return new Discovery(providers, e);
    }
    return new Discovery(providers, null);
  }

  /**
   * Rejects {@link Environment} implementations published as services directly.
   *
   * <p>Only {@link EnvironmentProvider} is discovered, so such a declaration does nothing at all:
   * its author believes they have configured Strands and has not.
   */
  private static void checkForMisdeclaredEnvironments() {
    String misdeclared;
    try {
      misdeclared =
          ServiceLoader.load(Environment.class, Environment.class.getClassLoader()).stream()
              .map(provider -> provider.type().getName())
              .collect(joining(", "));
    } catch (ServiceConfigurationError e) {
      throw new IllegalStateException(
          "Failed to read the java.util.ServiceLoader declarations for"
              + " com.google.async.strands.Environment. Strands only discovers"
              + " com.google.async.strands.EnvironmentProvider, so the declaration would have been"
              + " rejected even had it loaded; remove it, and publish a provider that returns the"
              + " environment instead.",
          e);
    }
    if (!misdeclared.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "[%s] are published as java.util.ServiceLoader services for"
                  + " com.google.async.strands.Environment, which Strands does not discover, so"
                  + " they would have been ignored. Publish a"
                  + " com.google.async.strands.EnvironmentProvider that returns the environment"
                  + " instead.",
              misdeclared));
    }
  }

  private DefaultEnvironment() {}
}
