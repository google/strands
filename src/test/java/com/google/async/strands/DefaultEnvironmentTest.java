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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.async.strands.EnvironmentProvider.Priority;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DefaultEnvironmentTest {

  /** Not an anonymous class so that the class name appearing in diagnostics is legible. */
  @ThreadSafe
  private static final class FakeProvider implements EnvironmentProvider {
    private final Priority priority;
    private final Environment environment = new Environment() {};
    private final AtomicInteger getCalls = new AtomicInteger();

    FakeProvider(Priority priority) {
      this.priority = priority;
    }

    @Override
    public Priority priority() {
      return priority;
    }

    @Override
    public Environment get() {
      getCalls.incrementAndGet();
      return environment;
    }
  }

  /** A second named type, so that a collision involves two distinct class names. */
  @ThreadSafe
  private static final class OtherFakeProvider implements EnvironmentProvider {
    private final Priority priority;
    private final Environment environment = new Environment() {};

    OtherFakeProvider(Priority priority) {
      this.priority = priority;
    }

    @Override
    public Priority priority() {
      return priority;
    }

    @Override
    public Environment get() {
      return environment;
    }
  }

  private static Environment resolve(List<EnvironmentProvider> discovered) {
    return DefaultEnvironment.resolve(
        discovered, /* discoveryFailure= */ null, /* preferred= */ null);
  }

  private static Environment resolve(List<EnvironmentProvider> discovered, String preferred) {
    return DefaultEnvironment.resolve(discovered, /* discoveryFailure= */ null, preferred);
  }

  private static void assertIsFallback(Environment environment) {
    assertThat(environment.eventListener()).isSameInstanceAs(EventListener.EMPTY);
    assertThat(environment.timingMode()).isEqualTo(TimingMode.OFF);
    assertThat(environment.contextPropagationOperator()).isEmpty();
  }

  // Precedence.

  @Test
  public void resolve_noProviders_returnsFallback() {
    assertIsFallback(resolve(ImmutableList.of()));
  }

  @Test
  public void resolve_singleProvider_returnsItsEnvironment() {
    FakeProvider only = new FakeProvider(Priority.PLATFORM);

    assertThat(resolve(ImmutableList.of(only))).isSameInstanceAs(only.environment);
  }

  @Test
  public void resolve_multipleProviders_returnsHighestPriority() {
    FakeProvider platform = new FakeProvider(Priority.PLATFORM);
    OtherFakeProvider framework = new OtherFakeProvider(Priority.FRAMEWORK);
    FakeProvider application = new FakeProvider(Priority.APPLICATION);

    assertThat(resolve(ImmutableList.of(platform, framework, application)))
        .isSameInstanceAs(application.environment);
  }

  @Test
  public void resolve_multipleProviders_isIndependentOfDiscoveryOrder() {
    FakeProvider platform = new FakeProvider(Priority.PLATFORM);
    OtherFakeProvider framework = new OtherFakeProvider(Priority.FRAMEWORK);

    assertThat(resolve(ImmutableList.of(platform, framework)))
        .isSameInstanceAs(framework.environment);
    assertThat(resolve(ImmutableList.of(framework, platform)))
        .isSameInstanceAs(framework.environment);
  }

  @Test
  public void resolve_losingProvider_isNeverAskedForItsEnvironment() {
    FakeProvider platform = new FakeProvider(Priority.PLATFORM);
    FakeProvider application = new FakeProvider(Priority.APPLICATION);

    Environment unused = resolve(ImmutableList.of(platform, application));

    assertThat(platform.getCalls.get()).isEqualTo(0);
    assertThat(application.getCalls.get()).isEqualTo(1);
  }

  // Collisions.

  @Test
  public void resolve_twoProvidersClaimSamePriority_throwsIllegalStateException() {
    FakeProvider one = new FakeProvider(Priority.FRAMEWORK);
    OtherFakeProvider two = new OtherFakeProvider(Priority.FRAMEWORK);
    ImmutableList<EnvironmentProvider> discovered = ImmutableList.of(one, two);

    IllegalStateException expected =
        assertThrows(IllegalStateException.class, () -> resolve(discovered));

    assertThat(expected).hasMessageThat().contains("Priority.FRAMEWORK");
    assertThat(expected).hasMessageThat().contains(FakeProvider.class.getName());
    assertThat(expected).hasMessageThat().contains(OtherFakeProvider.class.getName());
    assertThat(expected)
        .hasMessageThat()
        .contains("com.google.async.strands.defaultEnvironmentProvider");
  }

  @Test
  public void resolve_collisionBelowTheWinner_throwsIllegalStateException() {
    // A priority is a claim about a role, so two providers claiming one is a contradiction even
    // though a higher-priority provider would have won regardless.
    FakeProvider platform = new FakeProvider(Priority.PLATFORM);
    OtherFakeProvider alsoPlatform = new OtherFakeProvider(Priority.PLATFORM);
    FakeProvider application = new FakeProvider(Priority.APPLICATION);
    ImmutableList<EnvironmentProvider> discovered =
        ImmutableList.of(platform, alsoPlatform, application);

    IllegalStateException expected =
        assertThrows(IllegalStateException.class, () -> resolve(discovered));

    assertThat(expected).hasMessageThat().contains("Priority.PLATFORM");
  }

  @Test
  public void resolve_collision_doesNotAskEitherProviderForItsEnvironment() {
    FakeProvider one = new FakeProvider(Priority.APPLICATION);
    FakeProvider two = new FakeProvider(Priority.APPLICATION);
    ImmutableList<EnvironmentProvider> discovered = ImmutableList.of(one, two);

    IllegalStateException unused =
        assertThrows(IllegalStateException.class, () -> resolve(discovered));

    assertThat(one.getCalls.get()).isEqualTo(0);
    assertThat(two.getCalls.get()).isEqualTo(0);
  }

  // Discovery failures.

  @Test
  public void resolve_discoveryFailed_throwsIllegalStateException() {
    FakeProvider loaded = new FakeProvider(Priority.PLATFORM);
    ServiceConfigurationError failure = new ServiceConfigurationError("malformed declaration");

    IllegalStateException expected =
        assertThrows(
            IllegalStateException.class,
            () ->
                DefaultEnvironment.resolve(
                    ImmutableList.of(loaded), failure, /* preferred= */ null));

    assertThat(expected).hasCauseThat().isSameInstanceAs(failure);
    assertThat(expected).hasMessageThat().contains(FakeProvider.class.getName());
    assertThat(loaded.getCalls.get()).isEqualTo(0);
  }

  @Test
  public void resolve_discoveryFailedAndNothingLoaded_throwsRatherThanFallingBack() {
    ServiceConfigurationError failure = new ServiceConfigurationError("malformed declaration");

    assertThrows(
        IllegalStateException.class,
        () -> DefaultEnvironment.resolve(ImmutableList.of(), failure, /* preferred= */ null));
  }

  @Test
  public void resolve_discoveryFailed_isNotOverriddenByThePreferredProvider() {
    // The property picks among the providers that were found; it cannot vouch for a classpath that
    // could not be read, even when it happens to name one that did load.
    FakeProvider loaded = new FakeProvider(Priority.PLATFORM);
    ServiceConfigurationError failure = new ServiceConfigurationError("malformed declaration");
    ImmutableList<EnvironmentProvider> discovered = ImmutableList.of(loaded);

    IllegalStateException expected =
        assertThrows(
            IllegalStateException.class,
            () -> DefaultEnvironment.resolve(discovered, failure, FakeProvider.class.getName()));

    assertThat(expected).hasCauseThat().isSameInstanceAs(failure);
    assertThat(loaded.getCalls.get()).isEqualTo(0);
  }

  @Test
  public void resolve_discoveryFailed_isNotOverriddenByTheFallbackSelector() {
    ServiceConfigurationError failure = new ServiceConfigurationError("malformed declaration");

    IllegalStateException expected =
        assertThrows(
            IllegalStateException.class,
            () -> DefaultEnvironment.resolve(ImmutableList.of(), failure, "none"));

    assertThat(expected).hasCauseThat().isSameInstanceAs(failure);
  }

  // Explicit selection via the system property.

  @Test
  public void resolve_preferredProvider_winsOverHigherPriority() {
    FakeProvider platform = new FakeProvider(Priority.PLATFORM);
    OtherFakeProvider application = new OtherFakeProvider(Priority.APPLICATION);

    Environment resolved =
        resolve(ImmutableList.of(platform, application), FakeProvider.class.getName());

    assertThat(resolved).isSameInstanceAs(platform.environment);
  }

  @Test
  public void resolve_preferredProvider_resolvesACollision() {
    FakeProvider one = new FakeProvider(Priority.APPLICATION);
    OtherFakeProvider two = new OtherFakeProvider(Priority.APPLICATION);

    Environment resolved = resolve(ImmutableList.of(one, two), OtherFakeProvider.class.getName());

    assertThat(resolved).isSameInstanceAs(two.environment);
  }

  @Test
  public void resolve_preferredProviderNotDiscovered_throwsIllegalStateException() {
    // Reverting to priority order would silently install the provider the property was set to
    // avoid, making a typo indistinguishable from success.
    FakeProvider platform = new FakeProvider(Priority.PLATFORM);
    ImmutableList<EnvironmentProvider> discovered = ImmutableList.of(platform);

    IllegalStateException expected =
        assertThrows(
            IllegalStateException.class, () -> resolve(discovered, "com.example.NoSuchProvider"));

    assertThat(expected).hasMessageThat().contains("com.example.NoSuchProvider");
    assertThat(expected).hasMessageThat().contains(FakeProvider.class.getName());
    assertThat(platform.getCalls.get()).isEqualTo(0);
  }

  @Test
  public void resolve_preferredNone_returnsFallback() {
    FakeProvider platform = new FakeProvider(Priority.PLATFORM);

    assertIsFallback(resolve(ImmutableList.of(platform), "none"));
    assertThat(platform.getCalls.get()).isEqualTo(0);
  }

  @Test
  public void resolve_preferredNone_overridesACollision() {
    FakeProvider one = new FakeProvider(Priority.APPLICATION);
    OtherFakeProvider two = new OtherFakeProvider(Priority.APPLICATION);

    assertIsFallback(resolve(ImmutableList.of(one, two), "none"));
  }

  @Test
  public void resolve_emptyPreferredProvider_isIgnored() {
    FakeProvider platform = new FakeProvider(Priority.PLATFORM);

    assertThat(resolve(ImmutableList.of(platform), "")).isSameInstanceAs(platform.environment);
  }

  // Memoization.

  @Test
  public void defaultEnvironment_isStableAcrossCalls() {
    assertThat(DefaultEnvironment.get()).isSameInstanceAs(DefaultEnvironment.get());
  }
}
