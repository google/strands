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

import static com.google.common.base.StandardSystemProperty.JAVA_VERSION;
import static com.google.common.base.Throwables.throwIfUnchecked;

import com.google.common.flogger.GoogleLogger;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * Configures virtual thread scheduling and enforces the single-execution guarantee.
 *
 * <p>Currently, the JDK does not expose a public API to create virtual threads with a custom
 * scheduler. As a workaround, this class uses reflection to call a private constructor to create
 * virtual threads with a custom scheduler. This is known to work on at least JDK 25-28. Until this
 * is fixed, users will need to either provide a JVM argument (see below) or explicitly opt out of
 * the custom scheduler which entirely disables the single-execution guarantee.
 *
 * <p>By default, Strands enforces a single-execution guarantee (where all task virtual threads in a
 * scope share a sequential carrier executor) to ensure data-race freedom across sibling strands. On
 * standard OpenJDK, this requires the JVM argument:
 *
 * <pre>{@code
 * --add-opens java.base/java.lang=ALL-UNNAMED
 * }</pre>
 *
 * <p>To run in environments where this JVM argument cannot be provided, users may explicitly opt
 * out of the single-execution guarantee by setting:
 *
 * <pre>{@code
 * -Dcom.google.async.strands.allowUnsafeConcurrentExecution=true
 * }</pre>
 *
 * <p><b>WARNING:</b> Disabling the single-execution guarantee causes sibling strands to execute
 * concurrently in parallel across platform carrier threads, requiring callers to explicitly
 * synchronize all shared mutable state. A warning will be logged at runtime when this is disabled.
 */
final class VirtualThreadSupport {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * System property to explicitly opt out of the single-execution guarantee.
   *
   * <p>Defaults to {@code false}.
   */
  static final String ALLOW_UNSAFE_CONCURRENT_EXECUTION_PROPERTY =
      "com.google.async.strands.allowUnsafeConcurrentExecution";

  private static final Factory FACTORY;
  private static final Supplier<Executor> TASK_EXECUTOR;

  /**
   * Returns whether {@code java.base/java.lang} is open to the Strands module for deep reflection.
   */
  static boolean isJavaLangOpen() {
    return Object.class.getModule().isOpen("java.lang", VirtualThreadSupport.class.getModule());
  }

  static {
    boolean allowUnsafe = Boolean.getBoolean(ALLOW_UNSAFE_CONCURRENT_EXECUTION_PROPERTY);
    if (allowUnsafe) {

      logger.atWarning().log(
          """
          ==================================================================================
          WARNING: STRANDS SINGLE-EXECUTION GUARANTEE IS DISABLED!
          Property '%s' is set to true.
          Virtual threads will execute concurrently in parallel on the default ForkJoinPool.
          Shared mutable state across strands is NOT thread-safe and MUST be synchronized.
          ==================================================================================
          """,
          ALLOW_UNSAFE_CONCURRENT_EXECUTION_PROPERTY);
    }
    FACTORY = allowUnsafe ? new UnsafeFactory() : ReflectionFactory.create();
    TASK_EXECUTOR =
        allowUnsafe
            ? () -> null // Unused by the UnsafeFactory.
            : () -> new SequentialExecutor(ForkJoinPool.commonPool());
  }

  /**
   * Returns a {@link ThreadFactory} that can be used to create virtual threads that execute tasks
   * for Strands.
   */
  static final ThreadFactory taskThreadFactory() {
    return FACTORY.create(TASK_EXECUTOR.get()).name("Strands-Task-Virtual-", 1L).factory();
  }

  /**
   * Returns a {@link ThreadFactory} that can be used to create virtual threads that execute
   * internal support tasks for Strands.
   */
  static final ThreadFactory frameworkThreadFactory() {
    return FACTORY
        .create(ForkJoinPool.commonPool())
        .name("Strands-Framework-Virtual-", 1L)
        .factory();
  }

  /**
   * A factory for creating {@link Thread.Builder.OfVirtual} with a custom scheduler.
   *
   * <p>This is used to support custom scheduling on the JDK.
   */
  interface Factory {
    /**
     * Returns a {@link Thread.Builder.OfVirtual} configured with the given scheduler.
     *
     * @throws UnsupportedOperationException if custom scheduling is required but unavailable on
     *     this JDK and unsafe concurrent execution has not been explicitly allowed
     */
    Thread.Builder.OfVirtual create(Executor scheduler);
  }

  /**
   * A {@link Factory} that creates standard virtual threads on the default {@link ForkJoinPool}.
   */
  static final class UnsafeFactory implements Factory {
    @Override
    public Thread.Builder.OfVirtual create(Executor unused) {
      return Thread.ofVirtual();
    }
  }

  /** A {@link Factory} that fails with an actionable {@link UnsupportedOperationException}. */
  static final class BrokenFactory implements Factory {
    private final @Nullable Throwable cause;

    BrokenFactory() {
      this(null);
    }

    BrokenFactory(@Nullable Throwable cause) {
      this.cause = cause;
    }

    @Override
    public Thread.Builder.OfVirtual create(Executor scheduler) {
      Module ourModule = VirtualThreadSupport.class.getModule();
      String targetModule = ourModule.isNamed() ? ourModule.getName() : "ALL-UNNAMED";
      if (cause == null) {
        throw new UnsupportedOperationException(
            String.format(
                """
                Strands requires the single-execution guarantee to ensure thread safety across \
                tasks. Either launch the JVM with '--add-opens java.base/java.lang=%s' \
                to enable custom virtual thread scheduling, or explicitly opt out by setting \
                the system property '-D%s=true' (WARNING: this disables the single-execution \
                guarantee and requires explicit synchronization for shared mutable state).\
                """,
                targetModule, ALLOW_UNSAFE_CONCURRENT_EXECUTION_PROPERTY));
      }
      throw new UnsupportedOperationException(
          String.format(
              """
              Failed to reflectively configure custom virtual thread scheduling on JDK %s, \
              even though module java.base is opened to %s: %s. \
              You can opt out of the single-execution guarantee by setting the system property \
              '-D%s=true' (WARNING: this disables the single-execution guarantee and \
              requires explicit synchronization for shared mutable state).\
              """,
              System.getProperty(JAVA_VERSION.value()),
              targetModule,
              cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName(),
              ALLOW_UNSAFE_CONCURRENT_EXECUTION_PROPERTY),
          cause);
    }
  }

  /**
   * A {@link Factory} that uses reflection to create a {@link Thread.Builder.OfVirtual} with the a
   * custom scheduler.
   */
  static final class ReflectionFactory implements Factory {
    private final MethodHandle handle;

    private ReflectionFactory(MethodHandle handle) {
      this.handle = handle;
    }

    /**
     * Returns a {@link Factory} that uses reflection to create a {@link Thread.Builder.OfVirtual}
     * with a custom scheduler or {@link BrokenFactory} if reflection fails.
     *
     * <p>This is known to work on at least JDK 25-28.
     */
    static Factory create() {
      if (!isJavaLangOpen()) {
        return new BrokenFactory();
      }
      try {
        Class<?> clazz = Class.forName("java.lang.ThreadBuilders$VirtualThreadBuilder");
        Constructor<?> ctor = clazz.getDeclaredConstructor(Executor.class);
        MethodHandle ctorHandle =
            MethodHandles.privateLookupIn(clazz, MethodHandles.lookup())
                .unreflectConstructor(ctor)
                .asType(MethodType.methodType(Thread.Builder.OfVirtual.class, Executor.class));
        return new ReflectionFactory(ctorHandle);
      } catch (Throwable t) {
        return new BrokenFactory(t);
      }
    }

    @Override
    public Thread.Builder.OfVirtual create(Executor scheduler) {
      try {
        return (Thread.Builder.OfVirtual) handle.invokeExact(scheduler);
      } catch (Throwable t) {
        throwIfUnchecked(t);
        throw new IllegalStateException(t);
      }
    }
  }

  private VirtualThreadSupport() {}
}
