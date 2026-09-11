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

import com.google.async.strands.testing.StrandsRule;
import com.google.async.strands.testing.StrandsRule.InStrand;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ResultTest {

  @Rule public final StrandsRule strands = StrandsRule.create();

  @Test
  public void result_ok_successfulFieldsAreSet() {
    Result<Integer> result = Result.ofValue(0);

    assertThat(result.isOk()).isTrue();
    assertThat(result.isFailed()).isFalse();
  }

  @Test
  public void result_ok_canGetValue() {
    Result<Integer> result = Result.ofValue(0);

    assertThat(result.value()).isEqualTo(0);
  }

  @Test
  public void result_ok_cannotGetFailure() {
    Result<Integer> result = Result.ofValue(0);

    assertThrows(IllegalStateException.class, () -> result.failure());
  }

  @Test
  public void result_failed_successfulFieldsAreSet() {
    Result<Integer> result = Result.ofFailure(new IllegalArgumentException());

    assertThat(result.isOk()).isFalse();
    assertThat(result.isFailed()).isTrue();
  }

  @Test
  public void result_failed_cannotGetValue() {
    Result<Integer> result = Result.ofFailure(new IllegalArgumentException());

    assertThrows(IllegalStateException.class, () -> result.value());
  }

  @Test
  public void result_failed_canGetFailure() {
    IllegalArgumentException exception = new IllegalArgumentException();

    Result<Integer> result = Result.ofFailure(exception);

    assertThat(result.failure()).isSameInstanceAs(exception);
  }

  @Test
  public void result_ok_canGet() {
    Result<Integer> result = Result.ofValue(0);

    assertThat(result.get()).isEqualTo(0);
  }

  @Test
  public void result_failed_cannotGet() {
    Result<Integer> result = Result.ofFailure(new IllegalArgumentException());

    assertThrows(FailedTaskException.class, () -> result.get());
  }

  @Test
  public void result_ok_ifOk_consumesValue() {
    AtomicInteger value = new AtomicInteger(0);
    Result<Integer> result = Result.ofValue(1);

    result.ifOk(value::set);

    assertThat(value.get()).isEqualTo(1);
  }

  @Test
  public void result_failed_ifOk_doesNotConsumeValue() {
    AtomicInteger value = new AtomicInteger(0);
    Result<Integer> result = Result.ofFailure(new IllegalArgumentException());

    result.ifOk(value::set);

    assertThat(value.get()).isEqualTo(0);
  }

  @Test
  public void result_ok_ifFailed_doesNotConsumeValue() {
    AtomicInteger value = new AtomicInteger(0);
    Result<Integer> result = Result.ofValue(1);

    result.ifFailed(_ -> value.set(1));

    assertThat(value.get()).isEqualTo(0);
  }

  @Test
  public void result_failed_ifFailed_consumesValue() {
    IllegalArgumentException exception = new IllegalArgumentException();
    AtomicReference<Throwable> value = new AtomicReference<>(null);
    Result<Integer> result = Result.ofFailure(exception);

    result.ifFailed(value::set);

    assertThat(value.get()).isSameInstanceAs(exception);
  }

  @Test
  public void result_ok_map_transformsValue() throws Exception {
    Result<Integer> result = Result.ofValue(1);

    Result<String> mapped = result.map(v -> "number " + v);

    assertThat(mapped.value()).isEqualTo("number 1");
  }

  @Test
  public void result_failed_map_returnsFailed() throws Exception {
    Exception exception = new Exception();
    Result<Integer> result = Result.ofFailure(exception);

    Result<String> mapped = result.map(v -> "number " + v);

    assertThat(mapped.failure()).isSameInstanceAs(exception);
  }

  @Test
  public void result_ok_mapOrDefault_transformsValue() throws Exception {
    Result<Integer> result = Result.ofValue(1);

    Result<String> mapped = result.mapOrDefault(v -> "number " + v, "default");

    assertThat(mapped.value()).isEqualTo("number 1");
  }

  @Test
  public void result_failed_mapOrDefault_returnsDefault() throws Exception {
    Result<Integer> result = Result.ofFailure(new Exception());

    Result<String> mapped = result.mapOrDefault(v -> "number " + v, "default");

    assertThat(mapped.value()).isEqualTo("default");
  }

  @Test
  public void result_ok_mapOrElse_transformsValue() throws Exception {
    Result<Integer> result = Result.ofValue(1);

    Result<String> mapped = result.mapOrElse(v -> "number " + v, () -> "default");

    assertThat(mapped.value()).isEqualTo("number 1");
  }

  @Test
  public void result_failed_mapOrElse_returnsTaskResult() throws Exception {
    Result<Integer> result = Result.ofFailure(new Exception());

    Result<String> mapped = result.mapOrElse(v -> "number " + v, () -> "default");

    assertThat(mapped.value()).isEqualTo("default");
  }

  @Test
  public void result_ok_flatMap_transformsValue() throws Exception {
    Result<Integer> result = Result.ofValue(1);

    Result<String> flatMapped = result.flatMap(v -> Result.ofValue("number " + v));

    assertThat(flatMapped.value()).isEqualTo("number 1");
  }

  @Test
  public void result_ok_flatMap_toFailed() throws Exception {
    Result<Integer> result = Result.ofValue(1);
    Exception exception = new Exception();

    Result<String> flatMapped = result.flatMap(v -> Result.ofFailure(exception));

    assertThat(flatMapped.failure()).isSameInstanceAs(exception);
  }

  @Test
  @InStrand
  public void result_ok_mapAsync_transformsValue() throws Exception {
    Result<Integer> input = Result.ofValue(1);

    Result<String> mapped = input.mapAsync(v -> "number " + v);

    assertThat(mapped.value()).isEqualTo("number 1");
  }

  @Test
  @InStrand
  public void result_ok_flatMapAsync_transformsValue() throws Exception {
    Result<Integer> input = Result.ofValue(1);

    Result<String> flatMapped = input.flatMapAsync(v -> Result.ofValue("number " + v));

    assertThat(flatMapped.value()).isEqualTo("number 1");
  }

  @Test
  @InStrand
  public void result_ok_flatMapAsync_toFailed() throws Exception {
    Exception exception = new Exception();
    Result<Integer> input = Result.ofValue(1);

    Result<String> flatMapped = input.flatMapAsync(v -> Result.ofFailure(exception));

    assertThat(flatMapped.failure()).isSameInstanceAs(exception);
  }

  @Test
  public void result_ok_ifOk_pecsCompliance() {
    Result<Integer> result = Result.ofValue(1);
    AtomicReference<Number> value = new AtomicReference<>(null);
    Consumer<Number> action = value::set;

    result.ifOk(action);

    assertThat(value.get()).isEqualTo(1);
  }

  @Test
  public void result_ok_map_pecsCompliance() throws Exception {
    Result<Integer> result = Result.ofValue(1);
    Transform<Number, String, Exception> transform = n -> "number " + n;

    Result<String> mapped = result.map(transform);

    assertThat(mapped.value()).isEqualTo("number 1");
  }
}
