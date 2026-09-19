# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Starlark macros to generate test suites."""

load("@rules_java//java:defs.bzl", "java_test")

_TEMPLATE = """package {VAR_PACKAGE};
import org.junit.runners.Suite;
import org.junit.runner.RunWith;

@RunWith(Suite.class)
@Suite.SuiteClasses({{{VAR_CLASSES}}})
public class {VAR_NAME} {{}}
"""

def _impl(ctx):
    classes = ",".join(sorted(ctx.attr.test_classes))

    ctx.actions.write(
        output = ctx.outputs.out,
        content = _TEMPLATE.format(
            VAR_PACKAGE = ctx.attr.package_name,
            VAR_CLASSES = classes,
            VAR_NAME = ctx.attr.name,
        ),
    )

_gen_suite = rule(
    attrs = {
        "test_classes": attr.string_list(),
        "package_name": attr.string(),
    },
    outputs = {"out": "%{name}.java"},
    implementation = _impl,
)

def junit4_test_suites(
        name,
        sizes,
        srcs,
        deps,
        data = [],
        **kwargs):  # @unused.
    """Generates tests for test file in srcs ending in "Test.java"

    Args:
      name: name of the test suite to generate
      sizes: list of test sizes (e.g. ["small"])
      srcs: list of test source files
      deps: list of runtime dependencies required to run the test
      data: list of data dependencies required to run the test
      **kwargs: Additional keyword arguments to pass through to the underlying
        `java_test` rule.
    """

    package_name = native.package_name()

    # strip the path prefix from package name so that we get the correct test class name
    # "src/test/java/com/google/async/strands" becomes "com/google/async/strands"
    if "/test/java/" in package_name:
        package_name = package_name.rpartition("/test/java/")[2]

    test_files = srcs
    test_classes = []
    for src in test_files:
        test_name = src.replace(".java", "")
        test_classes.append((package_name + "/" + test_name + ".class").replace("/", "."))

    suite_name = "suite_" + name
    _gen_suite(
        name = suite_name,
        test_classes = test_classes,
        package_name = package_name.replace("/", "."),
    )

    java_test(
        name = name,
        test_class = (package_name + "/" + suite_name).replace("/", "."),
        srcs = [":" + suite_name] + srcs,
        deps = deps,
        data = data,
        tags = sizes,
        **kwargs
    )

    for size in sizes:
        alias_name = size.capitalize() + "Tests"
        if alias_name != name:
            native.alias(
                name = alias_name,
                actual = ":" + name,
            )
