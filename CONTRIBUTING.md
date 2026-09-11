# How to contribute

We'd love to accept your patches and contributions to this project.

## Before you begin

### Sign our Contributor License Agreement

Contributions to this project must be accompanied by a
[Contributor License Agreement](https://cla.developers.google.com/about) (CLA).
You (or your employer) retain the copyright to your contribution; this simply
gives us permission to use and redistribute your contributions as part of the
project.

If you or your current employer have already signed the Google CLA (even if it
was for a different project), you probably don't need to do it again.

Visit <https://cla.developers.google.com/> to see your current agreements or to
sign a new one.

### Review our community guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google/conduct/).

### Understand our Generative AI Policy

AI tools have the ability to produce more code than is possible for the Strands
team to read, understand, review, and accept into the repository. For this
reason, we request that all contributions adhere to the following rules:

1.  **No AI-Generated Interactions:** All communication in the repo must be
    authored by a human. *Exception: AIs may be used for directed writing
    assistance or translation.* Absolutely no automated agents are allowed to
    directly publish to GitHub.

2.  **Author Ownership and Accountability:** Code contributions are expected to
    be fully owned and understood by the human contributor. If the code was
    produced by generative AI, the author is expected to have reviewed and
    understood it in its entirety before submitting it for review. This includes
    all content: production code, tests, examples, tools, etc.

In addition to the above requirements, any AI-assisted contributions must also
comply with the
[Linux Foundation Generative AI Policy](https://www.linuxfoundation.org/legal/generative-ai).
This includes confirming that all contributions are legally allowed to be
contributed to the Strands project under the applicable license terms.

## Contribution process

### API changes

We make changes to Strands' public [APIs](https://en.wikipedia.org/wiki/API),
including adding new APIs, very carefully. Because of this, if you're interested
in seeing a new feature in Strands, the best approach is to create an
[issue](https://github.com/google/strands/issues) (or comment on an existing
issue if there is one) requesting the feature and describing specific use cases
for it.

If we decide to pursue a feature request, it will go through a thorough process
of API design and review. Any code should come after this.

### Pull requests

**All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose.**

Unless the change is a trivial fix such as for a typo, it's generally best to
start by opening a new issue describing the bug or feature you're intending to
fix. Even if you think it's relatively minor, it's helpful to know what people
are working on. And as mentioned above, API changes should be discussed
thoroughly before moving to code.

Some examples of types of pull requests that are immediately helpful:

-   Fixing a bug without changing a public API.
-   Fixing or improving documentation.
-   Improvements to Maven configuration.

Guidelines for any code contributions:

1.  Any significant changes should be accompanied by tests. The project already
    has good test coverage, so look at some existing tests if you're unsure how
    to go about it.
2.  All contributions must be licensed Apache 2.0 and all files must have a copy
    of the boilerplate license comment (can be copied from an existing file).
3.  Files should be formatted according to Google's [Java style guide].
4.  Do your best to have a [well-formed commit message] for the change.

[Java style guide]: https://google.github.io/styleguide/javaguide.html
[well-formed commit message]: https://google.github.io/eng-practices/review/developer/cl-descriptions.html

#### Merging pull requests

Due to this project's nature as a subset of Google's internal codebase which is
automatically synced to the public GitHub repository, we are unable to merge
pull requests directly into the main branch. Instead, once a pull request is
ready for merging, we'll make the appropriate changes in the internal codebase
and, when the change is synced out, give the pull request author credit for the
commit.
