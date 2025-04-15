# Contributing to MultiCloudJ

We appreciate all contributions to MultiCloudJ, whether they are bug reports, feature requests, improvements, or documentation updates. Your efforts help us maintain MultiCloudJ as an effective and comprehensive multi-substrate Java SDK.

---

## How to Contribute

### Reporting Issues

Before reporting an issue, please search existing issues on our [Issue Tracker](https://github.com/salesforce/multicloudj.git/issues) to ensure your issue hasn't already been reported. When creating a new issue:

- Provide a clear, descriptive title.
- Describe the issue clearly, including steps to reproduce, expected behavior, and actual behavior.
- Include relevant version information and environment details (Java version, operating system, cloud provider specifics).
- A sample client side code which reproduces the issue.

### Proposing New Features

Feature requests are always welcome. Before proposing a feature, please take a look at the roadmap if it's already there. When proposing a new feature:

- Clearly explain the feature and its benefits.
- If possible, research if the feature you are requesting can be supported across all cloud providers. 
- Provide examples, use cases, or other details to illustrate your request.

Please open a new issue with the label `enhancement`.

### Preparing to contribute code changes
#### Code Review Criteria

To improve your chances of successful contributions, familiarize yourself with the review process and rejection criteria. The [code reviewer's guide](https://google.github.io/eng-practices/review/reviewer/) from Google's Engineering Practices documentation provides valuable insights. Essentially, changes offering significant benefits with minimal risk tend to be merged quickly, while high-risk or low-value changes are more likely to be rejected.
**Positives:**
- Fixes the root cause of a bug in existing functionality
- Adds functionality or fixes problems needed by many users but provided the functionality is supported all cloud providers.
- Easily tested; includes tests and conformance tests.
- Reduces complexity and lines of code
- Change is discussed and familiar to committers

**Negatives/Risks:**
- Band-aids symptoms of a bug rather than fixing root causes
- Introduces cloud provider specific feature.
- Changes public API or semantics (rarely allowed and need to be in consensus beforehand)
- Adds substantial amounts of code
- Includes extensive modifications in a single "big bang" change


### Contributing Code

Contributions in the form of pull requests are highly encouraged but please communicate to set the context 
in advance by starting discussion thread on issues. If changes are minor, the PR with a good description 
explaining changes should suffice.

#### Step-by-step Guide

1. **Fork the repository** and clone your fork:

```bash
git clone https://github.com/<YOUR-USER-NAME>/multicloudj.git
```
2. **Create a branch** for your changes:

```bash
git checkout -b feature/my-new-feature
```

3. **Make your changes** and ensure that the code compiles and tests pass:

```bash
mvn clean test
```

4. **Commit your changes** with clear, concise, and descriptive commit messages.

```bash
git commit -m "[MULTICLOUDJ-1234] Add support for XYZ feature"
```

5. **Push your branch** to your fork:

```bash
git push origin feature/my-new-feature
```

6. **Open a pull request** from your forked branch to the `main` branch of MultiCloudJ. Provide a clear description of your changes, and reference relevant issues if applicable.

#### Pull Request Guidelines

- Make sure your pull request has a descriptive title.
- Describe your changes clearly and succinctly.
- Include unit tests to validate new functionality.
- Conformance tests should be added to validate the functionality you are adding/changing.
- Reference any related issues in your pull request description (e.g., fixes #123).

---

## Coding Style

MultiCloudJ follows standard Java coding conventions:

- We follow the [java style guidelines set by google](https://google.github.io/styleguide/javaguide.html#s1.1-terminology), make sure your PR adheres to this style.

---

## Testing Guidelines

- Include unit tests for all new features or bug fixes.
- Ensure your changes pass existing unit tests.
- Run tests locally using:

```bash
mvn clean test
```

---

## Documentation Contributions

Documentation updates and improvements are strongly encouraged.

- Update documentation in Markdown format.
- Ensure clarity and correctness.
- Include examples whenever possible.

---

## Community

We encourage you to participate in discussions, provide feedback, or seek help via our [Discussions Forum](https://github.com/yourorg/multicloudj/discussions).

---

## Code of Conduct

Please be respectful and considerate to all community members. See our [Code of Conduct](CODE_OF_CONDUCT.md) for more details.

---

Thank you for contributing to MultiCloudJ!