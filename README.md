#

[![Java 11 Build](https://github.com/salesforce/multicloudj/actions/workflows/java11-build.yml/badge.svg)](https://github.com/salesforce/multicloudj/actions/workflows/java11-build.yml)
[![Java 17 Build](https://github.com/salesforce/multicloudj/actions/workflows/java17-build.yml/badge.svg)](https://github.com/salesforce/multicloudj/actions/workflows/java17-build.yml)
[![Java 21 Build](https://github.com/salesforce/multicloudj/actions/workflows/java21-build.yml/badge.svg)](https://github.com/salesforce/multicloudj/actions/workflows/java21-build.yml)
[![codecov](https://codecov.io/gh/salesforce/multicloudj/branch/main/graph/badge.svg)](https://codecov.io/gh/salesforce/multicloudj)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Maven Central](https://img.shields.io/maven-central/v/com.salesforce.multicloudj/multicloudj-parent.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/com.salesforce.multicloudj/multicloudj-parent)


---

 MultiCloudJ
----------------------
Write once, deploy to any cloud provider...

MultiCloudJ is a cloud-agnostic Java SDK providing unified and substrate-neutral interfaces for cloud services. It enables developers to write once and deploy to any cloud provider with high-level APIs for Security Token Service (STS), Blob Storage, Document Store, and more, supporting major cloud providers like AWS, GCP, and Alibaba.
- **Security Token Service (STS)**
- **Blob Store**
- **Document Store**
- more to common...

MultiCloudJ simplifies multi-cloud compatibility, enabling consistent codebases and accelerating development for applications that needs to be deployed across different cloud platforms.

For more information, see [the MulticloudJ official web site](https://opensource.salesforce.com/multicloudj).

---

Requirements
--------------------

- Java 11 or higher
- Maven 3.8 or higher build automation

Getting Started
---------------------

This short [Getting Started Guide](https://opensource.salesforce.com/multicloudj/getting-started) will walk you through basic operations on blob store, 
and demonstrate some simple reads and writes.

For more examples: please refer to [examples](https://github.com/salesforce/multicloudj/tree/main/examples) where we have detailed examples on blob store, docstore and sts.

---


Building and Contributing
------------------------

To build MultiCloudJ from source:

```bash
git clone https://www.github.com/salesforce/multicloudj.git
cd multicloudj
mvn clean install
```

Visit our [Contribution Guidelines](CONTRIBUTING.md) for more information on how to contribute.

---

Documentation
------------------------
Detailed documentation can be found on our [official documentation site](https://opensource.salesforce.com/multicloudj).

---

Community
------------------------
- **Issues and Bug Reports**: [Github Issues](https://www.github.com/salesforce/multicloudj/issues)
- **Discussion and Q&A**: [Discussions](https://www.github.com/salesforce/multicloudj/issues)

---

License
------------------------
MultiCloudJ is released under the [Apache License 2.0](LICENSE.txt).
