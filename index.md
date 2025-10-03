---
layout: default
title: MultiCloudJ Overview
description: MultiCloudJ SDK documentation homepage
nav_order: 1
---

# MultiCloudJ
## Write once, deploy in any cloud...

MultiCloudJ providing unified and vendor-neutral interfaces for commonly used cloud services such as Blob Storage, Document Store, Security Token Service (STS), and more. It enabbles the cloud developers to write applications that can developed once and be deployed to any of the cloud providers. The interfaces are designed in a way that it can support any of the major cloud providers.


## Key Features

**Substrate-Neutral**  
Write once, deploy anywhere. No vendor lock-in or provider-specific code.

**Currently Supported Cloud Providers**  
Seamlessly work across AWS, Alibaba Cloud, and Google Cloud Platform with a single, consistent API. Additional cloud providers such as Azure can also be potentially supported.

**Uniform Semantics**  
Common abstractions with unform semantics that work identically across cloud providers.


**Enterprise Ready**  
Built with security, scalability, and maintainability in mind for production workloads.

**Extensiblity**  
Built with extensible architecture design to support additional cloud providers in future.

**Flexiblity**  
Providers flexibility to users to provider their own custom implementation and just have it in the run time.

## Supported Services

### STS (Security Token Service)
Get access credentials using roles, service accounts, and more across all supported cloud providers.

### Blob Storage
Manage object storage with unified operations for uploading, downloading, listing, and managing blobs.

### Document Store
Work with document-oriented databases using consistent interfaces for CRUD operations and querying.

Additional services such pubsub, secrets, compute are also in roadmap.




[Getting Started](getting-started/index.html){: .btn }
## Running Examples

MultiCloudJ includes comprehensive example programs demonstrating how to interact with supported services. Examples are located in the [examples](https://github.com/salesforce/multicloudj/tree/main/examples) directory and cover:

* STS authentication flows
* Blob storage operations
* Document store CRUD operations
* Cross-provider migration scenarios

## Where to Go from Here

**Documentation and Guides:**
* [How-to Guides](guides/index.html) - Step-by-step guides for developers ready to use the SDK
* [Design Decisions](design/index.html) - Architecture and design principles behind the SDK

**API Documentation:**
* [Java API Documentation (Javadoc)](api/java/latest/index.html) - Complete API reference

**Community and Support:**
* [GitHub Issues](https://github.com/salesforce/multicloudj/issues) - Report bugs and request features
* [Discussion Forum](https://github.com/salesforce/multicloudj/issues) - Get help from the community
* [Contribution Guidelines](https://github.com/salesforce/multicloudj/blob/main/CONTRIBUTING.md) - Contribute to the project

**Additional Resources:**
* [MultiCloudJ Homepage](https://github.com/salesforce/multicloudj) - Source code and project information
* [Release Notes](https://github.com/salesforce/multicloudj/releases) - Latest updates and changes
* [Examples Repository](https://github.com/salesforce/multicloudj/tree/main/examples) - Complete working examples

---

*MultiCloudJ is maintained by the Salesforce MultiCloudJ team and is open source under the Apache License 2.0.*
