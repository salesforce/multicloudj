---
layout: default
title: Design
nav_order: 4
has_children: true
permalink: /design/
---

# Design Guides

Welcome to the MultiCloudJ Design Guides. This section provides concise, task-focused guides to help you understand the key design decisions behind the SDK.

- [Layers of the SDK](layers.html) - Understanding the layered architecture
- [Error Handling](errors.html) - How errors are handled across the SDK
- [Flexibility](flexibility.html) - How to extend and customize the SDK

## Core Design Principles

MultiCloudJ is built on several key principles:

1. **Unified/Cloud Agnostic Interfaces/**: Write once, interact across multiple cloud providers without changing your application code.
2. **Extensibility**: New cloud providers can be added without modifying existing code.
3. **Flexibility**: Easily override the default implementations and inject your own custom implementation in the env.
4. **Simplicity**: The API is designed to be intuitive and easy to use.
5. **Uniform Semantics**: SDK provides the uniform semantics to the end user irrespective of the cloud provider.

   **Examples:**
   
   - **Object Deletion**: AWS S3 returns HTTP 200 when deleting a non-existent object, while Google Cloud Storage returns HTTP 404. MultiCloudJ provides consistent semantics, returning either 200 (success) or 400 (error) regardless of the underlying provider.
   
   - **Pagination**: DynamoDB paginated queries return `LastEvaluatedKey` for continuation, while Google Cloud Firestore doesn't provide this field and let the user figure that out using the last document in the page. MultiCloudJ abstracts this complexity and provides a uniform pagination API that works consistently across all providers.
6. **Reliability**: Robust error handling and retry mechanisms ensure reliable operation.

## Contributing to Design

We welcome contributions to improve MultiCloudJ's design. When contributing:

1. Follow the established architectural patterns
2. Document your design decisions
3. Consider backward compatibility
4. Test your changes thoroughly
5. Update relevant documentation

## Design Decisions

Key design decisions in MultiCloudJ include:

- **Layered Architecture**: Separating concerns between portable, driver, and provider layers
- **Builder Pattern**: Using builders for flexible client configuration
- **Provider Loading**: Dynamic loading of cloud provider implementations
- **Error Handling**: Comprehensive error handling and recovery mechanisms
- **Extensibility**: Support for custom providers and implementations

## Best Practices

When working with MultiCloudJ's design:

1. **Keep it Simple**: Prefer simple solutions over complex ones
2. **Be Consistent**: Follow established patterns and conventions
3. **Think Ahead**: Consider future extensibility and maintenance
4. **Document Everything**: Clear documentation is essential
5. **Test Thoroughly**: Ensure your changes work across all supported providers

## Related Resources

- [API Documentation](../api/java/latest/index.html)
- [How-to Guides](../guides/index.html)
- [Examples](https://github.com/salesforce/multicloudj/tree/main/examples)
- [Contribution Guidelines](https://github.com/salesforce/multicloudj/blob/main/CONTRIBUTING.md)
