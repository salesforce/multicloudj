# Flexibility

Substrate SDK remains extensible and you can have your custom providers implementing the driver layer and include the provider jar in the classpath. All you need to do is load the provider with providerId, for example:

```java
BucketClient.builder("<BlobstoreCustomProvider>")...
```

```java
DocstoreClient.builder("DocstoreCustomProvider")...
```

You can also keep the providers outside of this SDK repo anywhere as long as they have access to driver layer.

There might be few use-case where you want to tweak the implementation provided by Substrate SDK for your special requirements. You can can have your own implementation overriding the the implementation provided by Substrate SDK and provide that in the runtime.
