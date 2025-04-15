# Exception Handling in Substrate SDK

## How Does Exception Handling Work for SDK Users?

Substrate SDK defines substrate-agnostic exceptions for end users, so they don't have to deal with substrate-specific exceptions directly.

The SDK wraps all exceptions from cloud providers in a standardized wrapper based on the exception type before propagating it to the end user. Some broadly categorized exceptions include:

- **SubstrateSdkException** -->  Base class for all the exceptions
- **AccessDeniedException**
- **ResourceNotFoundException**
- **ResourceAlreadyExistsException**
- **ResourceExhaustedException**
- **InvalidArgumentException**
- **FailedPreConditionException**
- **DeadlineExceededException**
- **ResourceConflictException**
- **UnAuthorizedException**
- **UnSupportedOperationException**
- **UnknownException** --> This is used when no predefined mapping exists. Ideally, as the SDK evolves, there should be no occurrences of this exception.


---

## How Does Exception Handling Work for Provider Implementations?

### Handling Server-Side Exceptions 
Server-side exceptions are relatively straightforward because provider implementations do not need to process them explicitly. These exceptions are considered **RunTimeExceptions**, which are thrown to the client layer and wrapped according to the predefined mapping.

### When Provider Implementations Need to Throw Exceptions
There are cases where provider implementations must throw `SubstrateSdkException` themselves. Some examples include:

1. **Input Validation Failures**
    - If a provider implementation validates input parameters and detects an issue, it can throw an `InvalidArgumentException` directly.

2. **Converting Server-Side Exceptions to Different Semantics**
    - In some cases, a provider implementation may need to reinterpret server-side exceptions.
    - Example: In **DocStore**, the `create` API expects the item **not** to exist, while the `put` API does not have this constraint. If a key already exists, the provider must decide which exception to throw based on the API semantics.

#### Exception Wrapping
- If the exception is **already a subclass of `SubstrateSdkException`**, it is **not** re-wrapped.
- Otherwise, exceptions are wrapped according to predefined mappings to ensure a consistent experience for SDK users.

---
