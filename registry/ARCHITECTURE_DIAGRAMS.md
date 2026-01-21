# MultiCloudJ Registry Architecture Diagrams

This document contains detailed architecture diagrams for the MultiCloudJ Registry module.

## Table of Contents
1. [System Architecture Overview](#system-architecture-overview)
2. [Component Architecture](#component-architecture)
3. [Class Diagram](#class-diagram)
4. [Data Flow Diagram](#data-flow-diagram)
5. [Authentication Architecture](#authentication-architecture)
6. [Pull Flow Architecture](#pull-flow-architecture)
7. [Deployment Architecture](#deployment-architecture)

## System Architecture Overview

### High-Level System Architecture

```mermaid
graph TB
    subgraph "Application Layer"
        App1[Application 1]
        App2[Application 2]
        App3[Application N]
    end
    
    subgraph "MultiCloudJ Registry Module"
        subgraph "Portable Layer"
            RC[RepositoryClient<br/>Public API<br/>Builder Pattern<br/>Exception Handling]
        end
        
        subgraph "Driver Layer"
            AR[AbstractRegistry<br/>Common Pull Logic<br/>Multi-Arch Support<br/>Tar Building]
            ORC[OciRegistryClient<br/>HTTP Client<br/>Auth Management<br/>Retry Logic]
            DTB[DockerTarBuilder<br/>Tar Assembly<br/>Stream Support]
            
            subgraph "Authentication Components"
                RP[RegistryPing<br/>Auth Discovery]
                AC[AuthChallenge<br/>Challenge Parser]
                BTE[BearerTokenExchange<br/>Token Exchange]
            end
            
            subgraph "Supporting Classes"
                IR[ImageReference<br/>Reference Parser]
                IM[ImageMetadata<br/>Metadata Model]
                PR[PullResult<br/>Result Model]
            end
        end
        
        subgraph "Provider Layer"
            AWS[AwsRegistry<br/>AWS ECR<br/>ECR API Integration]
            GCP[GcpRegistry<br/>GCP GAR<br/>OAuth2 Integration]
            ALI[AliRegistry<br/>Alibaba ACR<br/>ACR API Integration]
        end
    end
    
    subgraph "Cloud Services"
        ECR[AWS ECR<br/>Elastic Container Registry]
        GAR[GCP Artifact Registry<br/>Container Registry]
        ACR[Alibaba ACR<br/>Container Registry]
    end
    
    subgraph "Infrastructure"
        STS[STS Service<br/>Credential Management]
        HTTP[HTTP/HTTPS<br/>Network Layer]
    end
    
    App1 --> RC
    App2 --> RC
    App3 --> RC
    
    RC --> AR
    AR --> ORC
    AR --> DTB
    AR --> IR
    AR --> IM
    AR --> PR
    
    ORC --> RP
    ORC --> AC
    ORC --> BTE
    
    AR -.->|extends| AWS
    AR -.->|extends| GCP
    AR -.->|extends| ALI
    
    AWS --> ECR
    GCP --> GAR
    ALI --> ACR
    
    AWS --> STS
    GCP --> STS
    ALI --> STS
    
    ORC --> HTTP
    AWS --> HTTP
    GCP --> HTTP
    ALI --> HTTP
    
    ECR --> HTTP
    GAR --> HTTP
    ACR --> HTTP
```

## Component Architecture

### Detailed Component Relationships

```mermaid
graph LR
    subgraph "User Code"
        User[User Application]
    end
    
    subgraph "Portable Layer - RepositoryClient"
        RC[RepositoryClient]
        RCB[RepositoryClientBuilder]
    end
    
    subgraph "Driver Layer - AbstractRegistry"
        AR[AbstractRegistry]
        ARB[AbstractRegistry.Builder]
    end
    
    subgraph "Driver Layer - OCI Protocol"
        ORC[OciRegistryClient]
        ORC_Auth[AuthProvider Interface]
    end
    
    subgraph "Driver Layer - Authentication"
        RP[RegistryPing]
        AC[AuthChallenge]
        BTE[BearerTokenExchange]
    end
    
    subgraph "Driver Layer - Image Processing"
        IR[ImageReference]
        DTB[DockerTarBuilder]
        IM[ImageMetadata]
        PR[PullResult]
    end
    
    subgraph "Provider Layer"
        AWS[AwsRegistry]
        GCP[GcpRegistry]
        ALI[AliRegistry]
    end
    
    subgraph "Cloud Services"
        ECR[AWS ECR]
        GAR[GCP GAR]
        ACR[Alibaba ACR]
    end
    
    User -->|uses| RC
    RC -->|uses| RCB
    RCB -->|creates| AR
    RC -->|delegates to| AR
    
    AR -->|uses| ORC
    AR -->|uses| DTB
    AR -->|uses| IR
    AR -->|creates| IM
    AR -->|returns| PR
    
    ORC -->|implements| ORC_Auth
    ORC -->|uses| RP
    ORC -->|uses| AC
    ORC -->|uses| BTE
    
    AR -.->|abstract| ARB
    ARB -.->|implemented by| AWS
    ARB -.->|implemented by| GCP
    ARB -.->|implemented by| ALI
    
    AWS -->|calls| ECR
    GCP -->|calls| GAR
    ALI -->|calls| ACR
    
    AWS -->|provides| ORC_Auth
    GCP -->|provides| ORC_Auth
    ALI -->|provides| ORC_Auth
```

## Class Diagram

### Core Classes and Interfaces

```mermaid
classDiagram
    class RepositoryClient {
        -AbstractRegistry registry
        +builder(providerId) RepositoryClientBuilder
        +pullImage(imageRef, Path) PullResult
        +pullImage(imageRef, OutputStream) PullResult
        +pullImage(imageRef, File) PullResult
        +pullImage(imageRef) PullResult
        +close()
    }
    
    class RepositoryClientBuilder {
        -AbstractRegistry.Builder registryBuilder
        +withRepository(repo) RepositoryClientBuilder
        +withRegion(region) RepositoryClientBuilder
        +withEndpoint(endpoint) RepositoryClientBuilder
        +withCredentialsOverrider(overrider) RepositoryClientBuilder
        +build() RepositoryClient
    }
    
    class AbstractRegistry {
        #String providerId
        #String repository
        #String region
        #CredentialsOverrider credentialsOverrider
        +pullImage(imageRef, Path) PullResult
        +pullImage(imageRef, OutputStream) PullResult
        +pullImage(imageRef, File) PullResult
        +pullImage(imageRef) PullResult
        #getRegistryEndpoint() String*
        #getDockerAuthToken() String*
        #getDockerAuthUsername() String*
        #getIdentityToken() String
        #doPullImageUsingOciRegistry(imageRef, Path) PullResult
        #doPullImageUsingOciRegistry(imageRef, OutputStream) PullResult
        #doPullImageUsingOciRegistry(imageRef) PullResult
    }
    
    class AwsRegistry {
        -EcrClient ecrClient
        +getRegistryEndpoint() String
        +getDockerAuthToken() String
        +getDockerAuthUsername() String
        +getIdentityToken() String
    }
    
    class GcpRegistry {
        -GoogleCredentials credentials
        +getRegistryEndpoint() String
        +getDockerAuthToken() String
        +getDockerAuthUsername() String
        +getIdentityToken() String
    }
    
    class AliRegistry {
        -AcsClient acsClient
        +getRegistryEndpoint() String
        +getDockerAuthToken() String
        +getDockerAuthUsername() String
        +getIdentityToken() String
    }
    
    class OciRegistryClient {
        -String registryEndpoint
        -String repository
        -AuthProvider authProvider
        -AuthChallenge challenge
        -BearerTokenExchange tokenExchange
        +fetchManifest(reference) Manifest
        +downloadBlob(digest) InputStream
        -getAuthHeader() String
        -ping() AuthChallenge
    }
    
    class AuthProvider {
        <<interface>>
        +getBasicAuthHeader() String
        +getIdentityToken() String
    }
    
    class RegistryPing {
        +ping(registryEndpoint) AuthChallenge
        -sendPingRequest(endpoint) HttpResponse
        -parseChallenge(wwwAuthHeader) AuthChallenge
    }
    
    class AuthChallenge {
        -String scheme
        -String realm
        -String service
        -Map~String,String~ parameters
        +getScheme() String
        +getRealm() String
        +getService() String
    }
    
    class BearerTokenExchange {
        +getBearerToken(challenge, identityToken, scopes) String
        -exchangeToken(realm, identityToken, scopes) String
    }
    
    class DockerTarBuilder {
        -TarArchiveOutputStream tarOut
        +addVersion()
        +addConfig(layerId, configJson)
        +addLayer(layerId, layerData)
        +addManifest(manifestJson)
        +close()
    }
    
    class ImageReference {
        -String registry
        -String repository
        -String reference
        -boolean isDigest
        +parse(imageRef) ImageReference
        +getRepository() String
        +getReference() String
    }
    
    class PullResult {
        -ImageMetadata metadata
        -String savedPath
        -InputStream inputStream
    }
    
    class ImageMetadata {
        -String digest
        -String tag
    }
    
    RepositoryClient --> AbstractRegistry
    RepositoryClient --> RepositoryClientBuilder
    RepositoryClientBuilder --> AbstractRegistry
    AbstractRegistry <|-- AwsRegistry
    AbstractRegistry <|-- GcpRegistry
    AbstractRegistry <|-- AliRegistry
    AbstractRegistry --> OciRegistryClient
    AbstractRegistry --> DockerTarBuilder
    AbstractRegistry --> ImageReference
    AbstractRegistry --> PullResult
    OciRegistryClient --> AuthProvider
    OciRegistryClient --> RegistryPing
    OciRegistryClient --> AuthChallenge
    OciRegistryClient --> BearerTokenExchange
    RegistryPing --> AuthChallenge
    BearerTokenExchange --> AuthChallenge
    PullResult --> ImageMetadata
    AwsRegistry ..|> AuthProvider
    GcpRegistry ..|> AuthProvider
    AliRegistry ..|> AuthProvider
```

## Data Flow Diagram

### Complete Pull Image Data Flow

```mermaid
sequenceDiagram
    participant User
    participant RC as RepositoryClient
    participant AR as AbstractRegistry
    participant IR as ImageReference
    participant ORC as OciRegistryClient
    participant RP as RegistryPing
    participant AC as AuthChallenge
    participant BTE as BearerTokenExchange
    participant DTB as DockerTarBuilder
    participant Cloud as Cloud Registry
    
    User->>RC: pullImage("my-image:latest", path)
    RC->>AR: pullImage(imageRef, destination)
    
    AR->>IR: parse(imageRef)
    IR-->>AR: ImageReference{repo, ref}
    
    AR->>AR: getRegistryEndpoint()
    AR-->>AR: "https://registry.example.com"
    
    AR->>AR: Create AuthProvider (anonymous class)
    Note over AR: Implements getBasicAuthHeader()<br/>and getIdentityToken()
    
    AR->>ORC: new OciRegistryClient(endpoint, repo, authProvider)
    
    Note over ORC: First Request - Need Auth
    ORC->>RP: ping(registryEndpoint)
    RP->>Cloud: GET /v2/
    Cloud-->>RP: 401 Unauthorized<br/>WWW-Authenticate: Bearer realm=...
    RP->>AC: parse(wwwAuthHeader)
    AC-->>RP: AuthChallenge{scheme: "Bearer", realm: "..."}
    RP-->>ORC: AuthChallenge
    
    ORC->>ORC: challenge.getScheme() == "Bearer"
    ORC->>AR: authProvider.getIdentityToken()
    AR->>AR: getIdentityToken() (GCP: OAuth2 token)
    AR-->>ORC: identityToken
    ORC->>BTE: getBearerToken(challenge, identityToken)
    BTE->>Cloud: POST /token<br/>grant_type=refresh_token<br/>service=...
    Cloud-->>BTE: {"access_token": "..."}
    BTE-->>ORC: bearerToken
    ORC->>ORC: Store bearerToken
    
    Note over ORC: Fetch Manifest
    ORC->>Cloud: GET /v2/{repo}/manifests/{ref}<br/>Authorization: Bearer {token}
    Cloud-->>ORC: Manifest JSON
    
    alt Multi-Arch Image
        ORC->>ORC: manifest.isIndex() == true
        ORC->>ORC: Select platform manifest
        ORC->>Cloud: GET /v2/{repo}/manifests/{digest}
        Cloud-->>ORC: Image Manifest
    end
    
    ORC-->>AR: Manifest{configDigest, layerDigests}
    
    Note over AR: Download Config
    AR->>ORC: downloadBlob(configDigest)
    ORC->>Cloud: GET /v2/{repo}/blobs/{configDigest}<br/>Authorization: Bearer {token}
    Cloud-->>ORC: Config JSON Stream
    ORC-->>AR: InputStream (config)
    AR->>AR: Read config to String
    
    Note over AR: Build Docker Tar
    AR->>DTB: new DockerTarBuilder(destination)
    AR->>DTB: addVersion()
    AR->>DTB: addConfig(layerId, configJson)
    
    loop For each layer
        AR->>ORC: downloadBlob(layerDigest)
        ORC->>Cloud: GET /v2/{repo}/blobs/{layerDigest}<br/>Authorization: Bearer {token}
        Cloud-->>ORC: Layer Data Stream
        ORC-->>AR: InputStream (layer)
        AR->>DTB: addLayer(layerId, layerStream)
        DTB->>DTB: Write layer to tar
    end
    
    AR->>DTB: addManifest(manifestJson)
    AR->>DTB: close()
    DTB-->>AR: Tar file complete
    
    AR->>AR: Build PullResult
    AR-->>RC: PullResult{metadata, savedPath}
    RC-->>User: PullResult
```

## Authentication Architecture

### Authentication Flow Diagram

```mermaid
stateDiagram-v2
    [*] --> StartPull
    
    StartPull --> ParseReference: Parse imageRef
    ParseReference --> GetEndpoint: Get registry endpoint
    
    GetEndpoint --> CreateClient: Create OciRegistryClient
    
    CreateClient --> PingRegistry: First request
    
    PingRegistry --> CheckResponse: GET /v2/
    
    CheckResponse --> NoAuth: 200 OK
    CheckResponse --> ParseChallenge: 401 Unauthorized
    
    NoAuth --> FetchManifest: No authentication needed
    
    ParseChallenge --> DetermineScheme: Parse WWW-Authenticate header
    
    DetermineScheme --> BasicAuth: Scheme = "Basic"
    DetermineScheme --> BearerAuth: Scheme = "Bearer"
    
    BasicAuth --> GetBasicCredentials: Get username:token
    GetBasicCredentials --> BuildBasicHeader: Build "Basic base64(...)"
    BuildBasicHeader --> FetchManifest: Use Basic Auth header
    
    BearerAuth --> GetIdentityToken: Get OAuth2 identity token
    GetIdentityToken --> ExchangeToken: POST to token server
    ExchangeToken --> ReceiveBearerToken: Receive Bearer Token
    ReceiveBearerToken --> StoreToken: Store token
    StoreToken --> FetchManifest: Use Bearer Token header
    
    FetchManifest --> CheckManifestType: GET /v2/{repo}/manifests/{ref}
    
    CheckManifestType --> HandleIndex: Is Image Index
    CheckManifestType --> HandleImage: Is Image Manifest
    
    HandleIndex --> SelectPlatform: Select platform manifest
    SelectPlatform --> FetchPlatformManifest: Fetch specific manifest
    FetchPlatformManifest --> HandleImage: Continue with image manifest
    
    HandleImage --> DownloadConfig: Download config blob
    DownloadConfig --> DownloadLayers: Download layer blobs
    DownloadLayers --> BuildTar: Build Docker tar file
    BuildTar --> [*]: Return PullResult
    
    note right of ExchangeToken
        POST to realm URL
        with identity_token
        and scopes
    end note
    
    note right of StoreToken
        Token cached for
        subsequent requests
    end note
```

### Authentication Component Interaction

```mermaid
graph TB
    subgraph "Authentication Discovery"
        A[OciRegistryClient] -->|1. ping| B[RegistryPing]
        B -->|2. GET /v2/| C[Cloud Registry]
        C -->|3. 401 WWW-Authenticate| B
        B -->|4. parse| D[AuthChallenge]
        D -->|5. return| A
    end
    
    subgraph "Basic Authentication"
        A -->|6a. getBasicAuthHeader| E[AuthProvider]
        E -->|7a. getDockerAuthToken| F[Provider Registry]
        F -->|8a. return token| E
        E -->|9a. build header| A
        A -->|10a. Basic base64| C
    end
    
    subgraph "Bearer Authentication"
        A -->|6b. getIdentityToken| E
        E -->|7b. getIdentityToken| F
        F -->|8b. return OAuth2 token| E
        E -->|9b. return identityToken| A
        A -->|10b. exchange| G[BearerTokenExchange]
        G -->|11. POST /token| H[Token Server]
        H -->|12. Bearer Token| G
        G -->|13. return| A
        A -->|14. Bearer token| C
    end
    
    style D fill:#e1f5ff
    style G fill:#e1f5ff
    style E fill:#fff4e1
    style F fill:#ffe1e1
```

## Pull Flow Architecture

### Pull Image Flow States

```mermaid
stateDiagram-v2
    [*] --> ParseImageRef
    
    ParseImageRef --> ValidateInput: Validate imageRef
    ValidateInput --> GetRegistryEndpoint: Get provider endpoint
    
    GetRegistryEndpoint --> CreateAuthProvider: Create AuthProvider
    CreateAuthProvider --> CreateOciClient: Create OciRegistryClient
    
    CreateOciClient --> DiscoverAuth: Ping registry
    DiscoverAuth --> Authenticate: Get auth challenge
    
    Authenticate --> FetchManifest: Authenticated
    
    FetchManifest --> CheckManifestType: GET manifest
    
    CheckManifestType --> HandleImageIndex: Is Index
    CheckManifestType --> HandleImageManifest: Is Manifest
    
    HandleImageIndex --> SelectPlatform: Select platform
    SelectPlatform --> FetchPlatformManifest: Fetch platform manifest
    FetchPlatformManifest --> HandleImageManifest: Continue
    
    HandleImageManifest --> DownloadConfig: Download config
    DownloadConfig --> DownloadLayers: Download layers
    
    DownloadLayers --> CheckPullMode: All layers downloaded
    
    CheckPullMode --> BuildTarToFile: Mode = Path/File
    CheckPullMode --> BuildTarToStream: Mode = OutputStream
    CheckPullMode --> BuildTarLazy: Mode = InputStream
    
    BuildTarToFile --> AssembleTar: Build tar file
    BuildTarToStream --> AssembleTar: Build tar stream
    BuildTarLazy --> StartBackgroundThread: Start background thread
    StartBackgroundThread --> AssembleTar: Build tar in background
    
    AssembleTar --> CreatePullResult: Tar complete
    CreatePullResult --> [*]: Return PullResult
    
    note right of BuildTarLazy
        Uses PipedInputStream/
        PipedOutputStream
        for lazy loading
    end note
```

### Pull Modes Comparison

```mermaid
graph TB
    subgraph "Pull Modes"
        A[pullImage]
        
        A -->|Path| B[Path Mode<br/>Save to File System]
        A -->|File| C[File Mode<br/>Save to File]
        A -->|OutputStream| D[OutputStream Mode<br/>Write to Stream]
        A -->|No param| E[InputStream Mode<br/>Lazy Loading]
    end
    
    subgraph "Path/File Mode"
        B --> F[Immediate Download]
        C --> F
        F --> G[Build Tar to File]
        G --> H[Return PullResult<br/>with savedPath]
    end
    
    subgraph "OutputStream Mode"
        D --> I[Immediate Download]
        I --> J[Build Tar to Stream]
        J --> K[Return PullResult<br/>savedPath=null]
    end
    
    subgraph "InputStream Mode"
        E --> L[Create PipedStream]
        L --> M[Start Background Thread]
        M --> N[Return PullResult<br/>with InputStream]
        N --> O[Data streams on-demand<br/>as InputStream is read]
    end
    
    style E fill:#e1f5ff
    style O fill:#e1f5ff
```

## Deployment Architecture

### Runtime Architecture

```mermaid
graph TB
    subgraph "JVM Application"
        subgraph "Application Code"
            App[User Application]
        end
        
        subgraph "MultiCloudJ Registry"
            RC[RepositoryClient]
            AR[AbstractRegistry]
            ORC[OciRegistryClient]
        end
        
        subgraph "Provider Implementations"
            AWS_Impl[AwsRegistry]
            GCP_Impl[GcpRegistry]
            ALI_Impl[AliRegistry]
        end
        
        subgraph "Dependencies"
            HTTP_Client[HTTP Client<br/>OkHttp/HttpClient]
            Tar_Lib[Apache Commons Compress<br/>Tar Support]
            JSON_Lib[Gson<br/>JSON Parsing]
        end
    end
    
    subgraph "Cloud Infrastructure"
        subgraph "AWS"
            ECR_Service[AWS ECR Service]
            STS_Service[AWS STS Service]
        end
        
        subgraph "GCP"
            GAR_Service[GCP Artifact Registry]
            OAuth_Service[OAuth2 Token Server]
            ADC[Application Default Credentials]
        end
        
        subgraph "Alibaba"
            ACR_Service[Alibaba ACR Service]
            ACR_Auth[ACR Auth Service]
        end
    end
    
    App --> RC
    RC --> AR
    AR --> ORC
    AR --> AWS_Impl
    AR --> GCP_Impl
    AR --> ALI_Impl
    
    ORC --> HTTP_Client
    AR --> Tar_Lib
    ORC --> JSON_Lib
    
    AWS_Impl --> ECR_Service
    AWS_Impl --> STS_Service
    
    GCP_Impl --> GAR_Service
    GCP_Impl --> OAuth_Service
    GCP_Impl --> ADC
    
    ALI_Impl --> ACR_Service
    ALI_Impl --> ACR_Auth
    
    HTTP_Client --> ECR_Service
    HTTP_Client --> GAR_Service
    HTTP_Client --> ACR_Service
    HTTP_Client --> OAuth_Service
```

### Module Dependencies

```mermaid
graph LR
    subgraph "registry-client"
        RC[RepositoryClient]
        AR[AbstractRegistry]
        ORC[OciRegistryClient]
        DTB[DockerTarBuilder]
    end
    
    subgraph "registry-aws"
        AWS[AwsRegistry]
        AWS_DEP[AWS SDK]
    end
    
    subgraph "registry-gcp"
        GCP[GcpRegistry]
        GCP_DEP[GCP SDK]
    end
    
    subgraph "registry-ali"
        ALI[AliRegistry]
        ALI_DEP[Alibaba SDK]
    end
    
    subgraph "Common Dependencies"
        HTTP[HTTP Client]
        TAR[Tar Library]
        JSON[JSON Library]
        SL[ServiceLoader]
    end
    
    RC --> AR
    AR --> ORC
    AR --> DTB
    AR -.->|extends| AWS
    AR -.->|extends| GCP
    AR -.->|extends| ALI
    
    AWS --> AWS_DEP
    GCP --> GCP_DEP
    ALI --> ALI_DEP
    
    ORC --> HTTP
    ORC --> JSON
    DTB --> TAR
    AWS --> SL
    GCP --> SL
    ALI --> SL
```

## Summary

These diagrams illustrate:

1. **System Architecture**: High-level view of all components and their relationships
2. **Component Architecture**: Detailed component interactions
3. **Class Diagram**: Object-oriented design and inheritance hierarchy
4. **Data Flow**: Step-by-step flow of a pull operation
5. **Authentication Architecture**: Dynamic authentication discovery and token exchange
6. **Pull Flow Architecture**: Different pull modes and their execution paths
7. **Deployment Architecture**: Runtime deployment and module dependencies

All diagrams use Mermaid syntax and can be rendered in any Markdown viewer that supports Mermaid (GitHub, GitLab, VS Code, etc.).
