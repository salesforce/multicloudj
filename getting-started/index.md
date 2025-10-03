---
layout: default
title: Getting Started
nav_order: 2
has_children: true
permalink: /getting-started/
---


## Quick Start
This tutorial provides a quick introduction to set up MultiCloudJ dependency and write a multi-cloud application with blobstore and docstore interface. 


### 1. Add maven Dependencies

Each service have a `$service-client` such as `blob-client` artifact as well `$service-$provider` artifact such `blob-aws`. 
The `$service-client` is MUST HAVE at the build time and your application code uses this to write the code.
The `$service-$provider` is required at the run time, and you can have dependencies for one or multiple providers at the same time. 
For example, you can have `blob-aws`, `blob-gcp` in your dependencies in your classpath at runtime 
and you can choose the provider during the client initialization.

Add MultiCloudJ blob and docstore dependencies to your Maven project:

```xml
<dependency>
    <groupId>com.salesforce.multicloudj</groupId>
    <artifactId>blob-client</artifactId>
    <version>0.2.2</version>
</dependency>
<dependency>
    <groupId>com.salesforce.multicloudj</groupId>
    <artifactId>blob-aws</artifactId>
    <version>0.2.2</version>
</dependency>
<dependency>
    <groupId>com.salesforce.multicloudj</groupId>
    <artifactId>blob-gcp</artifactId>
    <version>0.2.2</version>
</dependency>

```

Or for Gradle:

```gradle
implementation 'com.salesforce.multicloudj:blob-client:0.2.2'
implementation 'com.salesforce.multicloudj:blob-aws:0.2.2'
implementation 'com.salesforce.multicloudj:blob-gcp:0.2.2'
```

### 2. Set Up AWS Credentials

First, you need to configure AWS credentials on your machine. You have several options:

#### Option A: AWS CLI Configuration (Recommended)
Follow https://docs.aws.amazon.com/cli/v1/userguide/cli-chap-configure.html
To get started, You can have AWS credentials on environment variables or the ~/.aws/credentials file.


### 3. Create an S3 Bucket

You can create a bucket using the AWS Console, AWS CLI, or programmatically:

```bash
# Using AWS CLI
aws s3 mb s3://my-multicloudj-bucket --region us-west-2
```

### 4. Your First MultiCloudJ Application

Now let's create a simple application that connects to your bucket and performs basic operations:

```java
import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;

public class MultiCloudJQuickStart {
    public static void main(String[] args) {
        // Initialize a session to your S3 bucket
        BucketClient bucketClient = BucketClient.builder("aws")
            .withRegion("us-west-2")  // Replace with your region
            .withBucket("my-multicloudj-bucket")  // Replace with your bucket name
            .build();
        
        try {
            // Upload a blob
            System.out.println("Uploading blob...");
            UploadRequest uploadRequest = new UploadRequest("hello.txt");
            bucketClient.upload(uploadRequest, "Hello, MultiCloudJ!".getBytes());
            System.out.println("Upload successful!");
            
            // Download the blob
            System.out.println("Downloading blob...");
            DownloadRequest downloadRequest = new DownloadRequest("hello.txt");
            byte[] content = bucketClient.download(downloadRequest);
            System.out.println("Downloaded content: " + new String(content));
            
            // List all blobs in the bucket
            System.out.println("Listing blobs in bucket...");
            bucketClient.listBlobs().forEach(blob -> 
                System.out.println("Found blob: " + blob.getKey() + " (size: " + blob.getSize() + " bytes)")
            );
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
```

### 5. Run Your Application (If you have IDE that's better)

```bash
# Compile and run
javac -cp ".:multicloudj-blob-0.2.2.jar" MultiCloudJQuickStart.java
java -cp ".:multicloudj-blob-0.2.2.jar" MultiCloudJQuickStart
```

### 6. What Just Happened?

1. **BucketClient Creation**: We created a `BucketClient` instance that connects to your S3 bucket
2. **Upload**: We uploaded a simple text file with the content "Hello, MultiCloudJ!"
3. **Download**: We downloaded the same file and displayed its content
4. **List**: We listed all objects in your bucket to see what's there


## Next Steps

- **Explore Guides**: Check out our [detailed guides](guides/index.html) for specific use cases
- **API Reference**: Browse the complete [Java API documentation](api/java/latest/index.html)
- **Examples**: See working examples in our [examples repository](https://github.com/salesforce/multicloudj/tree/main/examples)
- **Design Decisions**: Understand the [architecture and design principles](design/index.html)

## Getting Help

- **GitHub Issues**: [Report bugs and request features](https://github.com/salesforce/multicloudj/issues)
- **Community**: [Join discussions](https://github.com/salesforce/multicloudj/issues)
- **Contributing**: [Learn how to contribute](https://github.com/salesforce/multicloudj/blob/main/CONTRIBUTING.md)

---

*MultiCloudJ is maintained by the Salesforce MultiCloudJ team and is open source under the Apache License 2.0.*

