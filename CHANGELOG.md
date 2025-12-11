# Changelog

## [0.2.19](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.18...multicloudj-v0.2.19) (2025-12-11)


### Blob Store

* Implement the DoesBucketExist for Blob ([#195](https://github.com/salesforce/multicloudj/issues/195)) ([a6ad1d7](https://github.com/salesforce/multicloudj/commit/a6ad1d79787e75ec35e6e95f9b72afa3c56b96bf))
* make blob store autocloseable throughout ([#194](https://github.com/salesforce/multicloudj/issues/194)) ([3610b57](https://github.com/salesforce/multicloudj/commit/3610b57bac3b9fe27fc1f92c97e19452c31d6a43))
* support copy from different source bucket ([#190](https://github.com/salesforce/multicloudj/issues/190)) ([40e6f4c](https://github.com/salesforce/multicloudj/commit/40e6f4c270dea745e36ae7e35cc7e04adb7d1d94))
* support tagging for multipart upload in AWS and GCP ([#172](https://github.com/salesforce/multicloudj/issues/172)) ([25b694b](https://github.com/salesforce/multicloudj/commit/25b694b041c4d845f6199daa541d637edae88e82))


### PubSub

* Enable implicit GetQueueUrl on initialization ([#183](https://github.com/salesforce/multicloudj/issues/183)) ([30af604](https://github.com/salesforce/multicloudj/commit/30af6040ee9dc2549f80b0627ee1097fe4017088))
* ensure atleast 1 message is pulled ([#197](https://github.com/salesforce/multicloudj/issues/197)) ([599a538](https://github.com/salesforce/multicloudj/commit/599a538eb9702be0872ea61cdf4240963bd6638b))

## [0.2.18](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.17...multicloudj-v0.2.18) (2025-12-02)


### Bug Fixes

* fix the release javadocs ([#179](https://github.com/salesforce/multicloudj/issues/179)) ([7ad2ad4](https://github.com/salesforce/multicloudj/commit/7ad2ad4166da79dbd98887a597905683e434229f))
* fix the release javadocs ([#182](https://github.com/salesforce/multicloudj/issues/182)) ([60d1d46](https://github.com/salesforce/multicloudj/commit/60d1d468d54143a0d46ddb13e7208875cfe3f30e))


### STS

* remove the scoped requirement for all credentials ([#177](https://github.com/salesforce/multicloudj/issues/177)) ([3dacf2f](https://github.com/salesforce/multicloudj/commit/3dacf2f287add77d97bf4e75f0a3764bcd17c7db))


### PubSub

* remove timeout while receiving from a subscription ([#180](https://github.com/salesforce/multicloudj/issues/180)) ([6c8b9dc](https://github.com/salesforce/multicloudj/commit/6c8b9dc609865fd71c5428eb1059201fc5981780))

## [0.2.17](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.16...multicloudj-v0.2.17) (2025-12-01)


### Bug Fixes

* delombok the javadocs generation before releasing docs ([#164](https://github.com/salesforce/multicloudj/issues/164)) ([d698004](https://github.com/salesforce/multicloudj/commit/d6980042fb54f1627523abf38bb975050af1ee80))
* fix the build for iam ([#161](https://github.com/salesforce/multicloudj/issues/161)) ([3f4c04c](https://github.com/salesforce/multicloudj/commit/3f4c04c16153abb8df6e859f92baea9cc89f7a32))


### Blob Store

* add bucket creation in the blobclient ([#168](https://github.com/salesforce/multicloudj/issues/168)) ([bd7dc35](https://github.com/salesforce/multicloudj/commit/bd7dc351390245c67de146f263f54d0afcd6ce09))


### PubSub

* Enable getAttributes in AWS  ([#171](https://github.com/salesforce/multicloudj/issues/171)) ([8226127](https://github.com/salesforce/multicloudj/commit/8226127814424524198ddf372af5508f3f78771c))
* Enable sendAck and sendNack apis for AWS Pubsub ([#134](https://github.com/salesforce/multicloudj/issues/134)) ([957db97](https://github.com/salesforce/multicloudj/commit/957db97f87f2f9cc32701f202fef6b5f34a86703))


### IAM

* implement IAM Identity Management APIs for GCP ([#142](https://github.com/salesforce/multicloudj/issues/142)) ([4bcd1d7](https://github.com/salesforce/multicloudj/commit/4bcd1d712cac1efdaa4fe22d267603b8b84c1cbe))
* implement policy related APIs(GCP) ([#141](https://github.com/salesforce/multicloudj/issues/141)) ([9f0d5a1](https://github.com/salesforce/multicloudj/commit/9f0d5a18e8069f35d07778c872db252be8ac394d))

## [0.2.16](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.15...multicloudj-v0.2.16) (2025-11-21)


### STS

* Make STS client anynomous for web identity ([#157](https://github.com/salesforce/multicloudj/issues/157)) ([a705153](https://github.com/salesforce/multicloudj/commit/a7051537ca4b91eb76ac1c531385b68b1423a8a4))

## [0.2.15](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.14...multicloudj-v0.2.15) (2025-11-19)


### STS

* enable web identity in aws sts and fix gcp get caller id ([#149](https://github.com/salesforce/multicloudj/issues/149)) ([fe1a50b](https://github.com/salesforce/multicloudj/commit/fe1a50bbb6d81cfcd629ceb2fface808dba6e752))


### PubSub

* fix client initialization in GCP ([#148](https://github.com/salesforce/multicloudj/issues/148)) ([6b08244](https://github.com/salesforce/multicloudj/commit/6b082442d20e49ebfc8f6714d95edec280ea0fb5))

## [0.2.14](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.13...multicloudj-v0.2.14) (2025-11-10)


### Blob Store

* fix the retry config for the bucketclient ([#135](https://github.com/salesforce/multicloudj/issues/135)) ([b3a72e4](https://github.com/salesforce/multicloudj/commit/b3a72e4178d092e257a5fa5d6580eb9beb72f715))

## [0.2.13](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.12...multicloudj-v0.2.13) (2025-11-06)


### Blob Store

* onboard retry config in the client and aws ([#113](https://github.com/salesforce/multicloudj/issues/113)) ([8df8d31](https://github.com/salesforce/multicloudj/commit/8df8d3169ea65fddb29e997b1b1d32a5c8c5c2d6))


### PubSub

* add send and receive apis for AWS SQS ([#125](https://github.com/salesforce/multicloudj/issues/125)) ([37f1e07](https://github.com/salesforce/multicloudj/commit/37f1e072b9d557b6903738a331e4b52be5c79713))


### Code Refactoring

* converge to a single patter for type safety ([#130](https://github.com/salesforce/multicloudj/issues/130)) ([3511698](https://github.com/salesforce/multicloudj/commit/351169864f0268c568150ce1d56dd35734ea5ba8))

## [0.2.12](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.11...multicloudj-v0.2.12) (2025-11-05)


### Blob Store

* Enable tags in the GcpTransformer ([#127](https://github.com/salesforce/multicloudj/issues/127)) ([728a632](https://github.com/salesforce/multicloudj/commit/728a6328721f9e0b146b29ac17b8c75ba8ccabd5))


### IAM

* driver layer contract for IAM ([#122](https://github.com/salesforce/multicloudj/issues/122)) ([c929930](https://github.com/salesforce/multicloudj/commit/c929930f041f7de4b2e8129d372be7beabeb5850))

## [0.2.11](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.10...multicloudj-v0.2.11) (2025-11-03)


### Blob Store

* fix GCP async client builder for configs ([#123](https://github.com/salesforce/multicloudj/issues/123)) ([088f0a2](https://github.com/salesforce/multicloudj/commit/088f0a2be5eb9b5165624167a653540dbcb8d80c))


### PubSub

* add getAttributes for gcp pubsub ([#120](https://github.com/salesforce/multicloudj/issues/120)) ([228ab6f](https://github.com/salesforce/multicloudj/commit/228ab6fda6f7ad7f963ef3c676cac513c4d62520))


### IAM

* onboarding client layer for IAM ([#90](https://github.com/salesforce/multicloudj/issues/90)) ([a57e09d](https://github.com/salesforce/multicloudj/commit/a57e09deb11eae6e0c3abe28a33f912729131d2e))

## [0.2.10](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.9...multicloudj-v0.2.10) (2025-10-30)


### Blob Store

* add getTag and setTag apis ([#117](https://github.com/salesforce/multicloudj/issues/117)) ([942347e](https://github.com/salesforce/multicloudj/commit/942347ef2ef428f0a19742078349b22df21cf6a9))
* Add SSE in multi-part upload ([#112](https://github.com/salesforce/multicloudj/issues/112)) ([32a920f](https://github.com/salesforce/multicloudj/commit/32a920fb6625cfdd30be6d4c9035429a0ebc2d0b))

## [0.2.9](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.8...multicloudj-v0.2.9) (2025-10-24)


### Document Store

* release please and fix the test ([#105](https://github.com/salesforce/multicloudj/issues/105)) ([d7458bd](https://github.com/salesforce/multicloudj/commit/d7458bd16fc9134a2faa6878d28716f66a3f2ea4))

## [0.2.8](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.7...multicloudj-v0.2.8) (2025-10-24)


### Bug Fixes

* correct changelog to show commits between releases ([#97](https://github.com/salesforce/multicloudj/issues/97)) ([f41d143](https://github.com/salesforce/multicloudj/commit/f41d1434b9f407487c4bd500973b72b9f8cf8275))


### Document Store

* test the release ([#101](https://github.com/salesforce/multicloudj/issues/101)) ([c94e18a](https://github.com/salesforce/multicloudj/commit/c94e18a270d80c44f4d53773ec9c6003d99ce2c5))
