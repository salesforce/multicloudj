# Changelog

## [0.2.26](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.25...multicloudj-v0.2.26) (2026-02-25)


### Blob Store

* change validate bucket exists method ([#305](https://github.com/salesforce/multicloudj/issues/305)) ([720fbf6](https://github.com/salesforce/multicloudj/commit/720fbf6d78d2791c7ec0c2ab4fe62bf55fd306c4))


### IAM

* implement AWS policy APIs and conformance tests ([#279](https://github.com/salesforce/multicloudj/issues/279)) ([34fceac](https://github.com/salesforce/multicloudj/commit/34fceac6425de3db9cd94ebb3a8016962f5cf466))
* Onboard examples for IAM APIs  ([#277](https://github.com/salesforce/multicloudj/issues/277)) ([cd82264](https://github.com/salesforce/multicloudj/commit/cd822647c4a863192b9415f418288951f8720c09))


### DB Backup Restore

* add examples ([#301](https://github.com/salesforce/multicloudj/issues/301)) ([7d3c34b](https://github.com/salesforce/multicloudj/commit/7d3c34b14e594a807283c58b8932074e7c908f94))
* add KMS encryption key and status message support to database restore ([#303](https://github.com/salesforce/multicloudj/issues/303)) ([07fcf88](https://github.com/salesforce/multicloudj/commit/07fcf8809081cb9df3e3476069b05cc2bc49c9d8))

## [0.2.25](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.24...multicloudj-v0.2.25) (2026-02-05)


### Blob Store

* Expose builder option useKmsManagedKey ([#281](https://github.com/salesforce/multicloudj/issues/281)) ([fafd802](https://github.com/salesforce/multicloudj/commit/fafd8025baf759740ffa13aa8dba1bfd2dfa92e4))
* handle the cases for network timeouts, connection timeouts, dns resolution errors ([#273](https://github.com/salesforce/multicloudj/issues/273)) ([7a7e55b](https://github.com/salesforce/multicloudj/commit/7a7e55baa48bc64840c78395975c63d192c83866))
* in memory blobstore for local testing ([#267](https://github.com/salesforce/multicloudj/issues/267)) ([0391e3a](https://github.com/salesforce/multicloudj/commit/0391e3a8c6c3e5d51fb4fe7be8bd96a55ac19398))


### STS

* fix the aws web identity token provider overrider ([#283](https://github.com/salesforce/multicloudj/issues/283)) ([4146d97](https://github.com/salesforce/multicloudj/commit/4146d97c77f236e04fe8b2f192ae113a1fe07123))


### PubSub

* Add SNS publish API ([#226](https://github.com/salesforce/multicloudj/issues/226)) ([f157be9](https://github.com/salesforce/multicloudj/commit/f157be93ab45f6172ae96a1233bd68350b440d7a))


### IAM

* added iam conformance tests for AWS ([#234](https://github.com/salesforce/multicloudj/issues/234)) ([463c43b](https://github.com/salesforce/multicloudj/commit/463c43bb348ef6c30e9caa1ffac603092f3bd901))
* implement AWS IAM policy APIs ([#259](https://github.com/salesforce/multicloudj/issues/259)) ([6a6b4f3](https://github.com/salesforce/multicloudj/commit/6a6b4f324cc67a09e36ce39b2f8aa68731a52f09))

## [0.2.24](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.23...multicloudj-v0.2.24) (2026-01-22)


### Blob Store

* handle the gcp blobstore endpoint with empty path / ([#263](https://github.com/salesforce/multicloudj/issues/263)) ([b1a69ec](https://github.com/salesforce/multicloudj/commit/b1a69ec60aa3a84dff42128db4d84b3044a080f8))


### IAM

* reuse single ObjectMapper instance to reduce overhead in GCP IAM ([#261](https://github.com/salesforce/multicloudj/issues/261)) ([c6ea753](https://github.com/salesforce/multicloudj/commit/c6ea753f0493eb8236200e9c640708589cb60f2c))

## [0.2.23](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.22...multicloudj-v0.2.23) (2026-01-16)


### Blob Store

* add checksum support in putobject ([#250](https://github.com/salesforce/multicloudj/issues/250)) ([7ed72c7](https://github.com/salesforce/multicloudj/commit/7ed72c70f46c10de5dcc012b95974e3e8e98ec7d))
* Support objectLock in GCP and AWS ([#242](https://github.com/salesforce/multicloudj/issues/242)) ([f7022e7](https://github.com/salesforce/multicloudj/commit/f7022e72a86547c42a5bfb556a59919954383832))


### STS

* support web identity tokens in gcp sts ([#249](https://github.com/salesforce/multicloudj/issues/249)) ([abe66c5](https://github.com/salesforce/multicloudj/commit/abe66c5e227929fa84e0760408185276a7878ef7))


### PubSub

* expose raw message delivery option for client ([#238](https://github.com/salesforce/multicloudj/issues/238)) ([681dcd0](https://github.com/salesforce/multicloudj/commit/681dcd09db18db96f85454d78be9ebf89f393c5c))


### IAM

* implement getInlinePolicyDetails API for AWS Substrate ([#233](https://github.com/salesforce/multicloudj/issues/233)) ([a591aeb](https://github.com/salesforce/multicloudj/commit/a591aeb0ddb7f58adc09564c2072edae1efd6200))
* implement identity APIs for AWS substrate ([#225](https://github.com/salesforce/multicloudj/issues/225)) ([470fb75](https://github.com/salesforce/multicloudj/commit/470fb757a966d5008aeb956805d5e4ca3c931472))

## [0.2.22](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.21...multicloudj-v0.2.22) (2026-01-10)


### STS

* support access boundary in aws and gcp sts  ([#240](https://github.com/salesforce/multicloudj/issues/240)) ([8157a56](https://github.com/salesforce/multicloudj/commit/8157a56fa8364457994183b3eebecae82c973460))


### PubSub

* support parsing sns messages in SQS subscription ([#231](https://github.com/salesforce/multicloudj/issues/231)) ([e355c22](https://github.com/salesforce/multicloudj/commit/e355c22308e932c8688bfd27a0fcbd81b470f9af))

## [0.2.21](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.20...multicloudj-v0.2.21) (2025-12-19)


### Blob Store

* onboard GCS native Multipart Upload ([#228](https://github.com/salesforce/multicloudj/issues/228)) ([8193069](https://github.com/salesforce/multicloudj/commit/819306958fb32d5296b5a44b3aef78e21aab6454))
* Upgrade the google-cloud-storage version to v2.60.0 ([#211](https://github.com/salesforce/multicloudj/issues/211)) ([fcb0943](https://github.com/salesforce/multicloudj/commit/fcb09437f87fb95bd09fad7e89ebaf7b819397c3))


### IAM

* add conformance tests for IAM Identity Management APIs for GCP ([#186](https://github.com/salesforce/multicloudj/issues/186)) ([3a5f83c](https://github.com/salesforce/multicloudj/commit/3a5f83cbd4d9740be77417b3da460109a1d12872))
* getInlinePolicyDetails API(GCP) ([#163](https://github.com/salesforce/multicloudj/issues/163)) ([5472da6](https://github.com/salesforce/multicloudj/commit/5472da6ce2ea0fb6d2cc6b7f2574619726e51d9e))

## [0.2.20](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.19...multicloudj-v0.2.20) (2025-12-15)


### Blob Store

* Enable timestamp data in BlobInfo in List api ([#201](https://github.com/salesforce/multicloudj/issues/201)) ([ca4c96a](https://github.com/salesforce/multicloudj/commit/ca4c96a6daa1eee8ff7be476cc5cac8a68ec261a))

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
