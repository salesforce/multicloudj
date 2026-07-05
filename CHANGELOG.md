# Changelog

## [0.4.1](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.4.0...multicloudj-v0.4.1) (2026-07-04)


### Blob Store

* add MD5 as a caller-supplied checksum algorithm (PUT + presign) ([#505](https://github.com/salesforce/multicloudj/issues/505)) ([7501bae](https://github.com/salesforce/multicloudj/commit/7501bae6078f3142a6d312a63510f3bcbdacf6f3))
* add the total bytes request when transfer logging is disabled ([#504](https://github.com/salesforce/multicloudj/issues/504)) ([8431cc3](https://github.com/salesforce/multicloudj/commit/8431cc35d807aeac0591c27ab3ed0eba18d59925))
* consolidate duplicated Ali OSS client construction into OssClientFactory ([#510](https://github.com/salesforce/multicloudj/issues/510)) ([0bc975d](https://github.com/salesforce/multicloudj/commit/0bc975debd968e2b8a9684eebabed2a8316556aa))
* dedupe Ali OSS download paths and avoid byte[] double buffering ([#508](https://github.com/salesforce/multicloudj/issues/508)) ([b36980b](https://github.com/salesforce/multicloudj/commit/b36980b65af369ccf6f1eef59ff4964c8e39ee87))
* fix the content length handling in aws blob store ([#522](https://github.com/salesforce/multicloudj/issues/522)) ([fbb9036](https://github.com/salesforce/multicloudj/commit/fbb9036b9acb6aaf37a60d54075016406b29afd0))
* gcp: switch InputStream upload path to storage.createFrom ([#527](https://github.com/salesforce/multicloudj/issues/527)) ([9250585](https://github.com/salesforce/multicloudj/commit/925058561d3e2a1f9b598e49aae09e5452b46f5d))
* rename OSSCredentialsProvider to OssCredentialsProvider and remove redundant Ali smoke ITs ([#509](https://github.com/salesforce/multicloudj/issues/509)) ([34d8de4](https://github.com/salesforce/multicloudj/commit/34d8de48d5ff09f3c85870a877f2c74e78df6ac3))


### STS

* implement AssumeRoleWithWebIdentity (OIDC) for Alibaba ([#521](https://github.com/salesforce/multicloudj/issues/521)) ([753138f](https://github.com/salesforce/multicloudj/commit/753138f3360e9eeeb89283ee56e1b31f66236a48))

## [0.4.0](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.3.6...multicloudj-v0.4.0) (2026-06-19)


### Features

* onboard retryable hints in exceptions ([#490](https://github.com/salesforce/multicloudj/issues/490)) ([97df2c0](https://github.com/salesforce/multicloudj/commit/97df2c081048d756214d5fac761a6b95cf0511fd))


### Blob Store

* add prefix-scope validation for bucket existence ([#470](https://github.com/salesforce/multicloudj/issues/470)) ([e64e363](https://github.com/salesforce/multicloudj/commit/e64e363e7f30d0480cf4e84040186eadbc2ddd06))
* add transfer status logging for Ali OSS async directory ops ([#473](https://github.com/salesforce/multicloudj/issues/473)) ([9af963f](https://github.com/salesforce/multicloudj/commit/9af963f15653d23dc7f1a298df29b1419d1ee815))
* add WireMock recordings for Ali presign v2 conformance tests ([#497](https://github.com/salesforce/multicloudj/issues/497)) ([6f84aef](https://github.com/salesforce/multicloudj/commit/6f84aefc4df308564fea987e27268ce08552c6a1))
* enable Ali OSS multipart-upload helper tier + SSE-KMS conformance tests ([#494](https://github.com/salesforce/multicloudj/issues/494)) ([9a1ba47](https://github.com/salesforce/multicloudj/commit/9a1ba47f884efe76bbeded891e3f4f7efb21b1a9))
* enable multipart upload conformance tests for Ali ([#492](https://github.com/salesforce/multicloudj/issues/492)) ([061a68f](https://github.com/salesforce/multicloudj/commit/061a68f33eea9c518742eefa596b45660d58518a))
* enable presign v2 positive conformance tests for Ali ([#495](https://github.com/salesforce/multicloudj/issues/495)) ([e03b5c9](https://github.com/salesforce/multicloudj/commit/e03b5c9433b43f4431df15bb9d020aac575ffc67))
* enable testCopy and testCopyFrom conformance tests for Ali ([#491](https://github.com/salesforce/multicloudj/issues/491)) ([1dd29c2](https://github.com/salesforce/multicloudj/commit/1dd29c2880739ed1ac065bcd5d75e2ab112924d5))
* gate Ali directory conformance tests on the capability flag ([#493](https://github.com/salesforce/multicloudj/issues/493)) ([b2ae6dd](https://github.com/salesforce/multicloudj/commit/b2ae6dd38e50a37edcf41f39fa7985143e921dbe))
* honor contentDisposition on Ali OSS presigned download URLs ([#484](https://github.com/salesforce/multicloudj/issues/484)) ([5afc04a](https://github.com/salesforce/multicloudj/commit/5afc04a6afedc9d30b59cc6d5348fb5ebc5d2bc8))
* honor useKmsManagedKey on Ali OSS upload ([#485](https://github.com/salesforce/multicloudj/issues/485)) ([9cdd8df](https://github.com/salesforce/multicloudj/commit/9cdd8dfa2b0d80db8431e77071a2b6d28d5dd8ad))
* implement checkArchived for Ali OSS (delete-marker detection) ([#480](https://github.com/salesforce/multicloudj/issues/480)) ([aaf4c0a](https://github.com/salesforce/multicloudj/commit/aaf4c0a92e54531f7dd8b7ea6ba293aa244640f8))
* populate lastModified timestamp in BlobInfo for Ali list API ([#474](https://github.com/salesforce/multicloudj/issues/474)) ([1d87e44](https://github.com/salesforce/multicloudj/commit/1d87e440019858c11d479652e51d3e78f923aebb))
* populate objectLockInfo in BlobMetadata for Ali ([#481](https://github.com/salesforce/multicloudj/issues/481)) ([693f703](https://github.com/salesforce/multicloudj/commit/693f703b9e330ca34c9bfa6a3e77fbec4449ab48))
* presigned URL v2 — upload constraint binding + signed headers ([#468](https://github.com/salesforce/multicloudj/issues/468)) ([a3bcaf2](https://github.com/salesforce/multicloudj/commit/a3bcaf2b328019fcb62cdd4e5d332abcf60ccb4a))
* replace inline fully-qualified class names with imports in Ali blob tests ([#498](https://github.com/salesforce/multicloudj/issues/498)) ([d0d0248](https://github.com/salesforce/multicloudj/commit/d0d0248e4f72b5502169f7cf0ee721bf2372a0c0))
* surface composite CRC64 checksum on Ali multipart upload completion ([#482](https://github.com/salesforce/multicloudj/issues/482)) ([f5ee081](https://github.com/salesforce/multicloudj/commit/f5ee08197d0f274614c05c8d0a5e16df2f933897))
* throw typed exception for unsupported Ali object lock retention mode upgrade ([#479](https://github.com/salesforce/multicloudj/issues/479)) ([d0d7434](https://github.com/salesforce/multicloudj/commit/d0d743464f4c10bab99ad7f5cb101a96f00baada))
* update the netty dependencies ([#499](https://github.com/salesforce/multicloudj/issues/499)) ([8ea95d6](https://github.com/salesforce/multicloudj/commit/8ea95d624816e865c4275b2e96ddb4705a5c6821))
* wire maxConnections and idleConnectionTimeout into the Ali OSS client ([#496](https://github.com/salesforce/multicloudj/issues/496)) ([b3deb52](https://github.com/salesforce/multicloudj/commit/b3deb526c73977749713ebc951492aea854e85cd))
* wire socketTimeout into the Ali OSS sync client builder ([#486](https://github.com/salesforce/multicloudj/issues/486)) ([7c96f24](https://github.com/salesforce/multicloudj/commit/7c96f243c0c593b7a604e2ad66053bd6ec3cdd80))

## [0.3.6](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.3.5...multicloudj-v0.3.6) (2026-06-05)


### Blob Store

* add async blob store skeleton for Ali OSS ([#445](https://github.com/salesforce/multicloudj/issues/445)) ([53cce24](https://github.com/salesforce/multicloudj/commit/53cce24c8826b7487924c8f5df5f4bc8ffe10a2f))
* add ListObjectVersions API ([#418](https://github.com/salesforce/multicloudj/issues/418)) ([e72e3da](https://github.com/salesforce/multicloudj/commit/e72e3dae12fa45a3fb73bb6b57412b59b8bb5042))
* change the ListBlobVersions signature to long term with ListBlobVersionRequest as param ([#456](https://github.com/salesforce/multicloudj/issues/456)) ([ed9003e](https://github.com/salesforce/multicloudj/commit/ed9003e86d2d148ce7d163ce9d4dde4bd678241a))
* Enable object lock for inmemory store ([#466](https://github.com/salesforce/multicloudj/issues/466)) ([ba67974](https://github.com/salesforce/multicloudj/commit/ba67974083354e7978bc8f414c5e8e3a33253d8e))
* Enhance conformance test for blobstore directory operations ([#449](https://github.com/salesforce/multicloudj/issues/449)) ([d9b4718](https://github.com/salesforce/multicloudj/commit/d9b471896dac331b8a7082eecf4b0b96b7f7f1c9))
* fix GCP large upload truncation for byte[] uploads ([#450](https://github.com/salesforce/multicloudj/issues/450)) ([6069776](https://github.com/salesforce/multicloudj/commit/6069776f4ebf8e2842adef0db88b068a37587e94))
* implement async delete and copy operations for Ali OSS ([#451](https://github.com/salesforce/multicloudj/issues/451)) ([e167a74](https://github.com/salesforce/multicloudj/commit/e167a74a7455b79c5cde4fcc5e2ab237d3d7962f))
* implement async directory operations for Ali OSS ([#460](https://github.com/salesforce/multicloudj/issues/460)) ([bc4600b](https://github.com/salesforce/multicloudj/commit/bc4600bcd6ed00400065b7b8bbacb1057e76e77d))
* implement async list/listPage operation for Ali OSS ([#454](https://github.com/salesforce/multicloudj/issues/454)) ([c791bac](https://github.com/salesforce/multicloudj/commit/c791bac2c977d29c5c728c8b7e2bde1bdaf1ba0a))
* implement async metadata operations for Ali OSS ([#452](https://github.com/salesforce/multicloudj/issues/452)) ([b1170a4](https://github.com/salesforce/multicloudj/commit/b1170a4bce4d09621f67d555988733f8e78236da))
* implement async multipart upload operation for Ali OSS ([#457](https://github.com/salesforce/multicloudj/issues/457)) ([c5e55fb](https://github.com/salesforce/multicloudj/commit/c5e55fbc7ae1c8f596f8c55f6fd3824b51c1c024))
* implement async tags and presign URL operation for Ali OSS ([#459](https://github.com/salesforce/multicloudj/issues/459)) ([b47f103](https://github.com/salesforce/multicloudj/commit/b47f103e52822248b4b00c406258c12a84dfc764))
* implement async upload and download operations for Ali OSS ([#447](https://github.com/salesforce/multicloudj/issues/447)) ([893cc37](https://github.com/salesforce/multicloudj/commit/893cc379534b58b7efa9967b14994df4f0a8d86a))
* implement listBlobVersions for Ali OSS using v2 SDK paginator ([#453](https://github.com/salesforce/multicloudj/issues/453)) ([3b0b630](https://github.com/salesforce/multicloudj/commit/3b0b630ba039302c84f395530971adb2413445e8))
* implement object lock/retention for Ali OSS ([#471](https://github.com/salesforce/multicloudj/issues/471)) ([fe62077](https://github.com/salesforce/multicloudj/commit/fe6207748d700fda45c4190894311e33d84dbe4c))
* implement retry configuration for Ali OSS sync and async clients ([#462](https://github.com/salesforce/multicloudj/issues/462)) ([2189864](https://github.com/salesforce/multicloudj/commit/218986401863a111872eea893292ecba27f545cd))
* migrate Ali blobstore implementation to OSS SDK v2 ([#436](https://github.com/salesforce/multicloudj/issues/436)) ([cbd2f32](https://github.com/salesforce/multicloudj/commit/cbd2f32a73a9da768af3f1a84eac34b8e1eafb92))


### Document Store

* add 4 new benchmarks + expand HighScore dataset ([#443](https://github.com/salesforce/multicloudj/issues/443)) ([08ad2a7](https://github.com/salesforce/multicloudj/commit/08ad2a70dfa68a71d288e49c8825ef4db7d0b039))
* clean up benchmark suite to spike's final 14 benchmarks ([#448](https://github.com/salesforce/multicloudj/issues/448)) ([6d3924a](https://github.com/salesforce/multicloudj/commit/6d3924a149c31a28e2758997cf6652ae39bd222c))
* fix critical bugs in benchmark harness ([#432](https://github.com/salesforce/multicloudj/issues/432)) ([cc0936b](https://github.com/salesforce/multicloudj/commit/cc0936bcd05c4820eaee4803e9a9a5d29556e2eb))


### STS

* add objectListPrefix support to GcpSts CAB expression ([#437](https://github.com/salesforce/multicloudj/issues/437)) ([1114f2a](https://github.com/salesforce/multicloudj/commit/1114f2ac223f2051e95e65e14bdebb56e7c511c6))
* onboard proxy configurations for sts interface ([#467](https://github.com/salesforce/multicloudj/issues/467)) ([314e52a](https://github.com/salesforce/multicloudj/commit/314e52a5932c57b1d08dd2f9391732af5255660a))
* remove weak coverage tests sts test files ([#469](https://github.com/salesforce/multicloudj/issues/469)) ([96f40cd](https://github.com/salesforce/multicloudj/commit/96f40cd419c225af1d4a9a47316b69e3e202044a))

## [0.3.5](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.3.4...multicloudj-v0.3.5) (2026-05-21)


### Blob Store

* fix the checksum crc32c validation with negative case with… ([#433](https://github.com/salesforce/multicloudj/issues/433)) ([228ec40](https://github.com/salesforce/multicloudj/commit/228ec40defe0a3c24cc3b67b6202edf43b36216f))
* onboard conformance test for Ali ([#425](https://github.com/salesforce/multicloudj/issues/425)) ([16268d7](https://github.com/salesforce/multicloudj/commit/16268d79507c41e64b16b19f1c8a984abf610ff2))

## [0.3.4](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.3.3...multicloudj-v0.3.4) (2026-05-15)


### Blob Store

* add object lock support to multipart uploads ([#412](https://github.com/salesforce/multicloudj/issues/412)) ([b0d51ad](https://github.com/salesforce/multicloudj/commit/b0d51ad007e0ac5010b899d96465bfce1a2fa058))
* Fix GcpTransformer.toBlobInfo(UploadRequest) so that UploadRequest.getChecksumValue() is forwarded to BlobInfo.setCrc32c(...) ([#419](https://github.com/salesforce/multicloudj/issues/419)) ([1072d63](https://github.com/salesforce/multicloudj/commit/1072d630e73f507c448c202292e991234fb39316))
* fix testInvalidCredentials conformance test for GCP ([#407](https://github.com/salesforce/multicloudj/issues/407)) ([4f9c835](https://github.com/salesforce/multicloudj/commit/4f9c835642d5e10905438be59748c57195cbc04e))
* GCP and AWS directory upload object lock ([#410](https://github.com/salesforce/multicloudj/issues/410)) ([2be326e](https://github.com/salesforce/multicloudj/commit/2be326e51e61993204028af7bcbd0df9936007cf))
* onboard handling for archived objects in download for aws/gcp ([#411](https://github.com/salesforce/multicloudj/issues/411)) ([1076088](https://github.com/salesforce/multicloudj/commit/107608827c1b3ce25e82432ce07de2f42b93bf9b))
* onboard perf configs on GCP ([#416](https://github.com/salesforce/multicloudj/issues/416)) ([6b928e9](https://github.com/salesforce/multicloudj/commit/6b928e904e17c15a353ff7acddc1c7810b495884))
* Onboard the logging and tracing in blobstore for all AWS/GCP ([#408](https://github.com/salesforce/multicloudj/issues/408)) ([db634b0](https://github.com/salesforce/multicloudj/commit/db634b038f10804615c5a7cb21a21ae6f95cabb3))
* support retention mode in updateObjectRetention for AWS & GCP ([#417](https://github.com/salesforce/multicloudj/issues/417)) ([48df8d6](https://github.com/salesforce/multicloudj/commit/48df8d618998d1caba724ecc46053bd2f205d270))


### Document Store

* Update the documentation for docstore query ([#413](https://github.com/salesforce/multicloudj/issues/413)) ([542fb1d](https://github.com/salesforce/multicloudj/commit/542fb1d2f4522ebba4ed9bc867eeb3953853bcf9))

## [0.3.3](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.3.2...multicloudj-v0.3.3) (2026-05-01)


### Blob Store

* addition of createdTime in the object metadata for all the… ([#400](https://github.com/salesforce/multicloudj/issues/400)) ([a284e81](https://github.com/salesforce/multicloudj/commit/a284e814432ffdfb3812ec15dbb76fd8b88a0e48))
* support parallel download option and createParentPath option in AWS and GCP ([#377](https://github.com/salesforce/multicloudj/issues/377)) ([da86573](https://github.com/salesforce/multicloudj/commit/da86573679517e667c09eed354884cd28e4980ff))

## [0.3.2](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.3.1...multicloudj-v0.3.2) (2026-04-28)


### Blob Store

* add an option to supply quota project id in bucket client ([#401](https://github.com/salesforce/multicloudj/issues/401)) ([98a6d91](https://github.com/salesforce/multicloudj/commit/98a6d915af2034eb3f6b26638049ff605f68b1b4))


### PubSub

* add nack visibility timeout ([#388](https://github.com/salesforce/multicloudj/issues/388)) ([ca3ce0e](https://github.com/salesforce/multicloudj/commit/ca3ce0e5054fcfb63d996a41da3092bdef42bdbe))

## [0.3.1](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.3.0...multicloudj-v0.3.1) (2026-04-16)


### Blob Store

* add contentType support for MPU and optimize wiremock recordings     ([#378](https://github.com/salesforce/multicloudj/issues/378)) ([3f1945c](https://github.com/salesforce/multicloudj/commit/3f1945c13e725087ae757ef0b21f9c187cc4e8a2))
* checksum support with sha256 ([#368](https://github.com/salesforce/multicloudj/issues/368)) ([1690f20](https://github.com/salesforce/multicloudj/commit/1690f20ba6869bf14d2fa063bd1490d3154c3cc6))
* fix GCP sync list api to return only blobs ([#354](https://github.com/salesforce/multicloudj/issues/354)) ([0cd097f](https://github.com/salesforce/multicloudj/commit/0cd097f0718e6943fdc93b9c865b085c56ec8528))
* LoggingTransferListener for AWS ([#367](https://github.com/salesforce/multicloudj/issues/367)) ([9d6bdaa](https://github.com/salesforce/multicloudj/commit/9d6bdaa9e24ec9922bc8d3452daf1a59e58b5bfa))
* make ListBlobsPageResponse constructor backward compatible ([#376](https://github.com/salesforce/multicloudj/issues/376)) ([b11491d](https://github.com/salesforce/multicloudj/commit/b11491dc5e326f3adbc0688ee8f5368f1ebcddf6))
* Onboard CommonPrefix support in listPage API ([#369](https://github.com/salesforce/multicloudj/issues/369)) ([f81e955](https://github.com/salesforce/multicloudj/commit/f81e955cacbc4f25606c1c38cfd297e3ca9baf21))
* onboarding the aws basic credentials in aws creds overider ([#384](https://github.com/salesforce/multicloudj/issues/384)) ([015ccdf](https://github.com/salesforce/multicloudj/commit/015ccdfd4576db1a14d89e0f60b45ac81767e84d))
* support isObjectLockSupported in in memory blobstore ([#380](https://github.com/salesforce/multicloudj/issues/380)) ([f2738a2](https://github.com/salesforce/multicloudj/commit/f2738a2249c5eb7bbcebe720a3dfc1fd260b597e))

## [0.3.0](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.28...multicloudj-v0.3.0) (2026-03-27)


### Features

* add Jekyll build step to generate-site.sh ([#350](https://github.com/salesforce/multicloudj/issues/350)) ([dc22fe0](https://github.com/salesforce/multicloudj/commit/dc22fe018879d6c9d6f4c8caa3db513921055b55))


### Blob Store

* onboard content-type in upload for all clouds ([#346](https://github.com/salesforce/multicloudj/issues/346)) ([f9d8080](https://github.com/salesforce/multicloudj/commit/f9d8080866885edc464b1f1e4cccc65fbb869132))

## [0.2.28](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.27...multicloudj-v0.2.28) (2026-03-17)


### Blob Store

* checksum support for mpu ([#332](https://github.com/salesforce/multicloudj/issues/332)) ([5a56444](https://github.com/salesforce/multicloudj/commit/5a564449035f890c2446f5cea204d8719eb525fe))
* Fix the doGetMetadata blob existence exception ([#341](https://github.com/salesforce/multicloudj/issues/341)) ([fc428ad](https://github.com/salesforce/multicloudj/commit/fc428add5b2babcdd36257dbf690117770b64710))
* Object lock conformance ([#336](https://github.com/salesforce/multicloudj/issues/336)) ([5a0fb4a](https://github.com/salesforce/multicloudj/commit/5a0fb4a9ab6a5b8ce9f92d50617f47d17f093969))


### Document Store

* fix the inequality filter scenario in pagination token ([#340](https://github.com/salesforce/multicloudj/issues/340)) ([f97b58b](https://github.com/salesforce/multicloudj/commit/f97b58b9decbb239b170093043168e32ef03d41c))
* support decoding byte[] ([#338](https://github.com/salesforce/multicloudj/issues/338)) ([fb1c6f7](https://github.com/salesforce/multicloudj/commit/fb1c6f79378140dfca992cf79b004f79f6360aa7))


### PubSub

* add the no-arg constructor ([#339](https://github.com/salesforce/multicloudj/issues/339)) ([bdba4b4](https://github.com/salesforce/multicloudj/commit/bdba4b43aadf4c50c5416c015e3a567e12e3438e))


### IAM

* implement substrate neutral policy document model ([#327](https://github.com/salesforce/multicloudj/issues/327)) ([03f6a8c](https://github.com/salesforce/multicloudj/commit/03f6a8ca172c72b8af37222db26b699ca8478db2))

## [0.2.27](https://github.com/salesforce/multicloudj/compare/multicloudj-v0.2.26...multicloudj-v0.2.27) (2026-03-04)


### Blob Store

* extend retry strategy to GCP ([#298](https://github.com/salesforce/multicloudj/issues/298)) ([a22c4b0](https://github.com/salesforce/multicloudj/commit/a22c4b063c7fc84978891a1d89c4d670fb1123d5))
* override proxy configs ([#318](https://github.com/salesforce/multicloudj/issues/318)) ([d148a85](https://github.com/salesforce/multicloudj/commit/d148a85922ce15356d40d7d537f57dfd5e87bf3c))


### IAM

* fix bugs in AWS IAM ([#311](https://github.com/salesforce/multicloudj/issues/311)) ([e741a87](https://github.com/salesforce/multicloudj/commit/e741a87378b1ff059633b5ad548a4004a5d7a781))

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
