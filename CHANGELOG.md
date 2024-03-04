# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [1.1.14] - 2024-03-04
### Added
* Documentation regarding the differences between the AWS JDBC Driver and the AWS JDBC Driver for MySQL ([PR #554](https://github.com/awslabs/aws-mysql-jdbc/pull/554)).
* Support for China endpoint patterns ([PR #543](https://github.com/awslabs/aws-mysql-jdbc/pull/543)).

### Fixed
* Statement cancel waits for statement to finish before cancelling ([Issue #527](https://github.com/awslabs/aws-mysql-jdbc/issues/527)).

## [1.1.13] - 2024-01-19
### Changed
* Upstream changes from MySQL 8.0.32 and 8.0.33 community driver. The upstream 8.0.33 changes include a security fix for [CVE-2023-21971 - RCE and Unauthorized DB Access](https://github.com/awslabs/aws-mysql-jdbc/security/advisories/GHSA-vj9v-mpjv-qf74).

## [1.1.12] - 2023-12-21
### Added
* [**Experimental** Enhanced Host Monitoring Plugin v2](https://github.com/awslabs/aws-mysql-jdbc/blob/main/README.md#experimental-enhanced-failure-monitoring-plugin-v2), which is a redesign of the original Enhanced Host Monitoring Plugin and addresses memory leaks and high CPU usage during monitoring sessions ([PR #513](https://github.com/awslabs/aws-mysql-jdbc/pull/513)).

## [1.1.11] - 2023-11-02
### Added
* Documentation regarding known limitations with Amazon Aurora Global Databases ([PR #482](https://github.com/awslabs/aws-mysql-jdbc/pull/482)).

### Fixed
* Batch execute with in-line comments resulting in unexpected errors ([Issue #464](https://github.com/awslabs/aws-mysql-jdbc/issues/464)).
* Setting query timeouts while connecting to reader cluster endpoints resulting in errors ([PR #474](https://github.com/awslabs/aws-mysql-jdbc/pull/474)).
* Incorrectly generated keys after executing batch and individual statements ([Issue #484](https://github.com/awslabs/aws-mysql-jdbc/issues/484)).

## [1.1.10] - 2023-10-04
### Added
* Documentation regarding the `enableFailoverStrictReader` parameter ([Issue #456](https://github.com/awslabs/aws-mysql-jdbc/issues/456).

### Fixed
* Incorrect BatchUpdateException.getUpdateCounts() on failed batch statement execution ([Issue #450](https://github.com/awslabs/aws-mysql-jdbc/issues/450)).

## [1.1.9] - 2023-07-31
### Added
* Documentation:
  * An example of how to enable logging ([PR #431](https://github.com/awslabs/aws-mysql-jdbc/pull/431)).
  * Release policy ([PR #434](https://github.com/awslabs/aws-mysql-jdbc/pull/434)).
* The `keepSessionStateOnFailover` failover property to allow retaining the connection session state after failover without manually reconfiguring a connection ([Issue #425](https://github.com/awslabs/aws-mysql-jdbc/issues/425)).

### Fixed
* Avoid updating topology during global transactions which caused JTA transactions to fail ([Issue #292](https://github.com/awslabs/aws-mysql-jdbc/issues/292)).
* Keep `currentHostIndex` and hosts list in sync and pick a new connection when host role changes ([Issue #303](https://github.com/awslabs/aws-mysql-jdbc/issues/303)).
* Redundant reset statement called due to incorrect condition ([Issue #422](https://github.com/awslabs/aws-mysql-jdbc/pull/435)).

## [1.1.8] - 2023-06-28
### Fixed
* The topology service cache no longer stores connection specific properties so connections to the same cluster will not connect with the wrong properties ([Issue #407](https://github.com/awslabs/aws-mysql-jdbc/issues/407)).
* Fixed value `convertToNull` being rejected for property `zeroDateTimeBehavior` because of capitalization ([Issue #411](https://github.com/awslabs/aws-mysql-jdbc/pull/413)).
* Handle case in the `FailoverConnectionPlugin` where the `currentHostIndex` is equal to `NO_CONNECTION_INDEX` ([Issue #417](https://github.com/awslabs/aws-mysql-jdbc/issues/417)).

## [1.1.7] - 2023-05-11
### Changed
* Removed the `isMultiWriterCluster` flag as [multi-writer clusters are end of life since February 2023](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.MySQL56.EOL.html) ([PR #405](https://github.com/awslabs/aws-mysql-jdbc/pull/405)).

### Fixed
* Fixed methods passing proxy statement objects by adding checks to unwrap them before casting to a `ClientPreparedStatement` ([Issue #401](https://github.com/awslabs/aws-mysql-jdbc/issues/401)).

## [1.1.6] - 2023-04-28
### Changed

* Refactored resource shutdown for `DatabaseMetaDataUsingInfoSchema` and `CallableStatement` ([PR #388](https://github.com/awslabs/aws-mysql-jdbc/pull/388) & [PR #390](https://github.com/awslabs/aws-mysql-jdbc/pull/390)).
* Upgraded `com.github.vlsi.license-gather` from 1.77 to 1.87 and `junit-jupiter-engine` from 5.8.2 to 5.9.2 ([PR #378](https://github.com/awslabs/aws-mysql-jdbc/pull/378) & [PR #355](https://github.com/awslabs/aws-mysql-jdbc/pull/355)).
* Excluded `SampleApplication` from distribution jar ([PR #392](https://github.com/awslabs/aws-mysql-jdbc/pull/392)).
* Updated README with list of database engine versions tested against ([here](https://github.com/awslabs/aws-mysql-jdbc#aurora-engine-version-testing)).

### Fixed
* Fixed `AuroraTopologyService` to use fallback timestamp when the topology query results in an invalid timestamp during daylight savings ([Issue 386](https://github.com/awslabs/aws-mysql-jdbc/issues/386)).

## [1.1.5] - 2023-03-31
### Changed

* Optimized thread locks and expiring cache for the Enhanced Monitoring Plugin ([PR #356](https://github.com/awslabs/aws-mysql-jdbc/pull/356), [PR #373](https://github.com/awslabs/aws-mysql-jdbc/pull/373)).
* Checked log level to avoid unnecessary resource allocation ([PR #367](https://github.com/awslabs/aws-mysql-jdbc/pull/367)).
* Updated README.md with the following
  * `clusterInstanceHostPattern` failover parameter example ([here](https://github.com/awslabs/aws-mysql-jdbc#failover-parameters)).
  * Latest Maven Central URLs for [direct download of the jar file](https://github.com/awslabs/aws-mysql-jdbc#direct-download-of-the-jar-file), [using the driver as a Maven dependency](https://github.com/awslabs/aws-mysql-jdbc#as-a-maven-dependency), and [using the driver as a Gradle dependency](https://github.com/awslabs/aws-mysql-jdbc#as-a-gradle-dependency). 
  * DBeaver instructions on how to enable database selectors in the UI ([here](https://github.com/awslabs/aws-mysql-jdbc#using-the-driver-with-a-database-client-dbeaver)).
  * Clarified that the driver does not support custom RDS clusters ([here](https://github.com/awslabs/aws-mysql-jdbc#connection-url-descriptions)).
  * Clarified Amazon RDS Blue/Green Deployments are not supported ([here](https://github.com/awslabs/aws-mysql-jdbc#amazon-rds-bluegreen-deployments)).

### Fixed
* Only update topology for specific methods so that certain workflows are not interrupted ([Issue 363](https://github.com/awslabs/aws-mysql-jdbc/issues/363)).
* Fixed methods passing in JdbcConnection proxies by adding checks before casting to ConnectionImpl ([Issue 365](https://github.com/awslabs/aws-mysql-jdbc/issues/365)).
* Fixed AWSSecretsManagerPlugin to update ConnectionProxy HostInfo openInitialConnection to prevent access denied errors ([Issue 361](https://github.com/awslabs/aws-mysql-jdbc/issues/361)).

## [1.1.4] - 2023-01-27
### Fixed
* Close AWS Secrets Manager Client. This prevents the leaking `PoolingHttpClientConnectionManager` issue described in ([Issue #343](https://github.com/awslabs/aws-mysql-jdbc/issues/343)).
* Changed the criteria for updating topology and connection switching to not rely on `isFailoverEnabled()` since it may be using a stale topology ([PR #345](https://github.com/awslabs/aws-mysql-jdbc/pull/345)).
* Make EFM variables thread-safe ([PR #332](https://github.com/awslabs/aws-mysql-jdbc/pull/332)).

## [1.1.3] - 2023-01-05

### Fixed
* Removed "SNAPSHOT" from the full version name. [Issue 318](https://github.com/awslabs/aws-mysql-jdbc/issues/318)
* Fixed incorrect driver classpath in README.md
* Fixed failure detection interval in EFM

### Added
* Shading to -sources.jar to reflect the main jar which is shaded

## [1.1.2] - 2022-11-22

### Added
* Upstream changes from MySQL 8.0.31 community driver.
* A new `enableFailoverStrictReader` parameter so the driver only reconnects to reader nodes after a failover.

### Changed
* Upgraded dependency versions

### Fixed
* Updated FailoverConnectionPlugin license.
* Fixed plugin factories package names in the README.
* Resolved an issue where the driver attempts to query for the topology during a prepared transaction.[Issue 292](https://github.com/awslabs/aws-mysql-jdbc/issues/292)
* Resolved an issue where distinct ClientPreparedStatement objects incorrectly have the same hashCode.[Issue 308](https://github.com/awslabs/aws-mysql-jdbc/issues/308)
* Fixed the writer failover process where the driver reconnects to a reader node due to outdated topology information.
* Fixed some incorrect log messages.
* Enhanced logging to avoid unnecessary String format calls.

## [1.1.1] - 2022-09-22

### Changed
* Upgraded dependency versions

### Fixed
* Resolved an issue where failover was taking longer than expected when the driver was used with certain versions of HikariCP. [Issue 254](https://github.com/awslabs/aws-mysql-jdbc/issues/254).
* Resolved an issue where the EFM plugin occasionally threw a NullPointerException while stopping the monitor context. [Issue 209](https://github.com/awslabs/aws-mysql-jdbc/issues/209).
* Fixed a bug where the AWS Secrets Manager Plugin was not checking nested exceptions when determining if the exception was caused by an unsuccessful login attempt.
* Fixed a bug where failover could take up to two times the length of the failoverTimeoutMs connection property.
* Fixed an incorrect URL template for DBeaver in README.md.
* Fixed some incorrect log messages.

## [1.1.0] - 2022-06-29

### Added
* [Secrets Manager Support](https://github.com/awslabs/aws-mysql-jdbc#aws-secrets-manager-plugin).

### Changed
* Upgraded the driver to use AWS Java SDK v.2.17.165.

### Fixed
* Resolved an issue related to `abort`, `close`, and `isClosed` by filtering out methods that do not require failover. [Issue 206](https://github.com/awslabs/aws-mysql-jdbc/issues/206).
* Resolved an issue where `max_allowed_packet` on the server wasn't being respected by the driver. [Issue 191](https://github.com/awslabs/aws-mysql-jdbc/issues/191).
* Resolved a concurrency issue with the Aurora toplogy cache. [Issue 188](https://github.com/awslabs/aws-mysql-jdbc/discussions/188).
* Resolved an issue where non-network errors were not propagated during failover.

## [1.0.0] - 2022-03-01

### Added
* Upstream changes from MySQL 8.0.28 community driver.
* Hikari connection pool integration tests have been added.
* Docker containers are now created as part of testing and used to run both unit and integration tests.
* Failover performance metrics tracked based on a cluster. With options to enable additional performance metrics per instance.

### Changed
* Failover functionality refactored into a connection plugin.

### Fixed
* Enhanced Failure Monitoring connection status check.

## [0.4.0] - 2021-12-14

### Added
* Ability to execute additional or supplementary logic related to a `Connection` through Connection Plugin Manager. More details can be found [here](https://github.com/awslabs/aws-mysql-jdbc#connection-plugin-manager). Tutorial on writing custom connection plugin can be found [here](https://github.com/awslabs/aws-mysql-jdbc/tree/main/src/demo/java/customplugins).
* [Enhanced Failure Monitoring](https://github.com/awslabs/aws-mysql-jdbc/#enhanced-failure-monitoring) for improved failure detection.

### Changed
* NetworkFailuresFailoverIntegrationTest now uses environment variables to match FailoverIntegrationTest and ReplicationFailoverIntegrationTest.
* Updated all dependencies.
* Removed `jboss-as-connector` dependency for more up-to-date dependency `javassist`.

## [0.3.0] - 2021-11-18

### Added
* AWS IAM Authentication is now supported. Usage instructions can be found [here](https://github.com/awslabs/aws-mysql-jdbc#aws-iam-database-authentication).

### Fixed
* Enforce Java 8 in the build process.
* Resolved an issue for when connecting with an invalid connection after a valid connection. Users were able to connect with cached information after a valid connection despite providing invalid information.
* Flakey tests.

## [0.2.0] - 2021-08-30

### Added
* Upstream changes from MySQL 8.0.23 community driver.

### Changed
* Clarifications and improvements to README.md.
* Potential Breaking Change: Loading of XML external entities are not loaded by default. Users must explictly allow it through using the new "allowXmlUnsafeExternalEntity" connection URL parameter. It is recommended that users verify external entities before loading them.
* `setAwsProtocolOnly` changed to be static method.

## [0.1.0] - 2021-01-06

### Added
* This driver is based on the MySQL 8.0.21 community driver. The driver is cluster aware for Amazon Aurora MySQL. It takes advantage of Amazon Aurora's fast failover capabilities, reducing failover times from minutes to seconds.

[1.1.14]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.13...1.1.14
[1.1.13]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.12...1.1.13
[1.1.12]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.11...1.1.12
[1.1.11]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.10...1.1.11
[1.1.10]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.9...1.1.10
[1.1.9]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.8...1.1.9
[1.1.8]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.7...1.1.8
[1.1.7]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.6...1.1.7
[1.1.6]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.5...1.1.6
[1.1.5]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.4...1.1.5
[1.1.4]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.3...1.1.4
[1.1.3]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.2...1.1.3
[1.1.2]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.1...1.1.2
[1.1.1]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.0...1.1.1
[1.1.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.0.0...1.1.0
[1.0.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/0.4.0...1.0.0
[0.4.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/0.3.0...0.4.0
[0.3.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/awslabs/aws-mysql-jdbc/releases/tag/0.1.0
