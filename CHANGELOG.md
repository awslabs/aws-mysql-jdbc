# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [1.1.2] - 2022-11-22

### Added
* Upstream changes from MySQL 8.0.31 community driver.
* A new `enableFailoverStrictReader` parameter so the driver only reconnects to reader nodes after a failover.

### Changed
* Upgraded dependency versions

### Fixed
* Update FailoverConnectionPlugin license.
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

[1.1.2]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.1...1.1.2
[1.1.1]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.1.0...1.1.1
[1.1.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/1.0.0...1.1.0
[1.0.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/0.4.0...1.0.0
[0.4.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/0.3.0...0.4.0
[0.3.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/awslabs/aws-mysql-jdbc/releases/tag/0.1.0
