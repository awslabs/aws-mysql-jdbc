# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

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

[1.0.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/0.4.0...1.0.0
[0.4.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/0.3.0...0.4.0
[0.3.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/awslabs/aws-mysql-jdbc/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/awslabs/aws-mysql-jdbc/releases/tag/0.1.0
