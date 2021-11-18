# Changelog

## [Version 0.3.0 (Public Preview)](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/0.3.0) - 2021-11-18

### Added
* AWS IAM Authentication is now supported. Usage instructions can be found [here](https://github.com/awslabs/aws-mysql-jdbc#aws-iam-database-authentication).

### Improvements
* Java 8 is now enforced in the build process.
* Resolved an issue for when connecting with an invalid connection after a valid connection. Users were able to connect with cached information after a valid connection despite providing invalid information.
* Bug fixes with flakey tests.

## [Version 0.2.0 (Public Preview)](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/0.2.0) - 2021-08-30

### Added
* Merged upstream changes from MySQL 8.0.23 community driver.

### Improvements
* `setAwsProtocolOnly` changed to be static method.
* Clarifications and improvements to README.md.

### Breaking Changes
* Potential Breaking Change: Loading of XML external entities are not loaded by default. Users must explictly allow it through using the new "allowXmlUnsafeExternalEntity" connection URL parameter. It is recommended that users verify external entities before loading them.

## [Version 0.1.0 (Public Preview)](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/0.1.0) - 2021-01-06

Based on the MySQL 8.0.21 community driver.

### Features
* The driver is cluster aware for Amazon Aurora MySQL. It takes advantage of Amazon Aurora's fast failover capabilities, reducing failover times from minutes to seconds.
