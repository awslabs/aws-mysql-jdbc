# Release Schedule and Maintenance Policy
| Release Date      | Release                                                                         |
|-------------------|---------------------------------------------------------------------------------|
| Mar 1, 2022       | [Release 1.0.0](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.0.0)   |  
| June 29, 2022     | [Release 1.1.0](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.0)   | 
| Sept 22, 2022     | [Release 1.1.1](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.1)   |
| Nov 22, 2022      | [Release 1.1.2](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.2)   |  
| Jan 5, 2023       | [Release 1.1.3](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.3)   |
| Jan 27, 2023      | [Release 1.1.4](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.4)   |
| Mar 30, 2023      | [Release 1.1.5](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.5)   |
| Apr 28, 2023      | [Release 1.1.6](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.6)   |
| May 11, 2023      | [Release 1.1.7](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.7)   |
| June 28, 2023     | [Release 1.1.8](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.8)   |
| July 31, 2023     | [Release 1.1.9](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.9)   |
| October 3, 2023   | [Release 1.1.10](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.10) |
| November 2, 2023  | [Release 1.1.11](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.11) |
| December 21, 2023 | [Release 1.1.12](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.12) |
| January 19, 2024  | [Release 1.1.13](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.13) |
| March 4, 2024     | [Release 1.1.14](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.14) |


aws-mysql-jdbc-driver [follows semver](https://semver.org/#semantic-versioning-200) which means we will only release
breaking changes in major versions (e.g. changes that are incompatible with existing APIs). This tends to happen when we need to change
the way the driver is currently working. Fortunately the JDBC API is fairly mature and hasn't changed, however, in the event that
the API changes we will release a version to be compatible. Minor version releases include new features as well as fixes to existing
features. We release patches to fix existing problems without adding new features. We do our best to deprecate existing features before
removing them completely.

## Maintenance Policy

New features will not be added to the aws-jdbc-driver going forward. Future development will be taking place in the
[aws-advanced-wrapper driver](https://github.com/awslabs/aws-advanced-jdbc-wrapper).
The aws-mysql-jdbc project follows the semantic versioning specification for assigning version numbers
to releases. We recommend to adopt the new wrapper driver before the maintenance window of current version ends on **July 25, 2024**.

However, sometimes an incompatible change is unavoidable. When this happens, the software’s maintainers will increment
the major version number (e.g., increment from release_policy 1.3.1 to release_policy 2.0.0).
The last minor version of the previous major version of the software will then enter a maintenance window
(e.g., 1.3.x). During the maintenance window, the software will continue to receive bug fixes and security patches,
but no new features.

We follow OpenSSF’s best practices for patching publicly known vulnerabilities, and we make sure that there are
no unpatched vulnerabilities of medium or higher severity that have been publicly known for more than 60 days
in our actively maintained versions.

The duration of the maintenance window will vary by product and release. By default, versions will remain under maintenance
until the next major version enters maintenance, or until 1 year passes, whichever is longer. Therefore, at any given time,
the current major version and previous major version will both be supported, as well as older major versions that have been
in maintenance for less than 12 months. Please note that, maintenance windows are influenced by the support schedules for included
software dependencies, community inputs, the scope of the changes introduced by the new version,and estimates for the effort
required to continue maintenance of the previous version.

The software maintainers will not back-port fixes or features to versions outside of the maintenance window.
However, we welcome PR’s with back-ports and will follow the project’s review process.
No new releases will result from these changes, but interested parties can create their own distribution
from the updated source after the PRs are merged.

| Major Version | Latest Minor Version | Status      | Initial Release | Maintenance Window Start | Maintenance Window End |
|---------------|----------------------|-------------|-----------------|--------------------------|------------------------|
| 1             | 1.0.0                | Maintenance | Mar 1, 2022     | June 29, 2022            | June 29, 2023          |
| 2             | 1.1.14               | Current     | June 28, 2023   | July 25, 2023            | July 25, 2024          |
