# Release Schedule and Maintenance Policy
| Release Date  | Release                                                                       |
|---------------|-------------------------------------------------------------------------------|
| Mar 1, 2022   | [Release 1.0.0](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.0.0) |  
| June 29, 2022 | [Release 1.1.0](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.0) | 
| Sept 22, 2022 | [Release 1.1.1](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.1) |
| Nov 22, 2022  | [Release 1.1.2](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.2) |  
| Jan 5, 2023   | [Release 1.1.3](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.3) |
| Jan 27, 2023  | [Release 1.1.4](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.4) |
| Mar 30, 2023  | [Release 1.1.5](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.5) |
| Apr 28, 2023  | [Release 1.1.6](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.6) |
| May 11, 2023  | [Release 1.1.7](https://github.com/awslabs/aws-mysql-jdbc/releases/tag/1.1.7) |


Aws-mysql-jdbc-driver [follows semver](https://semver.org/#semantic-versioning-200) which means we will only release
breaking changes in major versions. Generally speaking patches will be released to fix existing problems without adding
new features. Minor version releases will include new features as well as fixes to existing features. We will do our
best to deprecate existing features before removing them completely.


Aws-mysql-jdbc releases new major versions only when there are a critical mass of breaking changes 
(e.g. changes that are incompatible with existing APIs). This tends to happen if we need to
change the way the driver is currently working. Fortunately the JDBC API is fairly mature and hasn't changed, however
in the event that the API changes we will release a version to be compatible.

Maintenance Policy

New features will not be added to the aws-jdbc-driver. Future development will be taking place in the 
[aws-advanced-wrapper driver](https://github.com/awslabs/aws-advanced-jdbc-wrapper).
The Aws-mysql-jdbc project follows the semantic versioning specification for assigning version numbers
to releases, so you should be able to upgrade to the latest minor version of that same major version of the
software without encountering incompatible changes (e.g., 1.1.0 → 1.3.x).

Sometimes an incompatible change is unavoidable. When this happens, the software’s maintainers will increment
the major version number (e.g., increment from release_policy 1.3.1 to release_policy 2.0.0).
The last minor version of the previous major version of the software will then enter a maintenance window
(e.g., 1.3.x). During the maintenance window, the software will continue to receive bug fixes and security patches,
but no new features.

We follow OpenSSF’s best practices for patching publicly known vulnerabilities, and we make sure that there are
no unpatched vulnerabilities of medium or higher severity that have been publicly known for more than 60 days
in our actively maintained versions.

The duration of the maintenance window will vary from product to product and release to release.
By default, versions will remain under maintenance until the next major version enters maintenance,
or 1 year passes, whichever is longer. Therefore, at any given time, the current major version and
previous major version will both be supported, as well as older major versions that have been in maintenance
for less than 12 months. Please note that, maintenance windows are influenced by the support schedules for
dependencies the software includes, community input, the scope of the changes introduced by the new version,
and estimates for the effort required to continue maintenance of the previous version.

The software maintainers will not back-port fixes or features to versions outside the maintenance window.
That said, PRs with said back-ports are welcome and will follow the project’s review process.
No new releases will result from these changes, but interested parties can create their own distribution
from the updated source after the PRs are merged.

| Major Version | Latest Minor Version | Status      | Initial Release | Maintenance Window Start | Maintenance Window End |
|---------------|----------------------|-------------|-----------------|--------------------------|------------------------|
| 1             | 1.0.0                | Maintenance | Mar 1, 2022     | June 29, 2022            | June 29, 2023          | 
| 2             | 1.1.7                | Current     | June 29, 2022   | N/A                      | N/A                    | 