# Amazon Web Services (AWS) JDBC Driver for MySQL

[![Build Status](https://github.com/awslabs/aws-mysql-jdbc/workflows/CI/badge.svg)](https://github.com/awslabs/aws-mysql-jdbc/actions?query=workflow%3A%22CI%22)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.aws.rds/aws-mysql-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/software.aws.rds/aws-mysql-jdbc)
[![Javadoc](https://javadoc.io/badge2/software.aws.rds/aws-mysql-jdbc/javadoc.svg)](https://javadoc.io/doc/software.aws.rds/aws-mysql-jdbc)
[![License: GPLv2 with FOSS exception](https://img.shields.io/badge/license-GPLv2_with_FOSS_exception-c30014.svg)](LICENSE)

**The Amazon Web Services (AWS) JDBC Driver for MySQL** is a driver that allows an application to take full advantage of the features of clustered MySQL databases. It is based on and can be used as a drop-in compatible with the [MySQL Connector/J driver](https://github.com/mysql/mysql-connector-j), and is compatible with all MySQL deployments including regular RDS and MySQL databases as well as Aurora MySQL.

The AWS JDBC Driver for MySQL supports fast failover for Amazon Aurora with MySQL compatibility. Additional support for extended features of clustered databases, (including Amazon RDS for MySQL and on-premises MySQL deployment), is planned.

> **IMPORTANT** Because this is a project preview, we encourage you to experiment with MySQL driver but DO NOT adopt it for production use. Use of the MySQL driver in preview is subject to the terms and conditions contained in the [AWS Service Terms](https://aws.amazon.com/service-terms), particularly the Beta Service Participation Service Terms, and applies to any driver not marked as 'Generally Available'.
## What is Failover?

In an Amazon Aurora DB cluster, failover is a mechanism by which Aurora automatically repairs the DB cluster status when a primary DB instance becomes unavailable. During failover, an Aurora Replica becomes the new primary DB instance, so the DB cluster can provide maximum availability to a primary read-write DB instance. The AWS JDBC Driver for MySQL is designed to coordinate with this behavior to provide minimal downtime in the event of a DB instance failure.

## The AWS JDBC Driver failover process

<div style="text-align:center"><img src="./docs/files/images/failover_diagram.png" /></div>

The figure above provides a simplified overview of how the AWS JDBC Driver handles an Aurora failover. Starting at the top of the diagram, an application with the AWS JDBC Driver on its class path uses the driver to get a logical connection to an Aurora database. In this example, the application requests a connection using the Aurora DB cluster endpoint and is returned a logical connection that is physically connected to the primary DB instance in the DB cluster, DB instance C. Due to how the application operates against the logical connection, the physical connection details about which specific DB instance it is connected to have been abstracted away. Over the course of the application's lifetime, it executes various statements against the logical connection. If DB instance C is stable and active, these statements succeed and the application continues as normal. If DB instance C experiences a failure, Aurora will initiate failover to promote a new primary DB instance. At the same time, the AWS JDBC Driver will intercept the related communication exception and kick off its own internal failover process. In this case, in which the primary DB instance has failed, the driver will use its internal topology cache to temporarily connect to an active Aurora Replica. This Aurora Replica will be periodically queried for the DB cluster topology until the new primary DB instance is identified (DB instance A or B in this case). At this point, the driver will connect to the new primary DB instance and return control to the application by raising a SQLException with SQLState 08S02 so that the application can re-configure the session state as required. Although the DNS endpoint for the DB cluster might not yet resolve to the new primary DB instance, the driver has already discovered the new DB instance during the failover, and will be directly connected to it when the application continues executing statements. In this way the driver provides a faster way to reconnect to a newly promoted DB instance, thus improving the availability of the DB cluster.

## How the AWS JDBC Driver for MySQL improves the failover process

Although Aurora is able to provide maximum availability through the use of failover, existing client drivers do not currently support this functionality. This is partially due to the time required for the DNS of the new primary DB instance to be fully resolved to properly redirect the connection. The AWS JDBC Driver for MySQL fully utilizes failover behavior by maintaining a cache that contains the Aurora cluster topology, and each DB instance's role (Aurora Replica or primary DB instance). This topology is provided by a direct query to the Aurora database, essentially providing a shortcut to bypass the delays caused by DNS resolution. This allows the AWS JDBC Driver to more closely monitor the Aurora DB cluster status so a connection to the new primary DB instance can be established as quickly as possible. 

## Getting Started

### Prerequisites
You need to install Amazon Corretto 8+ or Java 8+ before using the AWS JDBC Driver for MySQL.

### Obtaining the AWS JDBC Driver for MySQL

#### Performing a direct download
You can install the AWS JDBC Driver for MySQL from pre-compiled packages that can be downloaded directly from [GitHub Releases](https://github.com/awslabs/aws-mysql-jdbc/releases) or [Maven Central](https://search.maven.org/search?q=g:software.aws.rds). Before installing the driver, you need to obtain the corresponding JAR file and include it in the application's CLASSPATH.

**Example - Using wget to install the driver with a direct download**
```bash
wget https://github.com/awslabs/aws-mysql-jdbc/releases/download/0.2.0/aws-mysql-jdbc-0.2.0.jar
```

**Example - Adding the driver to the CLASSPATH**
```bash
export CLASSPATH=$CLASSPATH:/home/userx/libs/aws-mysql-jdbc-0.2.0.jar
```

#### Installing the driver as a Maven dependency
Alternatively, you can use the [Maven dependency management](https://search.maven.org/search?q=g:software.aws.rds) tool to install the driver by adding the following configuration to the application's Project Object Model (POM) file:

**Example - Configuring Maven to install the driver**
```xml
<dependencies>
  <dependency>
    <groupId>software.aws.rds</groupId>
    <artifactId>aws-mysql-jdbc</artifactId>
    <version>0.2.0</version>
  </dependency>
</dependencies>
```

#### Installing the driver as a Gradle dependency
Alternatively, you can use the [Gradle dependency management](https://search.maven.org/search?q=g:software.aws.rds) tool to install the driver by adding the following configuration in the application's ```build.gradle``` file:

**Example - Configuring Gradle to install the driver**
```gradle
dependencies {
    implementation group: 'software.aws.rds', name: 'aws-mysql-jdbc', version: '0.2.0'
}
```
### Using the AWS JDBC Driver for MySQL
Usage of the AWS JDBC Driver for MySQL is identical to the [MySQL-Connector-J JDBC driver](https://github.com/mysql/mysql-connector-j). The sections below highlight usage specific to failover:

#### Driver name
The driver name to use is: ```software.aws.rds.jdbc.Driver```. If you are building the driver directly from ```main```, use the driver name: ```software.aws.rds.jdbc.mysql.Driver```. You will need this name when loading the driver explicitly to the driver manager.

#### Connection URL descriptions

Many different types of URLs that can connect to an Aurora DB cluster. For some URL types, the AWS JDBC Driver requires the user to provide information about the Aurora DB cluster to support failover functionality. The section that follows outlines the various URL types. For each type, information is provided on how the driver will behave and what information the driver requires about the DB cluster, where applicable.

Note: The connection string follows standard URL parameters. In order to add parameters to the connection string, simply add `?` and then specify `parameter_name=value` at the end of the connection string. You can add multiple parameters by separating the parameter name and value set (`parameter_name=value`) with the `&` symbol. For example, to add 2 parameters simply add `?param_name=value&param_2=value2` at the end of the connection string.
 

| URL Type        | Example           | Required Parameters  | Driver Behavior |
| ------------- |-------------| :-----:| --- |
| Cluster Endpoint      | `jdbc:mysql:aws://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:3306` | None | *Initial connection:* primary DB instance<br/>*Failover behavior:* connect to the new primary DB instance |
| Read-Only Cluster Endpoint      | `jdbc:mysql:aws://db-identifier.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:3306`      |   None |  *Initial connection:* any Aurora Replica<br/>*Failover behavior:* prioritize connecting to any active Aurora Replica but might connect to the primary DB instance if it provides a faster connection|
| Instance Endpoint | `jdbc:mysql:aws://instance-1.XYZ.us-east-2.rds.amazonaws.com:3306`      |    None | *Initial connection:* the instance specified (DB instance 1)<br/>*Failover behavior:* connect to the primary DB instance|
| RDS Custom Cluster | `jdbc:mysql:aws://db-identifier.cluster-custom-XYZ.us-east-2l.rds.amazonaws.com:3306`      |    None | *Initial connection:* any DB instance in the custom DB cluster<br/>*Failover behavior:* connect to the primary DB instance (note that this might be outside of the custom DB cluster) |
| IP Address | `jdbc:mysql:aws://10.10.10.10:3306`      |    `clusterInstanceHostPattern` | *Initial connection:* the DB instance specified<br/>*Failover behavior:* connect to the primary DB instance |
| Custom Domain | `jdbc:mysql:aws://my-custom-domain.com:3306`      |    `clusterInstanceHostPattern` | *Initial connection:* the DB instance specified<br/>*Failover behavior:* connect to the primary DB instance |
| Non-Aurora Endpoint | `jdbc:mysql:aws://localhost:3306`     |    None | A regular JDBC connection will be returned - no failover functionality |

(Information about the `clusterInstanceHostPattern` is mentioned in the section below.)

For more information about failover-related parameters that can be configured with the AWS JDBC Driver, see the following section.

#### Failover Parameters

In addition to [the parameters that can be configured for the MySQL Connector/J driver](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html), the following parameters can also be passed to the AWS JDBC Driver through the connection URL to configure additional driver behavior.

| Parameter       | Value           | Required      | Description  |
| ------------- |:-------------:|:-------------:| ----- |
|`enableClusterAwareFailover` | Boolean | No | Set to true to turn on the fast failover behavior offered by the AWS JDBC Driver. Set to false for simple JDBC connections that do not require fast failover functionality.<br/><br/>**Default value:** `true` |
|`clusterInstanceHostPattern` | String | If connecting using an IP address or custom domain URL: Yes<br/>Otherwise: No | This parameter is not required unless connecting to an AWS RDS cluster with an IP address or custom domain URL. In those cases, this parameter specifies the cluster instance DNS pattern that will be used to build a complete instance endpoint. A "?" character in this pattern should be used as a placeholder for the DB instance identifiers of the instances in the cluster. <br/><br/>Example: `?.my-domain.com`, `any-subdomain.?.my-domain.com:9999`<br/><br/>Usecase Example: If your cluster instance endpoints followed this pattern:`instanceIdentifier1.customHost`, `instanceIdentifier2.customHost`, etc. and you wanted your initial connection to be to `customHost:1234`, then your connection string should look something like this: `jdbc:mysql:aws://customHost:1234/test?clusterInstanceHostPattern=?.customHost`<br/><br/>**Default value:** if unspecified, and the provided connection string is not an IP address or custom domain, the driver will automatically acquire the cluster instance host pattern from the customer-provided connection string. |
|`clusterId` | String | No | A unique identifier for the cluster. Connections with the same cluster id share a cluster topology cache. This connection parameter is not required and thus should only be set if desired. <br/><br/>**Default value:** If unspecified, the driver will automatically acquire a cluster id for AWS RDS clusters. |
|`clusterTopologyRefreshRateMs` | Integer | No | Cluster topology refresh rate in milliseconds. The cached topology for the cluster will be invalidated after the specified time, after which it will be updated during the next interaction with the connection.<br/><br/>**Default value:** `30000` |
|`failoverTimeoutMs` | Integer | No | Maximum allowed time in millipseconds to attempt reconnecting to a new writer or reader instance after a cluster failover is initiated.<br/><br/>**Default value:** `60000` |
|`failoverClusterTopologyRefreshRateMs` | Integer | No | Cluster topology refresh rate in milliseconds during a writer failover process. During the writer failover process, cluster topology may be refreshed at a faster pace than normal to speed up discovery of the newly promoted writer.<br/><br/>**Default value:** `5000` |
|`failoverWriterReconnectIntervalMs` | Integer | No | Interval of time in milliseconds to wait between attempts to reconnect to a failed writer during a writer failover process.<br/><br/>**Default value:** `5000` |
|`failoverReaderConnectTimeoutMs` | Integer | No | Maximum allowed time in milliseconds to attempt to connect to a reader instance during a reader failover process. <br/><br/>**Default value:** `5000`
|`acceptAwsProtocolOnly` | Boolean | If using simultaneously with another MySQL driver that supports the same protocols: Yes<br/>Otherwise: No | Set to true to only accept connections for URLs with the jdbc:mysql:aws:// protocol. This setting should be set to true when running an application that uses this driver simultaneously with another MySQL driver that supports the same protocols (eg the MySQL JDBC Driver), to ensure the driver protocols do not clash. This behavior can also be set at the driver level for each connection with the Driver.setAcceptAwsProtocolOnly method; however, this connection parameter will take priority when present.<br/><br/>**Default value:** `false`
|`gatherPerfMetrics` | Boolean | No | Set to true if you would like the driver to record failover-associated metrics, which will then be logged upon closing the connection. This behavior can also be set with the Driver.setAcceptAwsProtocolOnly method at the driver level for each connection, however, this connection parameter will take priority when present.<br/><br/>**Default value:** `false` | 
|`allowXmlUnsafeExternalEntity` | Boolean | No | Set to true if you would like to use XML inputs that refer to external entities. WARNING: Setting this to true is unsafe since your system to be prone to XXE attacks.<br/><br/>**Default value:** `false` | 
#### Failover Exception Codes
##### 08001 - Unable to Establish SQL Connection
When the driver throws a SQLException with code ```08001```, it means the original connection failed, and the driver tried to failover to a new instance, but was unable to. There are various reasons this may happen: no nodes were available, a network failure occurred, etc. In this scenario, please wait until the server is up or other problems are solved. (Exception will be thrown.)

##### 08S02 - Communication Link 
When the driver throws a SQLException with code ```08S02```, it means the original connection failed while autocommit was set to true, and the driver successfully failed over to another available instance in the cluster. However, any session state configuration of the initial connection is now lost. In this scenario, the user should:

- Reuse and re-configure the original connection (e.g., Re-configure session state to be the same as the original connection).

- Repeat the query that was executed when the connection failed and continue work as desired.

###### Sample Code
```java
import java.sql.*;

/**
 * Scenario 1: Failover happens when autocommit is set to true - Catch SQLException with code 08S02.
 */
public class FailoverSampleApp1 {
  private static final String CONNECTION_STRING = "jdbc:mysql:aws://database-mysql.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/myDb";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final int MAX_RETRIES = 5;

  public static void main(String[] args) throws SQLException {
    // Create a connection.
    try(Connection conn = DriverManager.getConnection(CONNECTION_STRING, USERNAME, PASSWORD)) {
      // Configure the connection.
      setInitialSessionState(conn);
   
      // Do something with method "betterExecuteQuery" using the Cluster-Aware Driver.
      String select_sql = "SELECT * FROM employees";
      try(ResultSet rs = betterExecuteQuery(conn, select_sql)) {
        while (rs.next()) {
          System.out.println(rs.getString("name"));
        }
      }
    }
  }

  private static void setInitialSessionState(Connection conn) throws SQLException {
    // Your code here for the initial connection setup.
    try(Statement stmt1 = conn.createStatement()) {
      stmt1.executeUpdate("SET time_zone = \"+00:00\"");
    }
  }
  
  // A better executing query method when autocommit is set as the default value - True.
  private static ResultSet betterExecuteQuery(Connection conn, String query) throws SQLException {
    // Create a boolean flag.
    boolean isSuccess = false;
    // Record the times of re-try.
    int retries = 0;
    
    ResultSet rs = null;
    while (!isSuccess) {
      try {
        Statement stmt = conn.createStatement();
        rs = stmt.executeQuery(query);
        isSuccess = true;
    
      } catch (SQLException e) {
    
        // If the attempt to connect has failed MAX_RETRIES times,
        // throw the exception to inform users of the failed connection.
        if (retries > MAX_RETRIES) {
          throw e;
        }
    
        // Failover has occurred and the driver has failed over to another instance successfully.
        if (e.getSQLState().equalsIgnoreCase("08S02")) {
          // Re-config the connection.
          setInitialSessionState(conn);
          // Re-execute that query again.
          retries++;
  
        } else {
          // If some other exception occurs, throw the exception.
          throw e;
        }
      }
    }
    
    // return the ResultSet successfully.
    return rs;
  }
}
```

##### 08007 - Transaction Resolution Unknown
When the driver throws a SQLException with code ```08007```, it means the original connection failed within a transaction (while autocommit was set to false). In this scenario, the driver first attempts to rollback the transaction and then fails over to another available instance in the cluster. Note that the rollback might be unsuccessful as the initial connection may be broken at the time that the driver recognizes the problem. Note also that any session state configuration of the initial connection is now lost. In this scenario, the user should:

- Reuse and re-configure the original connection (e.g: re-configure session state to be the same as the original connection).

- Re-start the transaction and repeat all queries which were executed during the transaction before the connection failed.

- Repeat the query that was executed when the connection failed and continue work as desired.

###### Sample Code
```java
import java.sql.*;

/**
 * Scenario 2: Failover happens when autocommit is set to false - Catch SQLException with code 08007.
 */
public class FailoverSampleApp2 {
  private static final String CONNECTION_STRING = "jdbc:mysql:aws://database-mysql.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/myDb";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final int MAX_RETRIES = 5;

  public static void main(String[] args) throws SQLException {
    // Create a connection
    try(Connection conn = DriverManager.getConnection(CONNECTION_STRING, USERNAME, PASSWORD)) {
      // Configure the connection - set autocommit to false.
      setInitialSessionState(conn);
  
      // Do something with method "betterExecuteUpdate_setAutoCommitFalse" using the Cluster-Aware Driver.
      String[] update_sql = new String[3];
      // Add all queries that you want to execute inside a transaction.
      update_sql[0] = "INSERT INTO employees(name, position, salary) VALUES('john', 'developer', 2000)";
      update_sql[1] = "INSERT INTO employees(name, position, salary) VALUES('mary', 'manager', 2005)";
      update_sql[2] = "INSERT INTO employees(name, position, salary) VALUES('Tom', 'accountant', 2019)";
      betterExecuteUpdate_setAutoCommitFalse(conn, update_sql);
    }
  }

  private static void setInitialSessionState(Connection conn) throws SQLException {
    // Your code here for the initial connection setup.
    try(Statement stmt1 = conn.createStatement()) {
      stmt1.executeUpdate("SET time_zone = \"+00:00\"");
    }
    conn.setAutoCommit(false);
  }

  // A better executing query method when autocommit is set to False.
  private static void betterExecuteUpdate_setAutoCommitFalse(Connection conn, String[] queriesInTransaction) throws SQLException {
    // Create a boolean flag.
    boolean isSuccess = false;
    // Record the times of re-try.
    int retries = 0;

    while (!isSuccess) {
      try(Statement stmt = conn.createStatement()) {
        for(String sql: queriesInTransaction){
          stmt.executeUpdate(sql);
        }
        conn.commit();
        isSuccess = true;
      } catch (SQLException e) {

        // If the attempt to connect has failed MAX_RETRIES times,
        // rollback the transaction and throw the exception to inform users of the failed connection.
        if (retries > MAX_RETRIES) {
          conn.rollback();
          throw e;
        }

        // Failure happens within the transaction and the driver failed over to another instance successfully.
        if (e.getSQLState().equalsIgnoreCase("08007")) {
          // Re-config the connection, re-start the transaction.
          setInitialSessionState(conn);
          // Re-execute every queries that were inside the transaction.
          retries++;

        } else {
          // If some other exception occurs, rollback the transaction and throw the exception.
          conn.rollback();
          throw e;
        }
      } 
    }
  }
}
```
>### :warning: Warnings About Proper Usage of the AWS JDBC Driver for MySQL
>1. A common practice when using JDBC drivers is to wrap invocations against a connection object in a try-catch block, and dispose of the connection object if an exception is hit. If this practice is left unaltered, the application will lose the fast-failover functionality offered by the driver. When failover occurs, the driver internally establishes a ready-to-use connection inside the original connection object before throwing an exception to the user. If the connection object is disposed of, the newly established connection is thrown away. The correct practice is to check the SQL error code of the exception and reuse the connection object if the error code indicates successful failover. [FailoverSampleApp1](#sample-code) and [FailoverSampleApp2](#sample-code-1) demonstrate this practice. See the section below on [Failover Exception Codes](#failover-exception-codes) for more details.
>2. It is highly recommended that you use the cluster and read-only cluster endpoints instead of the direct instance endpoints of your Aurora cluster, unless you are confident about your application's usage of instance endpoints. Although the driver will correctly failover to the new writer instance when using instance endpoints, use of these endpoints is discouraged because individual instances can spontaneously change reader/writer status when failover occurs. The driver will always connect directly to the instance specified if an instance endpoint is provided, so a write-safe connection cannot be assumed if the application uses instance endpoints.
## Extra Additions

### XML Entity Injection Fix

The default XML parser contained a security risk which made the driver prone to XXE (XML Entity Injection) attacks. To solve this issue, we disabled DTDs (Document Type Definition) in the XML parser. If you require this restriction to be lifted in your application, you can set the `allowXmlUnsafeExternalEntity` parameter in the connection string to `true`. Please see the table below for a definition of this parameter. 

| Parameter       | Value           | Required      | Description  |
| ------------- |:-------------:|:-------------:| ----- |
|`allowXmlUnsafeExternalEntity` | Boolean | No | Set to true if you would like to use XML inputs that refer to external entities. WARNING: Setting this to `true` is unsafe; your system may to be prone to XXE attacks.<br/><br/>**Default value:** `false` |

### AWS IAM Database Authentication

One secure authentication method is to use Amazon AWS Identity and Access Management (IAM). 
When using AWS IAM database authentication, the host URL must be a valid Amazon endpoint, and not a custom domain or an IP address.
<br>ie. `database-mysql-name.cluster-XYZ.us-east-2.rds.amazonaws.com`


IAM database authentication is limited to certain database engines.
For more information about limitations and recommendations, please [read](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).

#### Setting up IAM database Authentication for MySQL 
1. Configure AWS IAM database authentication for an existing database or create a new database on AWS RDS Console
   1. [Creating new database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CreateDBInstance.html)
   2. [Modifying existing database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html)
2. Create/Change and [use AWS IAM policy](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.IAMPolicy.html) for AWS IAM database authentication
3. [Create a database account](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.DBAccounts.html) using AWS IAM database authentication
   1. Connect to the MySQL DB using the DB master login, and use the following command to create a new user<br>
   `CREATE USER example_user_name IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';`

| Parameter       | Value           | Required      | Description  |
| ------------- |:-------------:|:-------------:| ----- |
|`useAwsIam` | Boolean | No | Set to `true` if you would like to use AWS IAM database authentication<br/><br/>**Default value:** `false` |

###### Sample Code
```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import software.aws.rds.jdbc.mysql.shading.com.mysql.cj.conf.PropertyKey;
import software.aws.rds.jdbc.mysql.Driver;

public class AwsIamAuthenticationSample {

   private static final String CONNECTION_STRING = "jdbc:mysql:aws://database-mysql-name.cluster-XYZ.us-east-2.rds.amazonaws.com";
   private static final String USER = "example_user_name";

   public static void main(String[] args) throws SQLException {
      /// Load AWS RDS driver
      DriverManager.registerDriver(new Driver());

      // Create properties and set-up for AWS IAM database authentication
      final Properties properties = new Properties();
      properties.setProperty(PropertyKey.useAwsIam.getKeyName(), Boolean.TRUE.toString());
      properties.setProperty(PropertyKey.USER.getKeyName(), USER);

      // Try and make a connection
      try (final Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties)) {
         try (final Statement myQuery = conn.createStatement()) {
            try (final ResultSet rs = myQuery.executeQuery("SELECT NOW();")) {
               while (rs.next()) {
                  System.out.println(rs.getString(1));
               }
            }
         }
      }
   }
}
```

## Development

### Setup

After you have installed Amazon Corretto or Java according to the Prerequisites section, use the following command to clone the driver repository:

```bash
$ git clone https://github.com/awslabs/aws-mysql-jdbc.git
$ cd aws-mysql-jdbc
```

You can now make changes in the repository.
### Building the AWS JDBC Driver for MySQL

To build the AWS JDBC Driver without running the tests, navigate into the aws-mysql-jdbc directory and run the following command:

```bash
gradlew build -x test
```

Then run the following command:

```bash
gradlew build
```

### Running the Tests

You must install [Docker](https://docs.docker.com/get-docker/) before running the tests for the AWS JDBC Driver. After installing Docker, execute the following commands to create the Docker servers that the tests will run against:

```bash
$ cd aws-mysql-jdbc/docker
$ docker-compose up -d
$ cd ../
```

You can now use the following command to run the tests in the ```aws-mysql-jdbc``` directory:

```bash
gradlew test
```

To shut down the Docker servers after you've finishing testing:

```bash
$ cd aws-mysql-jdbc/docker
$ docker-compose down && docker-compose rm
$ cd ../
```

## Known Issues
### SSLHandshakeException
Using the driver with JDKs based on OpenJDK 8u292+ or OpenJDK 11.0.11+ will result in an exception: `SSLHandshakeException: No appropriate protocol`.
This is due to OpenJDK disabling TLS 1.0 and 1.1 availability in `security.properties`, for additional information see "[Disable TLS 1.0 and TLS 1.1](https://java.com/en/configure_crypto.html#DisableTLS)".
To resolve this exception, add the `enabledTLSProtocols=TLSv1.2` connection property when connecting to a database.

## Getting Help and Opening Issues

If you encounter a bug with the AWS JDBC Driver for MySQL, we'd like to hear about it. Please search the [existing issues](https://github.com/awslabs/aws-mysql-jdbc/issues) and see if others are also experiencing the issue before opening a new issue. When opening a new issue, we'll need the version of AWS JDBC Driver for MySQL, Java language version, OS you’re using, and the MySQL database version you're running against. Please include detailed information about reproducing the issue when appropriate.

GitHub issues are intended for bug reports and feature requests. Keeping the list of open issues lean will help us respond in a timely manner.

## Documentation

For additional documentation on the AWS JDBC Driver, [please refer to the documentation for the open-source mysql-connector-j driver that the AWS JDBC Driver was based on](https://dev.mysql.com/doc/connector-j/8.0/en/).

## License

This software is released under version 2 of the GNU General Public License (GPLv2).
