# Amazon Web Services (AWS) JDBC Driver for MySQL

[![Build Status](https://github.com/awslabs/aws-mysql-jdbc/workflows/CI/badge.svg)](https://github.com/awslabs/aws-mysql-jdbc/actions?query=workflow%3A%22CI%22)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.aws.rds/aws-mysql-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/software.aws.rds/aws-mysql-jdbc)
[![Javadoc](https://javadoc.io/badge2/software.aws.rds/aws-mysql-jdbc/javadoc.svg)](https://javadoc.io/doc/software.aws.rds/aws-mysql-jdbc)
[![License: GPLv2 with FOSS exception](https://img.shields.io/badge/license-GPLv2_with_FOSS_exception-c30014.svg)](LICENSE)

**The Amazon Web Services (AWS) JDBC Driver for MySQL** allows an application to take advantage of the features of clustered MySQL databases. It is based on and can be used as a drop-in compatible for the [MySQL Connector/J driver](https://github.com/mysql/mysql-connector-j), and is compatible with all MySQL deployments.

The AWS JDBC Driver for MySQL supports fast failover for Amazon Aurora with MySQL compatibility. Support for additional features of clustered databases, including features of Amazon RDS for MySQL and on-premises MySQL deployments, is planned.

> **IMPORTANT** Because this project is in preview, we encourage you to experiment with MySQL driver but DO NOT recommend adopting it for production use. Use of the MySQL driver in preview is subject to the terms and conditions in the [AWS Service Terms](https://aws.amazon.com/service-terms), (particularly the Beta Service Participation Service Terms). This applies to any drivers not marked as 'Generally Available'.
## What is Failover?

An Amazon Aurora DB cluster uses failover to automatically repairs the DB cluster status when a primary DB instance becomes unavailable. During failover, Aurora promotes a replica to become the new primary DB instance, so that the DB cluster can provide maximum availability to a primary read-write DB instance. The AWS JDBC Driver for MySQL is designed to coordinate with this behavior in order to provide minimal downtime in the event of a DB instance failure.

## Benefits of the AWS JDBC Driver for MySQL

Although Aurora is able to provide maximum availability through the use of failover, existing client drivers do not fully support this functionality. This is partially due to the time required for the DNS of the new primary DB instance to be fully resolved in order to properly direct the connection. The AWS JDBC Driver for MySQL fully utilizes failover behavior by maintaining a cache of the Aurora cluster topology and each DB instance's role (Aurora Replica or primary DB instance). This topology is provided via a direct query to the Aurora database, essentially providing a shortcut to bypass the delays caused by DNS resolution. With this knowledge, the AWS JDBC Driver can more closely monitor the Aurora DB cluster status so that a connection to the new primary DB instance can be established as fast as possible. Additionally, as noted above, the AWS JDBC Driver is designed to be a drop-in compatible for other MySQL JDBC drivers and can be used to interact with RDS and MySQL databases as well as Aurora MySQL.

## The AWS JDBC Driver Failover Process

<div style="text-align:center"><img src="./docs/files/images/failover_diagram.png" /></div>

The figure above provides a simplified overview of how the AWS JDBC Driver handles an Aurora failover encounter. Starting at the top of the diagram, an application with the AWS JDBC Driver on its class path uses the driver to get a logical connection to an Aurora database. In this example, the application requests a connection using the Aurora DB cluster endpoint and is returned a logical connection that is physically connected to the primary DB instance in the DB cluster, DB instance C. Due to how the application operates against the logical connection, the physical connection details about which specific DB instance it is connected to have been abstracted away. Over the course of the application's lifetime, it executes various statements against the logical connection. If DB instance C is stable and active, these statements succeed and the application continues as normal. If DB instance C later experiences a failure, Aurora will initiate failover to promote a new primary DB instance. At the same time, the AWS JDBC Driver will intercept the related communication exception and kick off its own internal failover process. In this case, in which the primary DB instance has failed, the driver will use its internal topology cache to temporarily connect to an active Aurora Replica. This Aurora Replica will be periodically queried for the DB cluster topology until the new primary DB instance is identified (DB instance A or B in this case). At this point, the driver will connect to the new primary DB instance and return control to the application by raising a SQLException with SQLState 08S02 so that they can reconfigure their session state as required. Although the DNS endpoint for the DB cluster might not yet resolve to the new primary DB instance, the driver has already discovered this new DB instance during its failover process and will be directly connected to it when the application continues executing statements. In this way the driver provides a faster way to reconnect to a newly promoted DB instance, thus increasing the availability of the DB cluster.

## Getting Started

### Prerequisites
You need to install Amazon Corretto 8+ or Java 8+ before using the AWS JDBC Driver for MySQL.

### Obtaining the AWS JDBC Driver for MySQL

#### Direct Download
The AWS JDBC Driver for MySQL can be installed from pre-compiled packages that can be downloaded directly from [GitHub Releases](https://github.com/awslabs/aws-mysql-jdbc/releases) or [Maven Central](https://search.maven.org/search?q=g:software.aws.rds). To install the driver, obtain the corresponding JAR file and include it in the application's CLASSPATH:

**Example - Direct Download via wget**
```bash
wget https://github.com/awslabs/aws-mysql-jdbc/releases/download/0.4.0/aws-mysql-jdbc-0.4.0.jar
```

**Example - Adding the Driver to the CLASSPATH**
```bash
export CLASSPATH=$CLASSPATH:/home/userx/libs/aws-mysql-jdbc-0.4.0.jar
```

#### As a Maven Dependency
You can use [Maven's dependency management](https://search.maven.org/search?q=g:software.aws.rds) to obtain the driver by adding the following configuration to the application's Project Object Model (POM) file:

**Example - Maven**
```xml
<dependencies>
  <dependency>
    <groupId>software.aws.rds</groupId>
    <artifactId>aws-mysql-jdbc</artifactId>
    <version>0.4.0</version>
  </dependency>
</dependencies>
```

#### As a Gradle Dependency
You can use [Gradle's dependency management](https://search.maven.org/search?q=g:software.aws.rds) to obtain the driver by adding the following configuration to the application's ```build.gradle``` file:

**Example - Gradle**
```gradle
dependencies {
    implementation group: 'software.aws.rds', name: 'aws-mysql-jdbc', version: '0.4.0'
}
```
### Using the AWS JDBC Driver for MySQL
The AWS JDBC Driver for MySQL is drop-in compatible, so usage is identical to the [MySQL-Connector-J JDBC driver](https://github.com/mysql/mysql-connector-j). The sections below highlight driver usage specific to failover.

#### Driver Name
Use the driver name: ```software.aws.rds.jdbc.Driver```. If you are building the driver directly from main, use the driver name: ```software.aws.rds.jdbc.mysql.Driver```.

This will be needed when loading the driver explicitly to the driver manager.

#### Connection URL Descriptions

There are many different types of URLs that can connect to an Aurora DB cluster; this section outlines the various URL types. For some URL types, the AWS JDBC Driver requires the user to provide some information about the Aurora DB cluster to provide failover functionality. For each URL type, information is provided below on how the driver will behave and what information the driver requires about the DB cluster, if applicable.

Note: The connection string follows standard URL parameters. In order to add parameters to the connection string, simply add `?` and then the `parameter_name=value` pair at the end of the connection string. You may add multiple parameters by separating the parameter name and value set (`parameter_name=value`) with the `&` symbol. For example, to add 2 parameters simply add `?param_name=value&param_2=value2` at the end of the connection string.
 

| URL Type        | Example           | Required Parameters  | Driver Behavior |
| --------------- |-------------------| :-------------------:| --------------- |
| Cluster Endpoint      | `jdbc:mysql:aws://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:3306` | None | *Initial connection:* primary DB instance<br/>*Failover behavior:* connect to the new primary DB instance |
| Read-Only Cluster Endpoint      | `jdbc:mysql:aws://db-identifier.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:3306`      |   None |  *Initial connection:* any Aurora Replica<br/>*Failover behavior:* prioritize connecting to any active Aurora Replica but might connect to the primary DB instance if it provides a faster connection|
| Instance Endpoint | `jdbc:mysql:aws://instance-1.XYZ.us-east-2.rds.amazonaws.com:3306`      |    None | *Initial connection:* the instance specified (DB instance 1)<br/>*Failover behavior:* connect to the primary DB instance|
| RDS Custom Cluster | `jdbc:mysql:aws://db-identifier.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:3306`      |    None | *Initial connection:* any DB instance in the custom DB cluster<br/>*Failover behavior:* connect to the primary DB instance (note that this might be outside of the custom DB cluster) |
| IP Address | `jdbc:mysql:aws://10.10.10.10:3306`      |    `clusterInstanceHostPattern` | *Initial connection:* the DB instance specified<br/>*Failover behavior:* connect to the primary DB instance |
| Custom Domain | `jdbc:mysql:aws://my-custom-domain.com:3306`      |    `clusterInstanceHostPattern` | *Initial connection:* the DB instance specified<br/>*Failover behavior:* connect to the primary DB instance |
| Non-Aurora Endpoint | `jdbc:mysql:aws://localhost:3306`     |    None | A regular JDBC connection will be returned - no failover functionality |

Information about the `clusterInstanceHostPattern` parameter is provided in the section below.

For more information about parameters that can be configured with the AWS JDBC Driver, see the section below about failover parameters.

#### Failover Parameters

In addition to [the parameters that you can configure for the MySQL Connector/J driver](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html), you can pass the following parameters to the AWS JDBC Driver through the connection URL to specify additional driver behavior.

| Parameter       | Value           | Required      | Description  | Default Value |
| --------------- |:---------------:|:-------------:|:------------ | ------------- |
|`enableClusterAwareFailover` | Boolean | No | Set to true to enable the fast failover behavior offered by the AWS JDBC Driver. Set to false for simple JDBC connections that do not require fast failover functionality. **NOTE:** In addition to this parameter, since the failover functionality is implemented with [connection plugins](#Connection-Plugin-Manager), disabling [`useConnectionPlugins`](#Connection-Plugin-Manager-Parameters) will also disable the failover functionality. | `true` |
|`clusterInstanceHostPattern` | String | If connecting using an IP address or custom domain URL: Yes<br/>Otherwise: No | This parameter is not required unless connecting to an AWS RDS cluster via an IP address or custom domain URL. In those cases, this parameter specifies the cluster instance DNS pattern that will be used to build a complete instance endpoint. A "?" character in this pattern should be used as a placeholder for the DB instance identifiers of the instances in the cluster. <br/><br/>Example: `?.my-domain.com`, `any-subdomain.?.my-domain.com:9999`<br/><br/>Usecase Example: If your cluster instance endpoints followed this pattern:`instanceIdentifier1.customHost`, `instanceIdentifier2.customHost`, etc. and you wanted your initial connection to be to `customHost:1234`, then your connection string should look something like this: `jdbc:mysql:aws://customHost:1234/test?clusterInstanceHostPattern=?.customHost` | If the provided connection string is not an IP address or custom domain, the driver will automatically acquire the cluster instance host pattern from the customer-provided connection string. |
|`clusterId` | String | No | A unique identifier for the cluster. Connections with the same cluster ID share a cluster topology cache. This connection parameter is not required and thus should only be set if desired. | The driver will automatically acquire a cluster id for AWS RDS clusters. |
|`clusterTopologyRefreshRateMs` | Integer | No | Cluster topology refresh rate in milliseconds. The cached topology for the cluster will be invalidated after the specified time, after which it will be updated during the next interaction with the connection. | `30000` |
|`failoverTimeoutMs` | Integer | No | Maximum allowed time in milliseconds to attempt reconnecting to a new writer or reader instance after a cluster failover is initiated. | `60000` |
|`failoverClusterTopologyRefreshRateMs` | Integer | No | Cluster topology refresh rate in milliseconds during a writer failover process. During the writer failover process, cluster topology may be refreshed at a faster pace than normal to speed up discovery of the newly promoted writer. | `5000` |
|`failoverWriterReconnectIntervalMs` | Integer | No | Interval of time in milliseconds to wait between attempts to reconnect to a failed writer during a writer failover process. | `5000` |
|`failoverReaderConnectTimeoutMs` | Integer | No | Maximum allowed time in milliseconds to attempt to connect to a reader instance during a reader failover process. | `5000` |
|`acceptAwsProtocolOnly` | Boolean | If using simultaneously with another MySQL driver that supports the same protocols: Yes<br/>Otherwise: No | Set to true to only accept connections for URLs with the jdbc:mysql:aws:// protocol. This setting should be set to true when running an application that uses this driver simultaneously with another MySQL driver that supports the same protocols (e.g. the MySQL JDBC Driver), to ensure the driver protocols do not clash. This behavior can also be set at the driver level for every connection via the Driver.setAcceptAwsProtocolOnly method; however, this connection parameter will take priority when present. | `false` |
|`gatherPerfMetrics` | Boolean | No | Set to true if you would like the driver to record failover-associated metrics, which will then be logged upon closing the connection. | `false` | 
|`allowXmlUnsafeExternalEntity` | Boolean | No | Set to true if you would like to use XML inputs that refer to external entities. WARNING: Setting this to true is unsafe since your system to be prone to XXE attacks. | `false` |

#### Failover Exception Codes
##### 08001 - Unable to Establish SQL Connection
When the driver throws a SQLException with code ```08001```, the original connection failed, and the driver tried to failover to a new instance, but was not able to. There are various reasons this may happen: no nodes were available, a network failure occurred, and so on. In this scenario, please wait until the server is up or other problems are solved (an exception will be thrown.)

##### 08S02 - Communication Link 
When the driver throws a SQLException with code ```08S02```, the original connection failed while autocommit was set to true, and the driver successfully failed over to another available instance in the cluster. However, any session state configuration of the initial connection is now lost. In this scenario, you should:

- Reuse and reconfigure the original connection (e.g., reconfigure session state to be the same as the original connection).

- Repeat the query that was executed when the connection failed and continue work as desired.

###### Sample Code
```java
import java.sql.*;

/**
 * Scenario 1: Failover happens when autocommit is set to true - Catch SQLException with code 08S02.
 */
public class FailoverSampleApp1 {

   private static final String CONNECTION_STRING = "jdbc:mysql:aws://database-mysql.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/employees";
   private static final String USERNAME = "username";
   private static final String PASSWORD = "password";
   private static final int MAX_RETRIES = 5;

   public static void main(String[] args) throws SQLException {
      try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, USERNAME, PASSWORD)) {
         // Configure the connection.
         setInitialSessionState(conn);

         // Do something with method "betterExecuteQuery" using the Cluster-Aware Driver.
         String select_sql = "SELECT * FROM employees";
         try (ResultSet rs = betterExecuteQuery(conn, select_sql)) {
            while (rs.next()) {
               System.out.println(rs.getString("first_name"));
            }
         }
      }
   }

   private static void setInitialSessionState(Connection conn) throws SQLException {
      // Your code here for the initial connection setup.
      try (Statement stmt1 = conn.createStatement()) {
         stmt1.executeUpdate("SET time_zone = \"+00:00\"");
      }
   }

   // A better executing query method when autocommit is set as the default value - true.
   private static ResultSet betterExecuteQuery(Connection conn, String query) throws SQLException {
      // Record the times of retry.
      int retries = 0;

      while (true) {
         try {
            Statement stmt = conn.createStatement();
            return stmt.executeQuery(query);
         } catch (SQLException e) {
            // If the attempt to connect has failed MAX_RETRIES times,            
            // throw the exception to inform users of the failed connection.
            if (retries > MAX_RETRIES) {
               throw e;
            }

            // Failover has occurred and the driver has failed over to another instance successfully.
            if ("08S02".equalsIgnoreCase(e.getSQLState())) {
               // Reconfigure the connection.
               setInitialSessionState(conn);
               // Re-execute that query again.
               retries++;

            } else {
               // If some other exception occurs, throw the exception.
               throw e;
            }
         }
      }
   }
}
```

##### 08007 - Transaction Resolution Unknown
When the driver throws a SQLException with code ```08007```, the original connection failed within a transaction (while autocommit was set to false). In this scenario, the driver first attempts to rollback the transaction and then fails over to another available instance in the cluster. Note that the rollback might be unsuccessful as the initial connection may be broken at the time that the driver recognizes the problem. Note also that any session state configuration of the initial connection is now lost. In this scenario, the user should:

- Reuse and reconfigure the original connection (e.g: reconfigure session state to be the same as the original connection).

- Re-start the transaction and repeat all queries which were executed during the transaction before the connection failed.

- Repeat that query that was executed when the connection failed and continue work as desired.

###### Sample Code
```java
import java.sql.*;

/**
 * Scenario 2: Failover happens when autocommit is set to false - Catch SQLException with code 08007.
 */
public class FailoverSampleApp2 {

   private static final String CONNECTION_STRING = "jdbc:mysql:aws://database-mysql.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/employees";
   private static final String USERNAME = "username";
   private static final String PASSWORD = "password";
   private static final int MAX_RETRIES = 5;

   public static void main(String[] args) throws SQLException {
      String[] update_sql = {
              "INSERT INTO employees(emp_no, birth_date, first_name, last_name, gender, hire_date) VALUES (5000000, '1958-05-01', 'John', 'Doe', 'M', '1997-11-30')",
              "INSERT INTO employees(emp_no, birth_date, first_name, last_name, gender, hire_date) VALUES (5000001, '1958-05-01', 'Mary', 'Malcolm', 'F', '1997-11-30')",
              "INSERT INTO employees(emp_no, birth_date, first_name, last_name, gender, hire_date) VALUES (5000002, '1958-05-01', 'Tom', 'Jerry', 'M', '1997-11-30')"
      };

      try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, USERNAME, PASSWORD)) {
         // Configure the connection - set autocommit to false.
         setInitialSessionState(conn);

         // Do something with method "betterExecuteUpdate_setAutoCommitFalse" using the Cluster-Aware Driver.           
         betterExecuteUpdate_setAutoCommitFalse(conn, update_sql);
      }
   }

   private static void setInitialSessionState(Connection conn) throws SQLException {
      // Your code here for the initial connection setup.
      try (Statement stmt1 = conn.createStatement()) {
         stmt1.executeUpdate("SET time_zone = \"+00:00\"");
      }
      conn.setAutoCommit(false);
   }

   // A better executing query method when autocommit is set to false.
   private static void betterExecuteUpdate_setAutoCommitFalse(Connection conn, String[] queriesInTransaction) throws SQLException {
      // Create a boolean flag.
      boolean isSuccess = false;
      // Record the times of retry.
      int retries = 0;

      while (!isSuccess) {
         try (Statement stmt = conn.createStatement()) {
            for (String sql : queriesInTransaction) {
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
            if ("08007".equalsIgnoreCase(e.getSQLState())) {
               // Reconfigure the connection, restart the transaction.
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
>1. A common practice when using JDBC drivers is to wrap invocations against a Connection object in a try-catch block, and dispose of the Connection object if an Exception is hit. If your application takes this approach, it will lose the fast-failover functionality offered by the Driver. When failover occurs, the Driver internally establishes a ready-to-use connection inside the original Connection object before throwing an exception to the user. If this Connection object is disposed of, the newly established connection will be thrown away. The correct practice is to check the SQL error code of the exception and reuse the Connection object if the error code indicates successful failover. [FailoverSampleApp1](#sample-code) and [FailoverSampleApp2](#sample-code-1) demonstrate this practice. See the section below on [Failover Exception Codes](#failover-exception-codes) for more details.
>2. It is highly recommended that you use the cluster and read-only cluster endpoints instead of the direct instance endpoints of your Aurora cluster, unless you are confident about your application's usage of instance endpoints. Although the Driver will correctly failover to the new writer instance when using instance endpoints, usage of these endpoints are discouraged because individual instances can spontaneously change reader/writer status when failover occurs. The driver will always connect directly to the instance specified if an instance endpoint is provided, so a write-safe connection cannot be assumed if the application uses instance endpoints.

### Connection Plugin Manager
Connection plugin manager initializes, triggers, and cleans up a chain of connection plugins. Connection plugins are widgets attached to each `Connection` objects to help execute additional or supplementary logic related to that `Connection`. [Enhanced Failure Monitoring](https://github.com/awslabs/aws-mysql-jdbc#enhanced-failure-monitoring) is one example of a connection plugin. 
<div style="text-align:center"><img src="./docs/files/images/connection_plugin_manager_diagram.png" /></div>

The figure above shows a simplified workflow of the connection plugin manager.  
Starting at the top, when a JDBC method is executed by the driver, it is passed to the connection plugin manager. From the connection plugin manager, the JDBC method is passed in order of plugins loaded like a chain. In this figure, `Custom Plugin A` to `Custom Plugin B` and finally to `Default Plugin` which executes the JDBC method and returns the result back up the chain.

[Enhanced Failure Monitoring](#Enhanced-Failure-Monitoring) and the failover functionality are implemented with connection plugins, these two plugins are loaded by default.
Additional custom plugins can be implemented and used alongside existing ones. Plugins can be chained together in a desired order.
> **NOTE:** Loading custom plugins will not include Enhanced Failure Monitoring and the Failover Connection Plugin unless explicitly stated through the `connectionPluginFactories` parameter.

The AWS JDBC Driver for MySQL attaches the `DefaultConnectionPlugin` to the tail of the connection plugin chain 
and actually executes the given JDBC method.

Since all the connection plugins are chained together, the prior connection plugin affects the
latter plugins. If the connection plugin at the head of the connection plugin chain measures the
execution time, this measurement would encompass the time spent in all the connection plugins down
the chain.

To learn how to write custom plugins, refer to examples located inside [Custom Plugins Demo](https://github.com/awslabs/aws-mysql-jdbc/tree/main/src/demo/java/customplugins).

#### Connection Plugin Manager Parameters
| Parameter       | Value           | Required      | Description  | Default Value |
| --------------- |:---------------:|:-------------:|:------------ | ------------- |
| `useConnectionPlugins` | Boolean | No | Disable connection plugins and execute JDBC methods directly. **NOTE:** Since the failover functionality and Enhanced Failure Monitoring are implemented with plugins, disabling connection plugins will also disable such functionality. | `True` |
| `connectionPluginFactories` | String | No | String of fully-qualified class name of plugin factories. <br/><br/>Each factory in the string should be comma-separated `,`<br/><br/>**NOTE: The order of factories declared matters.**  <br/><br/>Example: `customplugins.MethodCountConnectionPluginFactory`, `customplugins.ExecutionTimeConnectionPluginFactory,com.mysql.cj.jdbc.ha.plugins.NodeMonitoringConnectionPluginFactory` | `com.mysql.cj.jdbc.ha.plugins.NodeMonitoringConnectionPluginFactory` |

### Enhanced Failure Monitoring
<div style="text-align:center"><img src="./docs/files/images/enhanced_failure_monitoring_diagram.png" /></div>
The figure above shows a simplified workflow of Enhanced Failure Monitoring. Enhanced Failure Monitoring, is a connection plugin implemented by using a monitor thread. The monitor will periodically check the connected database node's health. In the case of the database node showing up as unhealthy, the query will be retried with a new database node and the monitor is restarted. 

Enhanced Failure Monitoring is loaded in by default and can be disabled by setting parameter `failureDetectionEnabled` to `false`. 

If custom connection plugins are loaded, Enhanced Failure Monitoring and Failover Connection Plugin 
will NOT be loaded unless explicitly included by adding `com.mysql.cj.jdbc.ha.plugins.failover.FailoverConnectionPluginFactory,com.mysql.cj.jdbc.ha.plugins.NodeMonitoringConnectionPluginFactory` when setting `connectionPluginFactories`. 

#### Enhanced Failure Monitoring Parameters
`failureDetectionTime`, `failureDetectionInterval`, and `failureDetectionCount` are similar to TCP Keep Alive parameters.

Additional monitoring configurations can be included by adding the prefix `monitoring-` to the configuration key.

| Parameter       | Value           | Required      | Description  | Default Value |
| --------------- |:---------------:|:-------------:|:------------ | ------------- |
|`failureDetectionEnabled` | Boolean | No | Set to false to disable Enhanced Failure Monitoring. | `true` |
|`failureDetectionTime` | Integer | No | Interval in milliseconds between sending a SQL query to the server and the first probe to the database node. | `30000` |
|`failureDetectionInterval` | Integer | No | Interval in milliseconds between probes to database node. | `5000` |
|`failureDetectionCount` | Integer | No | Number of failed connection checks before considering database node as unhealthy. | `3` |
|`monitorDisposalTime` | Integer | No | Interval in milliseconds for a monitor to be considered inactive and to be disposed. | `60000` |

## Extra Additions

### XML Entity Injection Fix

The default XML parser contained a security risk which made the driver prone to XML Entity Injection (XXE) attacks. To solve this issue, we disabled Document Type Definitions (DTDs) in the XML parser. If you require this restriction to be lifted in your application, you can set the `allowXmlUnsafeExternalEntity` parameter in the connection string to true. Please see the table below for a definition of this parameter. 

| Parameter       | Value           | Required      | Description  | Default Value |
| --------------- |:---------------:|:-------------:|:------------ | ------------- |
|`allowXmlUnsafeExternalEntity` | Boolean | No | Set to true if you would like to use XML inputs that refer to external entities. WARNING: Setting this to true is unsafe since your system to be prone to XXE attacks. | `false` |

### AWS IAM Database Authentication

**Note:** To preserve compatibility with customers using the community driver, IAM Authentication requires the [AWS Java SDK for Amazon RDS v1.x](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-rds) to be included separately in the classpath. The AWS Java SDK for Amazon RDS is a runtime dependency and must be resolved.

The driver supports Amazon AWS Identity and Access Management (IAM) authentication. When using AWS IAM database authentication, the host URL must be a valid Amazon endpoint, and not a custom domain or an IP address.
<br>ie. `database-mysql-name.cluster-XYZ.us-east-2.rds.amazonaws.com`


IAM database authentication is limited to certain database engines.
For more information on limitations and recommendations, please [read](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).

#### Setup for IAM database Authentication for MySQL 
1. Turn on AWS IAM database authentication for the existing database or create a new database on the AWS RDS Console:
   1. [Creating a new database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CreateDBInstance.html).
   2. [Modifying an existing database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html).
2. Create/Change and [use an AWS IAM policy](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.IAMPolicy.html) for AWS IAM database authentication.
3. [Create a database account](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.DBAccounts.html) using AWS IAM database authentication:
   1. Connect to your MySQL database using master logins, and use the following command to create a new user:<br>
   `CREATE USER example_user_name IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';`

| Parameter       | Value           | Required      | Description  | Default Value |
| --------------- |:---------------:|:-------------:|:------------ | ------------- |
|`useAwsIam` | Boolean | No | Set to true to enable AWS IAM database authentication | `false` |

###### Sample Code
```java
import java.sql.*;
import java.util.Properties;

public class AwsIamAuthenticationSample {

   private static final String CONNECTION_STRING = "jdbc:mysql:aws://database-mysql-name.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/employees";
   private static final String USER = "username";

   public static void main(String[] args) throws SQLException {
      final Properties properties = new Properties();
      // Enable AWS IAM database authentication
      properties.setProperty("useAwsIam", "true");
      properties.setProperty("user", USER);

      // Try and make a connection
      try (final Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties);
              final Statement myQuery = conn.createStatement()) {
         try (final ResultSet rs = myQuery.executeQuery("SELECT * FROM employees")) {
            while (rs.next()) {
               System.out.println(rs.getString(1));
            }
         }
      }
   }
}
```

## Development

### Setup

After installing Amazon Corretto or Java as directed in the prerequisites section, use the following command to clone the driver repository:

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

To build the driver and run the tests, you must first install [Docker](https://docs.docker.com/get-docker/).  After installing Docker, execute the following commands to create the Docker servers that the tests will run against:

```bash
$ cd aws-mysql-jdbc/docker
$ docker-compose up -d
$ cd ../
```

Then, to build the driver, run the following command:

```bash
gradlew build
```

### Running the Tests

After building the driver, and installing and configuring Docker, you can run the tests in the ```aws-mysql-jdbc``` directory with the following command:

```bash
gradlew test
```

To shut down the Docker servers when you have finished testing:

```bash
$ cd aws-mysql-jdbc/docker
$ docker-compose down && docker-compose rm
$ cd ../
```

## Known Issues
### SSLHandshakeException
Using the driver with JDKs based on OpenJDK 8u292+ or OpenJDK 11.0.11+ will result in an exception: `SSLHandshakeException: No appropriate protocol`.
This is due to OpenJDK disabling TLS 1.0 and 1.1 availability in `security.properties`. For additional information see "[Disable TLS 1.0 and TLS 1.1](https://java.com/en/configure_crypto.html#DisableTLS)".
To resolve this exception, add the `enabledTLSProtocols=TLSv1.2` connection property when connecting to a database.

## Getting Help and Opening Issues

If you encounter a bug with the AWS JDBC Driver for MySQL, we would like to hear about it. Please search the [existing issues](https://github.com/awslabs/aws-mysql-jdbc/issues) and see if others are also experiencing the issue before opening a new issue. When opening a new issue, we will need the version of AWS JDBC Driver for MySQL, Java language version, OS you’re using, and the MySQL database version you're running against. Please include a reproduction case for the issue when appropriate.

The GitHub issues are intended for bug reports and feature requests. Keeping the list of open issues lean will help us respond in a timely manner.

## Documentation

For additional documentation on the AWS JDBC Driver, [please refer to the documentation for the open-source mysql-connector-j driver that the AWS JDBC Driver is based on](https://dev.mysql.com/doc/connector-j/8.0/en/).

## License

This software is released under version 2 of the GNU General Public License (GPLv2).
