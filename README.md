# Amazon Web Services (AWS) JDBC Driver for MySQL

[![Build Status](https://github.com/awslabs/aws-mysql-jdbc/workflows/CI/badge.svg)](https://github.com/awslabs/aws-mysql-jdbc/actions?query=workflow%3A%22CI%22)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.aws.rds/aws-mysql-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/software.aws.rds/aws-mysql-jdbc)
[![Javadoc](https://javadoc.io/badge2/software.aws.rds/aws-mysql-jdbc/javadoc.svg)](https://javadoc.io/doc/software.aws.rds/aws-mysql-jdbc)
[![License: GPLv2 with FOSS exception](https://img.shields.io/badge/license-GPLv2_with_FOSS_exception-c30014.svg)](LICENSE)

**The Amazon Web Services (AWS) JDBC Driver for MySQL** allows an application to take advantage of the features of clustered MySQL databases. It is based on and can be used as a drop-in compatible for the [MySQL Connector/J driver](https://github.com/mysql/mysql-connector-j), and is compatible with all MySQL deployments.

The AWS JDBC Driver for MySQL supports fast failover for Amazon Aurora with MySQL compatibility. Support for additional features of clustered databases, including features of Amazon RDS for MySQL and on-premises MySQL deployments, is planned.

## What is Failover?

An Amazon Aurora DB cluster uses failover to automatically repairs the DB cluster status when a primary DB instance becomes unavailable. During failover, Aurora promotes a replica to become the new primary DB instance, so that the DB cluster can provide maximum availability to a primary read-write DB instance. The AWS JDBC Driver for MySQL is designed to coordinate with this behavior in order to provide minimal downtime in the event of a DB instance failure.

## Benefits of the AWS JDBC Driver for MySQL

Although Aurora is able to provide maximum availability through the use of failover, existing client drivers do not fully support this functionality. This is partially due to the time required for the DNS of the new primary DB instance to be fully resolved in order to properly direct the connection. The AWS JDBC Driver for MySQL fully utilizes failover behavior by maintaining a cache of the Aurora cluster topology and each DB instance's role (Aurora Replica or primary DB instance). This topology is provided via a direct query to the Aurora database, essentially providing a shortcut to bypass the delays caused by DNS resolution. With this knowledge, the AWS JDBC Driver can more closely monitor the Aurora DB cluster status so that a connection to the new primary DB instance can be established as fast as possible. Additionally, as noted above, the AWS JDBC Driver is designed to be a drop-in compatible for other MySQL JDBC drivers and can be used to interact with RDS and MySQL databases as well as Aurora MySQL.

## Getting Started

### Prerequisites
You need to install Amazon Corretto 8+ or Java 8+ before using the AWS JDBC Driver for MySQL.

### Installing or Upgrading the AWS JDBC Driver for MySQL

#### Direct Download of the .jar file
The AWS JDBC Driver for MySQL can be installed from pre-compiled packages that can be downloaded directly from [GitHub Releases](https://github.com/awslabs/aws-mysql-jdbc/releases) or [Maven Central](https://search.maven.org/search?q=g:software.aws.rds). To install the driver, obtain the corresponding JAR file and include it in the application's CLASSPATH:

**Example - Direct download with wget**
```bash
wget https://github.com/awslabs/aws-mysql-jdbc/releases/download/1.1.0/aws-mysql-jdbc-1.1.0.jar
cp aws-mysql-jdbc-1.1.0.jar /home/userx/libs/
export CLASSPATH=$CLASSPATH:/home/userx/libs/aws-mysql-jdbc-1.1.0.jar
```

**Upgrading to a newer version with wget**

To upgrade the driver, replace the .jar file of your earlier driver with the new `.jar` file. After replacing the `.jar` file, update the CLASSPATH to include the name of the new file. For example, to upgrade to version 1.1.0:
```bash
wget https://github.com/awslabs/aws-mysql-jdbc/releases/download/1.1.0/aws-mysql-jdbc-1.1.0.jar
cp aws-mysql-jdbc-1.1.0.jar /home/userx/libs/
export CLASSPATH=$CLASSPATH:/home/userx/libs/aws-mysql-jdbc-1.1.0.jar
```

#### As a Maven dependency
You can use [Maven's dependency management](https://search.maven.org/search?q=g:software.aws.rds) to obtain the driver by adding the following configuration to the application's Project Object Model (POM) file:

**Example - Maven**
```xml
<dependencies>
  <dependency>
    <groupId>software.aws.rds</groupId>
    <artifactId>aws-mysql-jdbc</artifactId>
    <version>1.1.0</version>
  </dependency>
</dependencies>
```

**Upgrading to a newer version with Maven**

To upgrade to a newer version of the driver, replace the version number identified in the `pom.xml` file with the newer driver version. For example, to upgrade to version 1.1.0, modify the file to include:
```xml
<dependencies>
  <dependency>
    <groupId>software.aws.rds</groupId>
    <artifactId>aws-mysql-jdbc</artifactId>
    <version>1.1.0</version>
  </dependency>
</dependencies>
```

#### As a Gradle dependency
You can use [Gradle's dependency management](https://search.maven.org/search?q=g:software.aws.rds) to obtain the driver by adding the following configuration to the application's ```build.gradle``` file:

**Example - Gradle**
```gradle
dependencies {
    implementation group: 'software.aws.rds', name: 'aws-mysql-jdbc', version: '1.1.0'
}
```

**Upgrading to a newer version with Gradle**

To upgrade to a newer version of the driver, replace the version number identified in the application's ```build.gradle``` file with the newer driver version. For example, to upgrade to version 1.1.0:
```gradle
dependencies {
    implementation group: 'software.aws.rds', name: 'aws-mysql-jdbc', version: '1.1.0'
}
```

#### Using an IDE (IntelliJ)
To use the driver with an IDE (for example, IntelliJ), download the `.jar` file and copy it into your `libs` folder.  Then, add the file to your CLASSPATH: 

**Example - IntelliJ**

```bash
wget https://github.com/awslabs/aws-mysql-jdbc/releases/download/1.1.0/aws-mysql-jdbc-1.1.0.jar
cp aws-mysql-jdbc-1.1.0.jar /home/userx/libs/
export CLASSPATH=$CLASSPATH:/home/userx/libs/aws-mysql-jdbc-1.1.0.jar
```

After downloading the `.jar` file and updating the CLASSPATH, add the driver information to your `Project`:

1. Open `Project Structure` in the IntelliJ user interface.

2. Select `Modules`.

3. Open the `Dependencies` tab for your project.

4. Add the AWS JDBC Driver for MySQL .jar file.

5. Update the connection URL prefix from jdbc:mysql:// to jdbc:mysql:aws://.

**Upgrading to a newer version with IntelliJ**

To upgrade to a newer version of the driver, download the updated driver, and add it to your CLASSPATH:  

```bash
wget https://github.com/awslabs/aws-mysql-jdbc/releases/download/1.1.0/aws-mysql-jdbc-1.1.0.jar
cp aws-mysql-jdbc-1.1.0.jar /home/userx/libs/
export CLASSPATH=$CLASSPATH:/home/userx/libs/aws-mysql-jdbc-1.1.0.jar
```

Then, follow the steps listed above to update your project to the latest version.


#### Using the Driver with a database client (DBeaver)
To use the driver with the DBeaver database client, download the `.jar` file, copy the file to your `libs` folder, and add the file to your `CLASSPATH`:

**Example - DBeaver**

```bash
wget https://github.com/awslabs/aws-mysql-jdbc/releases/download/1.1.0/aws-mysql-jdbc-1.1.0.jar
cp aws-mysql-jdbc-1.1.0.jar /home/userx/libs/
export CLASSPATH=$CLASSPATH:/home/userx/libs/aws-mysql-jdbc-1.1.0.jar
```

After downloading the .jar file and adding it to your CLASSPATH, add the driver information to the DBeaver client:

1. Open DBeaver’s `Driver Manager` window and click `New` to add the AWS JDBC Driver.

2. In `Settings`, enter the following information:

```bash
Default Port: 3306
Driver Name: AWS JDBC Driver for MySQL
Class Name: software.aws.rds.jdbc.mysql.Driver
URL Template: jdbc:mysql:aws://<endpoint>
```

3. Go to the `Libraries` tab, click `Add Artifact` and add the following dependency declaration:

```bash
<dependency>
  <groupId>software.aws.rds</groupId>
  <artifactId>aws-mysql-jdbc</artifactId>
</dependency>
```

4. DBeaver will respond: `Artifacts Found: 1`.

5. Click `OK` to finish adding the driver.

After adding driver information, you can create new connections that use the AWS JDBC Driver for MySQL.

**Upgrading to a newer version with the DBeaver Client**

To upgrade to a newer version of the driver, download the updated driver, and add it to your CLASSPATH:  

```bash
wget https://github.com/awslabs/aws-mysql-jdbc/releases/download/1.1.0/aws-mysql-jdbc-1.1.0.jar
cp aws-mysql-jdbc-1.1.0.jar /home/userx/libs/
export CLASSPATH=$CLASSPATH:/home/userx/libs/aws-mysql-jdbc-1.1.0.jar
```
Then, follow the steps listed above to update your DBeaver client settings.

### Using a Snapshot of the Driver
If there is an unreleased feature you would like to try, it may be available in a snapshot build of the driver. Snapshot builds can be found [here](https://aws.oss.sonatype.org/content/repositories/snapshots/software/aws/rds/aws-mysql-jdbc/). To use a snapshot, find the desired `.jar` file, which will be named `aws-mysql-jdbc-<version>-<date>-<time>-<snapshot-number>.jar`, and add it to your project as a dependency.

#### As a Maven dependency
```xml
<dependencies>
  <dependency>
    <groupId>software.aws.rds</groupId>
    <artifactId>aws-mysql-jdbc</artifactId>
    <version>1.1.0</version>
    <scope>system</scope>
    <systemPath>path-to-snapshot-jar</systemPath>
  </dependency>
</dependencies>
```

#### As a Gradle dependency
```gradle
dependencies {
    implementation(files("path-to-snapshot-jar"))
}
```


### Using the AWS JDBC Driver for MySQL
The AWS JDBC Driver for MySQL is drop-in compatible, so usage is identical to the [MySQL-Connector-J JDBC driver](https://github.com/mysql/mysql-connector-j). The sections below highlight driver usage specific to failover.

#### Driver Name
Use the driver name: ```software.aws.rds.jdbc.Driver```. If you are building the driver directly from main, use the driver name: ```software.aws.rds.jdbc.mysql.Driver```. You'll need the driver name when loading the driver explicitly to the driver manager.

### Connection URL Descriptions

There are many different types of URLs that can connect to an Aurora DB cluster; this section outlines the various URL types. For some URL types, the AWS JDBC Driver requires the user to provide some information about the Aurora DB cluster to provide failover functionality. For each URL type, information is provided below on how the driver will behave and what information the driver requires about the DB cluster, if applicable.

Note: The connection string follows standard URL parameters. In order to add parameters to the connection string, simply add `?` and then the `parameter_name=value` pair at the end of the connection string. You may add multiple parameters by separating the parameter name and value set (`parameter_name=value`) with the `&` symbol. For example, to add 2 parameters simply add `?param_name=value&param_2=value2` at the end of the connection string.
 

| URL Type                                      | Example           | Required Parameters  | Driver Behavior |
|-----------------------------------------------|-------------------| :-------------------:| --------------- |
| Cluster Endpoint                              | `jdbc:mysql:aws://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:3306` | None | *Initial connection:* primary DB instance<br/>*Failover behavior:* connect to the new primary DB instance |
| Read-Only Cluster Endpoint                    | `jdbc:mysql:aws://db-identifier.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:3306`     |   None |  *Initial connection:* any Aurora Replica<br/>*Failover behavior:* prioritize connecting to any active Aurora Replica but might connect to the primary DB instance if it provides a faster connection|
| Instance Endpoint                             | `jdbc:mysql:aws://instance-1.XYZ.us-east-2.rds.amazonaws.com:3306`     |    None | *Initial connection:* the instance specified (DB instance 1)<br/>*Failover behavior:* connect to the primary DB instance|
| RDS Custom Cluster                            | `jdbc:mysql:aws://db-identifier.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:3306`     |    None | *Initial connection:* any DB instance in the custom DB cluster<br/>*Failover behavior:* connect to the primary DB instance (note that this might be outside of the custom DB cluster) |
| IP Address                                    | `jdbc:mysql:aws://10.10.10.10:3306`     |    `clusterInstanceHostPattern` | *Initial connection:* the DB instance specified<br/>*Failover behavior:* connect to the primary DB instance |
| Custom Domain                                 | `jdbc:mysql:aws://my-custom-domain.com:3306`     |    `clusterInstanceHostPattern` | *Initial connection:* the DB instance specified<br/>*Failover behavior:* connect to the primary DB instance |
| Non-Aurora Endpoint                           | `jdbc:mysql:aws://localhost:3306`    |    None | A regular JDBC connection will be returned - no failover functionality |
| Aurora Endpoint<br/>(through a custom tunnel) | `jdbc:mysql:aws://localhost:3306` |    `clusterInstanceHostPattern` | *Initial connection:* the DB instance specified<br/>*Failover behavior:* connect to the primary DB instance |

Information about the `clusterInstanceHostPattern` parameter is provided in the section below.

For more information about parameters that can be configured with the AWS JDBC Driver, see the section below about failover parameters.

## Connection Plugin Manager
The connection plugin manager initializes, triggers, and cleans up a chain of connection plugins. Connection plugins are widgets attached to each `Connection` object to help execute additional or supplementary logic related to that `Connection`. [Failover](https://github.com/awslabs/aws-mysql-jdbc#failover) and [Enhanced Failure Monitoring](https://github.com/awslabs/aws-mysql-jdbc#enhanced-failure-monitoring) are both examples of connection plugins.


Failover and Enhanced Failure Monitoring are loaded by default. Additional custom plugins can be implemented and used alongside existing ones. Plugins can be loaded in a specific order to create a chain, and each plugin will pass its results to the next.
> **NOTE:** Loading custom plugins will not include Enhanced Failure Monitoring and the Failover Connection Plugin unless explicitly stated through the `connectionPluginFactories` parameter.
> 
<div style="text-align:center"><img src="./docs/files/images/connection_plugin_manager_diagram.png" /></div>

The figure above shows a simplified workflow of the connection plugin manager.

Starting at the top, when a JDBC method is executed by the driver, it is passed to the connection plugin manager. From the connection plugin manager, the JDBC method is passed in order to each plugin, and loaded like a chain. In this example, the method is passed first to `Custom Plugin A`, then to `Custom Plugin B`, and finally to `Default Plugin` which executes the JDBC method and returns the result back through the chain.

By default, the Enhanced Failure Monitoring plugin is loaded. Additional custom plugins can be implemented and used alongside existing ones. You can chain plugins together in a specified order. Loaded custom plugins will not include the Enhanced Failure Monitoring plugin unless explicitly stated with the `connectionPluginFactories` parameter.

The AWS JDBC Driver for MySQL attaches the `DefaultConnectionPlugin` to the tail of the connection plugin chain 
and actually executes the given JDBC method.

Since all the connection plugins are chained together, the earlier connection plugins affect the
latter plugins. If the connection plugin at the head of the connection plugin chain measures the
execution time, this measurement would encompass the time spent in all the connection plugins down
the chain.

To learn how to write custom plugins, refer to examples located inside [Custom Plugins Demo](https://github.com/awslabs/aws-mysql-jdbc/tree/main/src/demo/java/customplugins).

### Connection Plugin Manager Parameters
| Parameter       | Value           | Required      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                              | Default Value |
| --------------- |:---------------:|:-------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| ------------- |
| `useConnectionPlugins` | Boolean | No | When set to the default value `true`, the connection plugins will be loaded, including the Failover and Enhanced Failure Monitor plugins. When set to `false`, the connection plugins will not be loaded and the driver will instead execute JDBC methods directly. <br><br> **NOTE:** Since the failover functionality and Enhanced Failure Monitoring are implemented with plugins, disabling connection plugins will also disable such functionality. | `true` |
| `connectionPluginFactories` | String | No | String of fully-qualified class name of plugin factories that will create the plugin objects. <br/><br/>Each factory in the string should be comma-separated `,`<br/><br/>**NOTE: The order of factories declared matters.**  <br/><br/>Example: `customplugins.MethodCountConnectionPluginFactory`, `customplugins.ExecutionTimeConnectionPluginFactory,com.mysql.cj.jdbc.ha.plugins.NodeMonitoringConnectionPluginFactory`                             | `com.mysql.cj.jdbc.ha.plugins.failover.FailoverConnectionPluginFactory, com.mysql.cj.jdbc.ha.plugins.NodeMonitoringConnectionPluginFactory` |

## Failover Plugin

The failover plugin is loaded by default and can be disabled by setting parameter `enableClusterAwareFailover` to `false`.

### The AWS JDBC Driver Failover Process

<div style="text-align:center"><img src="./docs/files/images/failover_diagram.png" /></div>

The figure above provides a simplified overview of how the AWS JDBC Driver handles an Aurora failover encounter. Starting at the top of the diagram, an application with the AWS JDBC Driver on its class path uses the driver to get a logical connection to an Aurora database. 

In this example, the application requests a connection using the Aurora DB cluster endpoint and is returned a logical connection that contains a physical connection to DB instance C, the primary DB instance in the DB cluster. Due to how the application operates against the logical connection, the physical connection details about which specific DB instance it is connected to have been abstracted away. 

Over the course of the application's lifetime, it executes various statements against the logical connection. If DB instance C is stable and active, these statements succeed and the application continues as normal. If DB instance C later experiences a failure, Aurora will initiate failover to promote a replica to a new primary DB instance (DB instance A or B in this case). At the same time, the AWS JDBC Driver will intercept the related communication exception and kick off its own internal failover process. 

In this case, in which the primary DB instance has failed, the driver will use its internal topology cache to temporarily connect to an active Aurora Replica. The topology cache will contain  information regarding which instances are available. The active Aurora Replica the driver has connected to will be periodically queried for the DB cluster topology until the new primary DB instance is identified (DB instance A or B). 

At this point, the driver will connect to the new primary DB instance and return control to the application by raising a SQLException with SQLState 08S02 so that they can reconfigure their session state as required. Although the DNS endpoint for the DB cluster might not yet resolve to the new primary DB instance, the driver has already discovered this new DB instance during its failover process and will be directly connected to it when the application continues executing statements. 

To the application, the logical connection will not appear to have changed, but the physical connection will have been swapped to the new primary DB instance. In this way the driver provides a faster way to reconnect to a newly promoted DB instance, thus increasing the availability of the DB cluster.

### Setting the socket timeout

If you turn off the enhanced failure monitoring plugin, which is loaded by default, it is highly recommended that you set a socket timeout according to your application's needs. This can be done via the `socketTimeout` connection property. Without enhanced failure monitoring or a socket timeout, the failover plugin may take a long time to detect a failover event.  We recommend that you set the socket timeout to a value slightly higher than the maximum time that you expect your queries to run. If you are using the enhanced failure monitoring plugin, setting the socket timeout is not required - the plugin will automatically detect any problems with the connection.

### Failover Parameters

In addition to [the parameters that you can configure for the MySQL Connector/J driver](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html), you can pass the following parameters to the AWS JDBC Driver through the connection URL to specify additional driver behavior.

| Parameter       | Value           | Required      | Description  | Default Value |
| --------------- |:---------------:|:-------------:|:------------ | ------------- |
|`enableClusterAwareFailover` | Boolean | No | Set to `true` to enable the fast failover behavior offered by the AWS JDBC Driver. Set to `false` for simple JDBC connections that do not require fast failover functionality. <br><br> **NOTE:** In addition to this parameter, since the failover functionality is implemented with [connection plugins](#Connection-Plugin-Manager), disabling [`useConnectionPlugins`](#Connection-Plugin-Manager-Parameters) will also disable the failover functionality. When `useConnectionPlugins` is enabled, the failover plugin will be loaded by default regardless of whether `enableClusterAwareFailover` is true or false. | `true` |
|`clusterInstanceHostPattern` | String | If connecting using an IP address or custom domain URL: Yes<br/>Otherwise: No | This parameter is not required unless connecting to an AWS RDS cluster via an IP address or custom domain URL. In those cases, this parameter specifies the cluster instance DNS pattern that will be used to build a complete instance endpoint. A "?" character in this pattern should be used as a placeholder for the DB instance identifiers of the instances in the cluster. <br/><br/>Example: `?.my-domain.com`, `any-subdomain.?.my-domain.com:9999`<br/><br/>Usecase Example: If your cluster instance endpoints followed this pattern:`instanceIdentifier1.customHost`, `instanceIdentifier2.customHost`, etc. and you wanted your initial connection to be to `customHost:1234`, then your connection string should look something like this: `jdbc:mysql:aws://customHost:1234/test?clusterInstanceHostPattern=?.customHost` | If the provided connection string is not an IP address or custom domain, the driver will automatically acquire the cluster instance host pattern from the customer-provided connection string. |
|`clusterId` | String | No | A unique identifier for the cluster. Connections with the same cluster ID share a cluster topology cache. This connection parameter is not required and thus should only be set if desired. | The driver will automatically acquire a cluster id for AWS RDS clusters. |
|`clusterTopologyRefreshRateMs` | Integer | No | Cluster topology refresh rate in milliseconds. The cached topology for the cluster will be invalidated after the specified time, after which it will be updated during the next interaction with the connection. | `30000` |
|`failoverTimeoutMs` | Integer | No | Maximum allowed time in milliseconds to attempt reconnecting to a new writer or reader instance after a cluster failover is initiated. | `60000` |
|`failoverClusterTopologyRefreshRateMs` | Integer | No | Cluster topology refresh rate in milliseconds during a writer failover process. During the writer failover process, cluster topology may be refreshed at a faster pace than normal to speed up discovery of the newly promoted writer. | `5000` |
|`failoverWriterReconnectIntervalMs` | Integer | No | Interval of time in milliseconds to wait between attempts to reconnect to a failed writer during a writer failover process. | `5000` |
|`failoverReaderConnectTimeoutMs` | Integer | No | Maximum allowed time in milliseconds to attempt to connect to a reader instance during a reader failover process. | `5000` |
|`acceptAwsProtocolOnly` | Boolean | If using simultaneously with another MySQL driver that supports the same protocols: Yes<br/>Otherwise: No | Set to true to only accept connections for URLs with the jdbc:mysql:aws:// protocol. This setting should be set to true when running an application that uses this driver simultaneously with another MySQL driver that supports the same protocols (e.g. the MySQL JDBC Driver), to ensure the driver protocols do not clash. This behavior can also be set at the driver level for every connection via the Driver.setAcceptAwsProtocolOnly method; however, this connection parameter will take priority when present. | `false` |
|`gatherPerfMetrics` | Boolean | No | Set to true if you would like the driver to record failover-associated metrics. <br><br>To print collected metrics, call `IClusterAwareMetricsReporter.reportMetrics(String connUrl, Log log)`. <br>e.g. `IClusterAwareMetricsReporter.reportMetrics(db-identifier.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:3306, myLog)` <br><br>To reset collected metrics, call `IClusterAwareMetricsReporter.reportMetrics()`. | `false` | 
|`gatherMetricsPerInstance` | Boolean | No | Set to true to gather additional performance metrics per instance as well as cluster. Set to false to only gather performance metrics per cluster. <br><br>To print collected metrics at instance level, call `IClusterAwareMetricsReporter.reportMetrics(String connUrl, Log log, true)`. | `false` | 
|`allowXmlUnsafeExternalEntity` | Boolean | No | Set to true if you would like to use XML inputs that refer to external entities. WARNING: Setting this to true is unsafe since your system to be prone to XXE attacks. | `false` |
|`socketTimeout` | Integer | Recommended if you turn off the enhanced failure monitoring plugin | Timeout (in milliseconds) on network socket operations (0, the default means no timeout). | `0` (No timeout) |

### Failover Exception Codes
#### 08001 - Unable to Establish SQL Connection
When the driver throws a SQLException with code ```08001```, the original connection failed, and the driver tried to failover to a new instance, but was not able to. There are various reasons this may happen: no nodes were available, a network failure occurred, and so on. In this scenario, please wait until the server is up or other problems are solved (an exception will be thrown.)

#### 08S02 - Communication Link 
When the driver throws a SQLException with code ```08S02```, the original connection failed while autocommit was set to true, and the driver successfully failed over to another available instance in the cluster. However, any session state configuration of the initial connection is now lost. In this scenario, you should:

- Reconfigure and reuse the original connection (the reconfigured session state will be the same as the original connection).

- Repeat the query that was executed when the connection failed and continue work as desired.

#### Sample Code
```java
import java.sql.*;

/**
 * Scenario 1: Failover happens when autocommit is set to true - Catch SQLException with code 08S02.
 */
public class FailoverSampleApp1 {

   private static final String CONNECTION_STRING = "jdbc:mysql:aws://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/employees";
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

#### 08007 - Transaction Resolution Unknown
When the driver throws a SQLException with code ```08007```, the original connection failed within a transaction (while autocommit was set to false). In this scenario, the driver first attempts to rollback the transaction and then fails over to another available instance in the cluster. Note that the rollback might be unsuccessful as the initial connection may be broken at the time that the driver recognizes the problem. Note also that any session state configuration of the initial connection is now lost. In this scenario, you should:

- Reconfigure and reuse the original connection (the reconfigured session state will be the same as the original connection).

- Re-start the transaction and repeat all queries which were executed during the transaction before the connection failed.

- Repeat the query that was executed when the connection failed and continue work as desired.

#### Sample Code
```java
import java.sql.*;

/**
 * Scenario 2: Failover happens when autocommit is set to false - Catch SQLException with code 08007.
 */
public class FailoverSampleApp2 {

   private static final String CONNECTION_STRING = "jdbc:mysql:aws://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/employees";
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

### Connection Pooling
The AWS JDBC Driver for MySQL is compatible with connection pooling, but some connection pooling libraries may contain additional behaviour when dealing with SQL exceptions. This means the exceptions created by the AWS JDBC Driver for MySQL may not be recognized, and depending on the connection pool, connections may be closed prematurely due to the unrecognized SQL exceptions. Users are recommended to investigate the connection pooling library of their choice, and to implement any required additional code to allow the connection pool to accept the exceptions raised by the driver. For example, the library HikariCP has an interface that users can implement to prevent connections from being closed immediately after failover, as seen below.

#### Sample Code
```java
import com.zaxxer.hikari.SQLExceptionOverride;

import java.sql.SQLException;

public class HikariCPSQLException implements SQLExceptionOverride {
    public Override adjudicate(final SQLException sqlException) {
        String sqlState = sqlException.getSQLState();
        if (sqlState.equalsIgnoreCase("08S02") ||
            sqlState.equalsIgnoreCase("08007")) {
            return Override.DO_NOT_EVICT;
        } else {
            return Override.CONTINUE_EVICT;
        }
    }
}
```

>### :warning: Warnings About Proper Usage of the AWS JDBC Driver for MySQL
>1. A common practice when using JDBC drivers is to wrap invocations against a Connection object in a try-catch block, and dispose of the Connection object if an Exception is hit. If your application takes this approach, it will lose the fast-failover functionality offered by the Driver. When failover occurs, the Driver internally establishes a ready-to-use connection inside the original Connection object before throwing an exception to the user. If this Connection object is disposed of, the newly established connection will be thrown away. The correct practice is to check the SQL error code of the exception and reuse the Connection object if the error code indicates successful failover. [FailoverSampleApp1](#sample-code) and [FailoverSampleApp2](#sample-code-1) demonstrate this practice. See the section below on [Failover Exception Codes](#failover-exception-codes) for more details.
>2. It is highly recommended that you use the cluster and read-only cluster endpoints instead of the direct instance endpoints of your Aurora cluster, unless you are confident about your application's usage of instance endpoints. Although the Driver will correctly failover to the new writer instance when using instance endpoints, use of these endpoints is discouraged because individual instances can spontaneously change reader/writer status when failover occurs. The driver will always connect directly to the instance specified if an instance endpoint is provided, so a write-safe connection cannot be assumed if the application uses instance endpoints.

## Enhanced Failure Monitoring
<div style="text-align:center"><img src="./docs/files/images/enhanced_failure_monitoring_diagram.png" /></div>
The figure above shows a simplified workflow of Enhanced Failure Monitoring. Enhanced Failure Monitoring is a connection plugin implemented by a monitor thread. The monitor will periodically check the connected database node's health. If a database node is determined to be unhealthy, the query will be retried with a new database node and the monitor restarted. 

The Enhanced Failure Monitoring plugin is loaded by default, and can be disabled by setting parameter `failureDetectionEnabled` to `false`. 

If custom connection plugins are loaded, Enhanced Failure Monitoring and Failover Connection Plugin 
will NOT be loaded unless explicitly included by adding `com.mysql.cj.jdbc.ha.plugins.failover.FailoverConnectionPluginFactory,com.mysql.cj.jdbc.ha.plugins.NodeMonitoringConnectionPluginFactory` to `connectionPluginFactories`. 

### Enhanced Failure Monitoring Parameters
`failureDetectionTime`, `failureDetectionInterval`, and `failureDetectionCount` are similar to TCP Keep Alive parameters.

You can include additional monitoring configurations by adding the prefix `monitoring-` to the configuration key.

| Parameter       | Value           | Required      | Description  | Default Value |
| --------------- |:---------------:|:-------------:|:------------ | ------------- |
|`failureDetectionEnabled` | Boolean | No | Set to `true` to enable Enahanced Failure Monitoring. Set to `false` to disable it. <br><br> **NOTE:** In addition to this parameter, since the Enhanced Failure Monitor is implemented with [connection plugins](#Connection-Plugin-Manager), disabling [`useConnectionPlugins`](#Connection-Plugin-Manager-Parameters) will also disable the Enhanced Failure Monitor functionality. When `useConnectionPlugins` is enabled, the Enhanced Failure Monitor plugin will be loaded by default regardless of whether `failureDetectionEnabled` is true or false. | `true` |
|`failureDetectionTime` | Integer | No | Interval in milliseconds between sending a SQL query to the server and the first probe to the database node. | `30000` |
|`failureDetectionInterval` | Integer | No | Interval in milliseconds between probes to database node. | `5000` |
|`failureDetectionCount` | Integer | No | Number of failed connection checks before considering database node as unhealthy. | `3` |
|`monitorDisposalTime` | Integer | No | Interval in milliseconds for a monitor to be considered inactive and to be disposed. | `60000` |

>### :warning: Warnings About Usage of the AWS JDBC Driver for MySQL with RDS Proxy
> Using RDS Proxy endpoints with AWS JDBC Driver for MySQL with Enhanced Failure Monitoring plugin doesn't cause any critical issue. However such approach isn't recommended. The main reason is that RDS Proxy transparently re-routes driver requests to one of database instances. RDS Proxy decides which database instance is used to based on many criteria and it's on per request basis. Such switching from between different instances makes the plugin useless in terms of instance health monitoring. The plugin couldn't identify what actual instance it's connected to and which one it's monitoring. That could be a source of false positive failure detections. At the same time the plugin can still proactively monitor network connectivity to RDS Proxy endpoints and report outages back to a user application if they occur.
> 
> It is suggested to turn off Enhanced Failure Monitoring plugin, or to avoid using RDS Proxy endpoints when the plugin is active. 

## AWS Secrets Manager Plugin

The AWS JDBC Driver for MySQL supports usage of database credentials stored in the [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) through the AWS Secrets Manager Plugin. This plugin is optional and can be enabled with the `connectionPluginFactories` parameter as seen in the [connection plugin manager parameters table](#connection-plugin-manager-parameters). When a user creates a new connection with this plugin enabled, the plugin will retrieve the secret and the connection will be created using those credentials.

> **Note:** To use this plugin, you must include the runtime dependencies [Jackson Databind](https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind) and [AWS Secrets Manager](https://mvnrepository.com/artifact/software.amazon.awssdk/secretsmanager) in your project.

The following properties are required for the AWS Secrets Manager Plugin to retrieve secrets.

| Parameter                | Value  | Required | Description                                             | Example          |
|--------------------------|:------:|:--------:|:--------------------------------------------------------|:-----------------|
| `secretsManagerSecretId` | String |   Yes    | Set this value to be the secret name or the secret ARN. | `test-secret-id` |
| `secretsManagerRegion`   | String |   Yes    | Set this value to be the region your secret is in.      | `us-east-1`      |

### Example
```java
import java.sql.*;
import java.util.Properties;
import software.aws.rds.jdbc.mysql.shading.com.mysql.cj.jdbc.ha.plugins.AWSSecretsManagerPluginFactory;

public class AWSSecretsManagerPluginSample {

   private static final String CONNECTION_STRING = "jdbc:mysql:aws://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/employees";
   private static final String SECRET_ID = "secretId";
   private static final String REGION = "us-east-1";

   public static void main(String[] args) throws SQLException {
      final Properties properties = new Properties();
      // Enable the AWS Secrets Manager Plugin:
      properties.setProperty("connectionPluginFactories", AWSSecretsManagerPluginFactory.class.getName());
      
      // Set these properties so secrets can be retrieved:
      properties.setProperty("secretsManagerSecretId", SECRET_ID);
      properties.setProperty("secretsManagerRegion", REGION);

      // Try and make a connection:
      try (final Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties);
           final Statement statement = conn.createStatement();
           final ResultSet rs = statement.executeQuery("SELECT * FROM employees")) {
         while (rs.next()) {
            System.out.println(rs.getString("first_name"));
         }
      }
   }
}
```

## Read-Write Splitting Plugin

The read-write splitting plugin adds functionality to switch between writer/reader instances via calls to the `JdbcConnection#setReadOnly` method. Upon calling `setReadOnly(true)`, the plugin will establish a connection to a random reader instance and direct subsequent queries to this instance. Future calls to `setReadOnly` will switch between the established writer and reader connections according to the boolean argument you supply to the `setReadOnly` method.

### Session State Limitations with the Read-Write Splitting Plugin

There are many session state attributes that can change during a session, and many ways to change them. Consequently, the read-write splitting plugin has limited support for transferring session state between connections. The following attributes will be automatically transferred when switching connections:

- autocommit value
- current selected database
- transaction isolation level
- SQL SELECT max-rows limit

All other session state attributes will be lost when switching connections. There are two scenarios when the plugin may switch a connection:
1. You have loaded the plugin but have kept reader load balancing disabled. In this case, the connection will switch between the writer/reader when calling `setReadOnly`.
2. You have loaded the plugin and have enabled reader load balancing. In this case, the connection will still switch between the writer/reader when calling `setReadOnly`. It will also switch at transaction boundaries. See the section on [reader load balancing](#reader-load-balancing) for more information on what is consided a transaction boundary.

If your SQL workflow depends on session state attributes that are not mentioned above, you will need to re-configure those attributes whenever the connection is switched. If you have loaded the plugin but have kept reader load balancing disabled, you will need to re-configure these attributes after each call to `setReadOnly`. If reader load balancing is enabled, you will also need to re-configure these attributes after each transaction boundary. Since reader load balancing frequently switches the connection, we recommend that you keep it disabled if your workflow depends on session state attributes that are not automatically transferred.

### Loading the Read-Write Splitting Plugin

The read-write splitting plugin is not loaded by default. To load the plugin, set the `connectionPluginFactories` connection parameter:

```
final Properties properties = new Properties();
properties.setProperty("connectionPluginFactories", ReadWriteSplittingPluginFactory.class.getName());
```

If you would like to load the read-write splitting plugin alongside the failover and enhanced failure monitoring plugins, the read-write splitting plugin must be the first plugin in the connection chain, otherwise failover exceptions will not be properly processed by the plugin:

```
final Properties properties = new Properties();
properties.setProperty(
        "connectionPluginFactories",
        String.format("%s,%s,%s",
                ReadWriteSplittingPluginFactory.class.getName(),
                FailoverConnectionPluginFactory.class.getName(),
                NodeMonitoringConnectionPluginFactory.class.getName())
```

### Reader Load Balancing

The plugin can also load balance queries among available reader instances by enabling the `loadBalanceReadOnlyTraffic` connection parameter. This parameter is disabled by default. To enable it, set the following connection parameter:

```
final Properties properties = new Properties();
properties.setProperty("loadBalanceReadOnlyTraffic", "true");
```

Once this parameter is enabled, queries will be load balanced among reader instances after calling `setReadOnly(true)` on the Connection object. Load balancing will switch to a new randomly selected reader instance at each transaction boundary.

### Limitations with Reader Load Balancing

When reader load balancing is enabled, the read-write splitting plugin will analyze methods and statements executed against the Connection object to determine when the connection is at a transaction boundary. This analysis does not support SQL strings containing multiple statements. If your SQL strings contain multiple statements, we recommend that you do not enable reader load balancing as the resulting behavior is not defined.

### Using the Read-Write Splitting Plugin against RDS/Aurora Clusters

When using the read-write splitting plugin against RDS or Aurora clusters, the plugin automatically acquires the cluster topology by querying the cluster. Because of this functionality, you do not have to supply multiple instance URLs in the connection string. Instead, supply just the URL for the initial instance to which you're connecting.

### Using the Read-Write Splitting Plugin against Non-RDS Clusters

If you are using the read-write splitting plugin against a cluster that is not hosted on RDS or Aurora, the plugin will not be able to automatically acquire the cluster topology. Instead, you must supply the topology information in the connection string as a comma-delimited list of multiple instance URLs. The first instance in the list must be the writer instance:

```
String connectionUrl = "jdbc:mysql:aws://writer-instance-1.com,reader-instance-1.com,reader-instance-2.com/database-name"
```

### Read Write Splitting Plugin Parameters

| Parameter | Value | Required | Description | Default Value |
| --- | --- | --- | --- | --- |
| `loadBalanceReadOnlyTraffic` | Boolean | No  | Set to `true` to load balance queries among available reader instances. Once enabled, load balancing will automatically be performed for reader instances when the connection has been set to read-only mode via `JdbcConnection#setReadOnly` | `false` |

## Extra Additions

### XML Entity Injection Fix

The default XML parser contained a security risk which made the driver prone to XML Entity Injection (XXE) attacks. To solve this issue, we disabled Document Type Definitions (DTDs) in the XML parser. If you require this restriction to be lifted in your application, you can set the `allowXmlUnsafeExternalEntity` parameter in the connection string to true. Please see the table below for a definition of this parameter. 

| Parameter       | Value           | Required      | Description  | Default Value |
| --------------- |:---------------:|:-------------:|:------------ | ------------- |
|`allowXmlUnsafeExternalEntity` | Boolean | No | Set to true if you would like to use XML inputs that refer to external entities. WARNING: Setting this to true is unsafe since your system to be prone to XXE attacks. | `false` |

### AWS IAM Database Authentication

**Note:** To preserve compatibility with customers using the community driver, IAM Authentication requires the [AWS Java SDK RDS v2.x](https://mvnrepository.com/artifact/software.amazon.awssdk/rds) to be included separately in the classpath. The AWS Java SDK RDS is a runtime dependency and must be resolved.

The driver supports Amazon AWS Identity and Access Management (IAM) authentication. When using AWS IAM database authentication, the host URL must be a valid Amazon endpoint, and not a custom domain or an IP address.
<br>ie. `db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com`


IAM database authentication use is limited to certain database engines. For more information on limitations and recommendations, please [review the IAM documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).

#### Setup for IAM database Authentication for MySQL 
1. Turn on AWS IAM database authentication for the existing database or create a new database on the AWS RDS Console:
   1. If needed, review the documentation about [creating a new database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CreateDBInstance.html).
   2. If needed, review the documentation about [modifying an existing database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html).
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

   private static final String CONNECTION_STRING = "jdbc:mysql:aws://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/employees";
   private static final String USER = "username";

   public static void main(String[] args) throws SQLException {
      final Properties properties = new Properties();
      // Enable AWS IAM database authentication
      properties.setProperty("useAwsIam", "true");
      properties.setProperty("user", USER);

      // Try and make a connection
      try (final Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties);
           final Statement statement = conn.createStatement();
           final ResultSet rs = statement.executeQuery("SELECT * FROM employees")) {
         while (rs.next()) {
            System.out.println(rs.getString("first_name"));
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

To build the AWS JDBC Driver, navigate into the aws-mysql-jdbc directory and run the following command:

```bash
gradlew build
```

To build the driver without running the tests, run the following command:

```bash
gradlew build -x test
```

### Running the Tests

To run the tests, you must first install [Docker](https://docs.docker.com/get-docker/). After building the driver, and installing and configuring Docker, you can run the tests in the `aws-mysql-jdbc` directory with the following command:

```bash
gradlew test
```

To run tests for the base driver functionality, use the following command:
```bash
gradlew test-community-docker
```

To run the integration tests, you will need some environment variables and an AWS User. Integration tests are set up to create clusters for each test. The AWS User will need access to do the following: create/delete Aurora clusters for testing, AWS IAM access for RDS, and allowlisting IP address of the current runner in EC2 Security Group. 
Descriptions of the necessary environment variables is in the table below.

To run the integration tests on macOS, use the following command (replace the variables with the appropriate values):
```bash
AWS_ACCESS_KEY_ID=ASIAIOSFODNN7EXAMPLE AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY AWS_SESSION_TOKEN=AQoDYXdzEJr...<remainder of session token> ./gradlew test-integration-docker
```

To run the integration tests on Windows, use the following command (replace the variables with the appropriate values):
```bash
cmd /c "SET AWS_ACCESS_KEY_ID=ASIAIOSFODNN7EXAMPLE & SET AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY & SET AWS_SESSION_TOKEN=AQoDYXdzEJr...<remainder of session token> .\gradlew test-integration-docker
```

#### Environment Variables
| Environment Variable | Required        | Description  | Example Value |
| -------------------- |:---------------:|:------------:| ------------- |
| `AWS_ACCESS_KEY_ID` | Yes | Access keys for CLI, SDK, & API access | ASIAIOSFODNN7EXAMPLE |
| `AWS_SECRET_ACCESS_KEY` | Yes | Secret Access keys for CLI, SDK, & API access | wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY |
| `AWS_SESSION_TOKEN` | Required if using [temporary AWS credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html). | AWS Session Token for CLI, SDK, & API access. | AQoDYXdzEJr...<remainder of session token> |
| `TEST_DB_CLUSTER_IDENTIFIER` | No, but suggested to set | The database identifier for your Aurora cluster. It is suggested to have this set to avoid conflicting with existing clusters. | db-identifier |
| `TEST_USERNAME` | No | The username to access the database. | username |
| `TEST_PASSWORD` | No | The database cluster password. | password |
| `TEST_DB_USER` | No | User within the database that is identified with AWS IAM database authentication. This is used for AWS IAM authentication. | jane_doe |

### Enable Logging
Enable logging by specifying the `logger` parameter in the connection string. For more information on the configuration parameter, see [Debugging/Profiling](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-debugging-profiling.html).
The driver currently does not support custom logging outside the usual logging frameworks like SLF4J. For more information on using SLF4J with the driver see [here](https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-logging-slf4j.html).

## Known Issues
### SSLHandshakeException
Using the driver with JDKs based on OpenJDK 8u292+ or OpenJDK 11.0.11+ will result in an exception: `SSLHandshakeException: No appropriate protocol`.
This is due to OpenJDK disabling TLS 1.0 and 1.1 availability in `security.properties`. For additional information see "[Disable TLS 1.0 and TLS 1.1](https://java.com/en/configure_crypto.html#DisableTLS)".
To resolve this exception, add the `enabledTLSProtocols=TLSv1.2` connection property when connecting to a database.

### Password Expiration With AWS Secrets Manager
When using the driver with AWS Secrets Manager, the driver does not automatically update expired credentials **during failover**.
The driver only handles expired credentials when opening new connections.
If the credentials expire during failover, the driver raises a SQLException with SQLState 28000 to the client application.
A possible solution to handle password expiration during failover is to catch SQLException with SQLState 28000 and create a new connection.

```java
import java.sql.*;
import java.util.Properties;
import com.mysql.cj.jdbc.ha.plugins.AWSSecretsManagerPluginFactory;
import com.mysql.cj.jdbc.ha.plugins.failover.FailoverConnectionPluginFactory;
import com.mysql.cj.jdbc.ha.plugins.NodeMonitoringConnectionPluginFactory;
import com.mysql.cj.log.StandardLogger;

public class AWSSecretsManagerPluginSample2 {

   private static final String CONNECTION_STRING = "jdbc:mysql:aws://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/employees";
   private static final String SECRET_ID = "secretId";
   private static final String REGION = "us-east-1";
   private static final int MAX_RETRIES = 5;

   public static void main(String[] args) throws SQLException {
      final Properties properties = new Properties();
      properties.setProperty("logger", StandardLogger.class.getName());

      // Enable the AWS Secrets Manager Plugin:
      properties.setProperty(
              "connectionPluginFactories",
              String.format("%s,%s,%s",
                      AWSSecretsManagerPluginFactory.class.getName(),
                      FailoverConnectionPluginFactory.class.getName(),
                      NodeMonitoringConnectionPluginFactory.class.getName())
      );

      // Set these properties so secrets can be retrieved:
      properties.setProperty("secretsManagerSecretId", SECRET_ID);
      properties.setProperty("secretsManagerRegion", REGION);

      try (ResultSet rs = executeQueryWithPasswordExpirationHandling("SELECT * FROM employees", properties)) {
         while (rs.next()) {
            System.out.println(rs.getString("first_name"));
         }
      }
   }

   private static ResultSet executeQueryWithPasswordExpirationHandling(
           String query,
           Properties properties) throws SQLException {
      int retries = 0;
      boolean reconnect = false;
      Connection connection = null;

      while (true) {
         try {
            if (connection == null || reconnect) {
               connection = DriverManager.getConnection(CONNECTION_STRING, properties);
            }
            Statement statement = connection.createStatement();
            return statement.executeQuery(query);
         } catch (SQLException e) {
            // If the attempt to connect has failed MAX_RETRIES times,
            // throw the exception to inform users of the failed connection.
            if (retries > MAX_RETRIES) {
               throw e;
            }

            if ("28000".equalsIgnoreCase(e.getSQLState())) {
               // Failover has occurred and failed due to access denied error.
               // Create a new connection and re-execute the query.
               reconnect = true;
               retries++;
            } else if ("08S02".equalsIgnoreCase(e.getSQLState())) {
               // Failover has occurred and the driver has failed over to another instance successfully.
               // Re-execute the query.
               retries++;
            } else {
               throw e;
            }
         }
      }
   }
}

```


## Getting Help and Opening Issues

If you encounter a bug with the AWS JDBC Driver for MySQL, we would like to hear about it. Please search the [existing issues](https://github.com/awslabs/aws-mysql-jdbc/issues) and see if others are also experiencing the issue before opening a new issue. When opening a new issue, we will need the version of AWS JDBC Driver for MySQL, Java language version, OS you’re using, and the MySQL database version you're running against. Please include a reproduction case for the issue when appropriate.

The GitHub issues are intended for bug reports and feature requests. Keeping the list of open issues lean will help us respond in a timely manner.

## Documentation

For additional documentation on the AWS JDBC Driver, [please refer to the documentation for the open-source mysql-connector-j driver that the AWS JDBC Driver is based on](https://dev.mysql.com/doc/connector-j/8.0/en/).

## License

This software is released under version 2 of the GNU General Public License (GPLv2).
