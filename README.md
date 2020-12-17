# Amazon Web Services (AWS) JDBC Driver for MySQL

The AWS JDBC Driver for MySQL is a client driver designed to take full advantage of the high database availability offered by Aurora MySQL. In the event of a database failure, this availability is provided using a process known as failover, which is described below.

The AWS JDBC Driver is a drop-in replacement for the open-source [MySQL-Connector-J JDBC driver](https://github.com/mysql/mysql-connector-j) and is based on it. You can use the AWS JDBC Driver in place to interact with Aurora MySQL, RDS MySQL, and on-premise MySQL, without experiencing any unexpected or undesired behavior.

## What is Failover?

In an Amazon Aurora DB cluster, failover is a mechanism by which Aurora automatically repairs the DB cluster status when a primary DB instance becomes unavailable. It achieves this goal by electing an Aurora Replica to become the new primary DB instance, so that the DB cluster can provide maximum availability to a primary read-write DB instance. The AWS JDBC Driver for MySQL is designed to coordinate with this behavior in order to provide minimal downtime in the event of a DB instance failure.

## Benefits of the AWS JDBC Driver for MySQL

Although Aurora is able to provide maximum availability through the use of failover, existing client drivers do not currently take full advantage of this functionality. This is partially due to the time required for the DNS of the new primary DB instance to be fully resolved in order to properly direct the connection. The AWS JDBC Driver for MySQL fully utilizes failover behavior by maintaining a cache of the Aurora cluster topology and each DB instance's role (Aurora Replica or primary DB instance). This topology is provided via a direct query to the Aurora database, essentially providing a shortcut to bypass the delays caused by DNS resolution. With this knowledge, the AWS JDBC Driver can more closely monitor the Aurora DB cluster status so that a connection to the new primary DB instance can be established as fast as possible. Additionally, as noted above, the AWS JDBC Driver is designed to be a drop-in replacement for other MySQL JDBC drivers and can be used to interact with regular RDS and MySQL databases as well as Aurora MySQL.

## The AWS JDBC Driver Failover Process

<div style="text-align:center"><img src="./doc/failover_diagram.png" /></div>

The figure above provides a simplified overview of how the AWS JDBC Driver handles an Aurora failover encounter. Starting at the top of the diagram, an application with the AWS JDBC Driver on its class path uses the driver to get a logical connection to an Aurora database. In this example, the application requests a connection using the Aurora DB cluster endpoint and is returned a logical connection that is physically connected to the primary DB instance in the DB cluster, DB instance C. Due to how the application operates against the logical connection, the physical connection details about which specific DB instance it is connected to have been abstracted away. Over the course of the application's lifetime, it executes various statements against the logical connection. If DB instance C is stable and active, these statements succeed and the application continues as normal. If DB instance C later experiences a failure, Aurora will initiate failover to promote a new primary DB instance. At the same time, the AWS JDBC Driver will intercept the related communication exception and kick off its own internal failover process. In this case, in which the primary DB instance has failed, the driver will use its internal topology cache to temporarily connect to an active Aurora Replica. This Aurora Replica will be periodically queried for the DB cluster topology until the new primary DB instance is identified (DB instance A or B in this case). At this point, the driver will connect to the new primary DB instance and return control to the application by raising a SQLException with SQLState 08S02 so that they can re-configure their session state as required. Although the DNS endpoint for the DB cluster might not yet resolve to the new primary DB instance, the driver has already discovered this new DB instance during its failover process and will be directly connected to it when the application continues executing statements. In this way the driver provides a faster way to reconnect to a newly promoted DB instance, thus increasing the availability of the DB cluster.

## Requirements

The AWS JDBC Driver requires Amazon Corretto 8+ or Java 8+.

To install Amazon Corretto, see the [Amazon Corretto user guides](https://docs.aws.amazon.com/corretto/index.html).

## Getting Started

For an example of how to use the AWS JDBC Driver, including coding samples, see "Getting started with AWS JDBC driver for MySQL" [TODO: insert link here]

### Connection URL Descriptions

There are many different types of URLs that can connect to an Aurora DB cluster. For some of these URL types, the AWS JDBC Driver requires the user to provide some information about the Aurora DB cluster to provide failover functionality. This section outlines the various URL types. For each type, information is provided on how the driver will behave and what information the driver requires about the DB cluster, if applicable.

| URL Type        | Example           | Required Parameters  | Driver Behavior |
| ------------- |-------------| :-----:| --- |
| Cluster Endpoint      | `jdbc:mysql://db-identifier.cluster-czygpppufgy4.us-east-2.rds.amazonaws.com:3306` | None | *Initial connection:* primary DB instance<br/>*Failover behavior:* connect to the new primary DB instance |
| Read-Only Cluster Endpoint      | `jdbc:mysql://db-identifier.cluster-ro-czygpppufgy4.us-east-2.rds.amazonaws.com:3306`      |   None |  *Initial connection:* any Aurora Replica<br/>*Failover behavior:* prioritize connecting to any active Aurora Replica but might connect to the primary DB instance if it provides a faster connection|
| Instance Endpoint | `jdbc:mysql://instance-1.czygpppufgy4.us-east-2.rds.amazonaws.com:3306`      |    None | *Initial connection:* the instance specified (DB instance 1)<br/>*Failover behavior:* connect to the primary DB instance|
| RDS Custom Cluster | `jdbc:mysql://db-identifier.cluster-custom-czygpppufgy4.us-east-2l.rds.amazonaws.com:3306`      |    None | *Initial connection:* any DB instance in the custom DB cluster<br/>*Failover behavior:* connect to the primary DB instance (note that this might be outside of the custom DB cluster) |
| IP Address | `jdbc:mysql://10.10.10.10:3306`      |    `clusterInstanceHostPattern` | *Initial connection:* the DB instance specified<br/>*Failover behavior:* connect to the primary DB instance |
| Custom Domain | `jdbc:mysql://my-custom-domain.com:3306`      |    `clusterInstanceHostPattern` | *Initial connection:* the DB instance specified<br/>*Failover behavior:* connect to the primary DB instance |
| Non-Aurora Endpoint | `jdbc:mysql://localhost:3306`     |    `clusterInstanceHostPattern` | A regular JDBC connection will be returned - no failover functionality |

For more information about parameters that can be configured with the AWS JDBC Driver, see "Getting started with AWS JDBC driver for MySQL" [TODO: insert link here]

## Development

### Setup

After you have installed Amazon Corretto or Java according to the requirements section, use the below command to clone the driver repository.

```bash
$ git clone https://github.com/awslabs/aws-mysql-jdbc.git
$ cd mysql-connector-j
```

Changes can now be made in the repository.
### Building the AWS JDBC Driver for MySQL

To build the AWS JDBC Driver without running the tests, navigate into the aws-mysql-jdbc directory and run the following command:

```bash
gradlew build -x test
```

To build the driver and run the tests, Docker must be installed. Refer to the section below and then run the following command

```bash
gradlew build
```

### Running the Tests

To run the tests for the AWS JDBC Driver, [Docker](https://docs.docker.com/get-docker/) must be installed. After installing Docker, execute the following commands to create the Docker servers that the tests will run against:

```bash
$ cd aws-mysql-jdbc/docker
$ docker-compose up -d
$ cd ../
```

You can now run the tests in the aws-mysql-jdbc directory using the following command:

```bash
gradlew test
```

To shut down the Docker servers after finishing testing:

```bash
$ cd aws-mysql-jdbc/docker
$ docker-compose down && docker-compose rm
$ cd ../
```

## Getting Help and Opening Issues

If you encounter a bug with the AWS JDBC Driver for MySQL, we would like to hear about it. Please search the existing issues and see if others are also experiencing the issue before opening a new issue. When opening a new issue, we will need the version of AWS JDBC Driver for MySQL, Java language version, OS you’re using, and the MySQL database version you're running against. Please also include reproduction case for the issue when appropriate.

The GitHub issues are intended for bug reports and feature requests. Keeping the list of open issues lean will help us respond in a timely manner.

## Documentation

For additional documentation on the AWS JDBC Driver, [please refer to the documentation for the open-source mysql-connector-j driver that the AWS JDBC Driver was based on](https://dev.mysql.com/doc/connector-j/8.0/en/).

## License

This software is released under version 2 of the GNU General Public License (GPLv2).