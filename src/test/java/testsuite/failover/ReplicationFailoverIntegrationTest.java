/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of this program hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of this connector, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package testsuite.failover;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.RebootDBInstanceRequest;
import com.amazonaws.services.rds.model.DescribeDBClustersResult;
import com.amazonaws.services.rds.model.DescribeDBClustersRequest;
import com.amazonaws.services.rds.model.DBClusterMember;
import com.amazonaws.services.rds.model.DBCluster;
import com.amazonaws.services.rds.model.FailoverDBClusterRequest;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.LogFactory;
import com.mysql.cj.log.StandardLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.aws.rds.jdbc.mysql.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Integration testing with Aurora MySQL replica failover logic.
 */
@Disabled
public class ReplicationFailoverIntegrationTest {
  /*
   * Before running these tests we need to initialize the test cluster as the following.
   *
   * Expected cluster state:
   * +------------------+--------+---------+
   * |   Instance Id    |  Role  | Status  |
   * +------------------+--------+---------+
   * | mysql-instance-1 | Writer | Running |
   * | mysql-instance-2 | Reader | Running |
   * | mysql-instance-3 | Reader | Running |
   * | mysql-instance-4 | Reader | Running |
   * | mysql-instance-5 | Reader | Running |
   * +------------------+--------+---------+
   *
   * */
  private static final String DB_CONN_STR_PREFIX = "jdbc:mysql:replication://";
  private static final String DB_CONN_STR_SUFFIX = System.getenv("DB_CONN_STR_SUFFIX");

  private static final String TEST_DB_CLUSTER_IDENTIFIER =
      System.getenv("TEST_DB_CLUSTER_IDENTIFIER");
  private static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  private static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");
  private static final int TEST_CLUSTER_SIZE = 5;
  private static String INSTANCE_ID_1 = "";
  private static String INSTANCE_ID_2 = "";
  private static String INSTANCE_ID_3 = "";
  private static String INSTANCE_ID_4 = "";
  private static String INSTANCE_ID_5 = "";
  private final List<String> hosts = new ArrayList<>();
  private static final String NO_WRITER_AVAILABLE =
      "Cannot get the id of the writer Instance in the cluster.";

  private final Log log;

  private final AmazonRDS rdsClient = AmazonRDSClientBuilder.standard().build();

  /**
   * ReplicationFailoverIntegrationTest constructor.
   */
  public ReplicationFailoverIntegrationTest() throws SQLException {
    DriverManager.registerDriver(new Driver());
    this.log =
        LogFactory.getLogger(StandardLogger.class.getName(), Log.LOGGER_INSTANCE_NAME);

    initiateInstanceNames();
    hosts.add(INSTANCE_ID_1);
    hosts.add(INSTANCE_ID_2);
    hosts.add(INSTANCE_ID_3);
    hosts.add(INSTANCE_ID_4);
    hosts.add(INSTANCE_ID_5);
  }

  @Test
  @Timeout(3 * 60000)
  public void testReplicationFailover() throws SQLException {
    final String initalWriterInstance = INSTANCE_ID_1;
    String replicationHosts = getReplicationHosts();

    Connection testConnection =
        DriverManager.getConnection(DB_CONN_STR_PREFIX + replicationHosts,
            TEST_USERNAME,
            TEST_PASSWORD);

    // Switch to a replica
    testConnection.setReadOnly(true);
    String replicaInstance = queryInstanceId(testConnection);
    assertNotEquals(initalWriterInstance, replicaInstance);
    rebootInstance(replicaInstance);

    try {
      while (true) {
        queryInstanceId(testConnection);
      }
    } catch (SQLException e) {
      // do nothing
    }

    // Assert that we are connected to the new replica after failover happens.
    final String newInstance = queryInstanceId(testConnection);
    assertNotEquals(newInstance, replicaInstance);
    assertNotEquals(newInstance, initalWriterInstance);
  }

  private String getReplicationHosts() {
    StringBuilder hostsStringBuilder = new StringBuilder();
    int numHosts = 3;
    for (int i = 0; i < numHosts; i++) {
      hostsStringBuilder.append(hosts.get(i)).append(DB_CONN_STR_SUFFIX);
      if (i < numHosts - 1) {
        hostsStringBuilder.append(",");
      }
    }
    return hostsStringBuilder.toString();
  }

  private void rebootInstance(String instance) {
    RebootDBInstanceRequest rebootRequest =
        new RebootDBInstanceRequest().withDBInstanceIdentifier(instance);
    rdsClient.rebootDBInstance(rebootRequest);
  }

  private DBCluster getDBCluster() {
    DescribeDBClustersRequest dbClustersRequest =
        new DescribeDBClustersRequest().withDBClusterIdentifier(TEST_DB_CLUSTER_IDENTIFIER);
    DescribeDBClustersResult dbClustersResult =
        rdsClient.describeDBClusters(dbClustersRequest);
    List<DBCluster> dbClusterList = dbClustersResult.getDBClusters();
    return dbClusterList.get(0);
  }

  private void initiateInstanceNames() {
    this.log.logDebug("Initiating db instance names.");
    List<DBClusterMember> dbClusterMembers = getDBClusterMemberList();

    assertEquals(TEST_CLUSTER_SIZE, dbClusterMembers.size());
    INSTANCE_ID_1 = dbClusterMembers.get(0).getDBInstanceIdentifier();
    INSTANCE_ID_2 = dbClusterMembers.get(1).getDBInstanceIdentifier();
    INSTANCE_ID_3 = dbClusterMembers.get(2).getDBInstanceIdentifier();
    INSTANCE_ID_4 = dbClusterMembers.get(3).getDBInstanceIdentifier();
    INSTANCE_ID_5 = dbClusterMembers.get(4).getDBInstanceIdentifier();
  }

  private List<DBClusterMember> getDBClusterMemberList() {
    DBCluster dbCluster = getDBCluster();
    return dbCluster.getDBClusterMembers();
  }

  private String getDBClusterWriterInstanceId() {
    List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList().stream()
            .filter(DBClusterMember::isClusterWriter)
            .collect(Collectors.toList());
    if (matchedMemberList.isEmpty()) {
      throw new RuntimeException(NO_WRITER_AVAILABLE);
    }
    // Should be only one writer at index 0.
    return matchedMemberList.get(0).getDBInstanceIdentifier();
  }

  private void failoverClusterWithATargetInstance(String targetInstanceId)
      throws InterruptedException {
    this.log.logDebug("Failover cluster to " + targetInstanceId);
    waitUntilClusterHasRightState();
    FailoverDBClusterRequest request =
        new FailoverDBClusterRequest()
            .withDBClusterIdentifier(TEST_DB_CLUSTER_IDENTIFIER)
            .withTargetDBInstanceIdentifier(targetInstanceId);

    while (true) {
      try {
        rdsClient.failoverDBCluster(request);
        break;
      } catch (Exception e) {
        Thread.sleep(3000);
      }
    }

    this.log.logDebug("Cluster failover request successful.");
  }

  private void waitUntilClusterHasRightState() throws InterruptedException {
    this.log.logDebug("Wait until cluster is in available state.");
    String status = getDBCluster().getStatus();
    while (!"available".equalsIgnoreCase(status)) {
      Thread.sleep(3000);
      status = getDBCluster().getStatus();
    }
    this.log.logDebug("Cluster is available.");
  }

  private Connection createConnectionWithProxyDisabled(String instanceID)
      throws SQLException {
    return DriverManager.getConnection(
        DB_CONN_STR_PREFIX + instanceID + DB_CONN_STR_SUFFIX
            + "?enableClusterAwareFailover=false",
        TEST_USERNAME,
        TEST_PASSWORD);
  }

  private String queryInstanceId(Connection connection) throws SQLException {
    try (Statement myStmt = connection.createStatement();
         ResultSet resultSet = myStmt.executeQuery("select @@aurora_server_id")
    ) {
      if (resultSet.next()) {
        return resultSet.getString("@@aurora_server_id");
      }
    }
    throw new SQLException();
  }

  private void waitUntilFirstInstanceIsWriter() throws InterruptedException {
    this.log.logDebug("Failover cluster to Instance 1.");
    failoverClusterWithATargetInstance(INSTANCE_ID_1);
    String clusterWriterId = getDBClusterWriterInstanceId();

    this.log.logDebug("Wait until Instance 1 becomes the writer.");
    while (!INSTANCE_ID_1.equals(clusterWriterId)) {
      clusterWriterId = getDBClusterWriterInstanceId();
      System.out.println("Writer is still " + clusterWriterId);
      Thread.sleep(3000);
    }
  }

  /**
   * Block until the specified instance is accessible again.
   */
  public void waitUntilInstanceIsUp(String instanceId) throws InterruptedException {
    this.log.logDebug("Wait until " + instanceId + " is up.");
    while (true) {
      try (Connection conn = createConnectionWithProxyDisabled(instanceId)) {
        conn.close();
        break;
      } catch (SQLException ex) {
        // Continue waiting until instance is up.
      }
      Thread.sleep(3000);
    }
    this.log.logDebug(instanceId + " is up.");
  }

  @BeforeEach
  private void resetCluster() throws InterruptedException {
    this.log.logDebug("Resetting cluster.");
    waitUntilFirstInstanceIsWriter();
    waitUntilInstanceIsUp(INSTANCE_ID_1);
    waitUntilInstanceIsUp(INSTANCE_ID_2);
    waitUntilInstanceIsUp(INSTANCE_ID_3);
    waitUntilInstanceIsUp(INSTANCE_ID_4);
    waitUntilInstanceIsUp(INSTANCE_ID_5);
  }
}
