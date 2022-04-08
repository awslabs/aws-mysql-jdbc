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

package testsuite.integration.container;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration testing with Aurora MySQL replica failover logic.
 */
public class ReplicationFailoverIntegrationTest extends AuroraMysqlIntegrationBaseTest{
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

  @Test
  @Timeout(3 * 60000)
  public void testReplicationFailover() throws SQLException {
    assertTrue(clusterSize >= 5, "Minimal cluster configuration: 1 writer + 4 readers");
    final String initialWriterInstance = instanceIDs[0];
    String replicationHosts = getReplicationHosts();

    try (Connection testConnection = DriverManager.getConnection(DB_CONN_STR_PREFIX + replicationHosts,
            TEST_USERNAME, TEST_PASSWORD)) {
      // Switch to a replica
      testConnection.setReadOnly(true);
      String replicaInstance = queryInstanceId(testConnection);
      assertNotEquals(initialWriterInstance, replicaInstance);
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
      assertNotEquals(newInstance, initialWriterInstance);
    }
  }

  private String getReplicationHosts() {
    StringBuilder hostsStringBuilder = new StringBuilder();
    int numHosts = 3;
    for (int i = 0; i < numHosts; i++) {
      hostsStringBuilder.append(instanceIDs[i]).append(DB_CONN_STR_SUFFIX);
      if (i < numHosts - 1) {
        hostsStringBuilder.append(",");
      }
    }
    return hostsStringBuilder.toString();
  }

  private void rebootInstance(String instance) {
    rdsClient.rebootDBInstance((builder) -> builder.dbInstanceIdentifier(instance));
  }

  private void failoverClusterWithATargetInstance(String targetInstanceId)
      throws InterruptedException {
    waitUntilClusterHasRightState();

    while (true) {
      try {
        rdsClient.failoverDBCluster(
          (builder) -> builder.dbClusterIdentifier(DB_CLUSTER_IDENTIFIER)
            .targetDBInstanceIdentifier(targetInstanceId));
        break;
      } catch (Exception e) {
        Thread.sleep(3000);
      }
    }
  }

  private void waitUntilClusterHasRightState() throws InterruptedException {
    String status = getDBCluster().status();
    while (!"available".equalsIgnoreCase(status)) {
      Thread.sleep(3000);
      status = getDBCluster().status();
    }
  }

  private void waitUntilFirstInstanceIsWriter() throws InterruptedException {
    final String firstInstance = instanceIDs[0];
    failoverClusterWithATargetInstance(firstInstance);
    String clusterWriterId = getDBClusterWriterInstanceId();

    while (!firstInstance.equals(clusterWriterId)) {
      clusterWriterId = getDBClusterWriterInstanceId();
      System.out.println("Writer is still " + clusterWriterId);
      Thread.sleep(3000);
    }
  }

  @BeforeEach
  private void resetCluster() throws InterruptedException {
    waitUntilFirstInstanceIsWriter();
  }
}
