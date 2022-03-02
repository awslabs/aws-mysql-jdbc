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

import com.amazonaws.services.rds.model.FailoverDBClusterRequest;
import com.mysql.cj.conf.PropertyKey;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import eu.rekawek.toxiproxy.Proxy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Integration testing with Aurora MySQL failover logic. */
public class AuroraMysqlFailoverIntegrationTest extends AuroraMysqlIntegrationBaseTest {

  /* Writer connection failover tests. */

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new
   * writer. Driver failover occurs when executing a method against the connection
   */
  @Test
  public void test_failFromWriterToNewWriter_failOnConnectionInvocation()
      throws SQLException, InterruptedException {

    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, initDefaultProps())) {
      // Crash Instance1 and nominate a new writer
      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      // Failure occurs on Connection invocation
      assertFirstQueryThrows(conn, "08S02");

      // Assert that we are connected to the new writer after failover happens.
      final String currentConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(currentConnectionId));
      assertNotEquals(currentConnectionId, initialWriterId);
    }
  }

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new
   * writer Driver failover occurs when executing a method against an object bound to the connection
   * (eg a Statement object created by the connection).
   */
  @Test
  public void test_failFromWriterToNewWriter_failOnConnectionBoundObjectInvocation()
      throws SQLException, InterruptedException {

    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, initDefaultProps())) {
      final Statement stmt = conn.createStatement();

      // Crash Instance1 and nominate a new writer
      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      // Failure occurs on Statement invocation
      assertFirstQueryThrows(stmt, "08S02");

      // Assert that the driver is connected to the new writer after failover happens.
      final String currentConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(currentConnectionId));
      assertNotEquals(initialWriterId, currentConnectionId);
    }
  }

  /**
   * Current reader dies, no other reader instance, failover to writer, then writer dies, failover
   * to another available reader instance.
   */
  @Test
  public void test_failFromReaderToWriterToAnyAvailableInstance()
      throws SQLException, IOException, InterruptedException {

    assertTrue(clusterSize >= 3, "Minimal cluster configuration: 1 writer + 2 readers");

    // Crashing all readers except the first one
    for (int i = 2; i < clusterSize; i++) {
      final Proxy instanceProxy = proxyMap.get(instanceIDs[i]);
      containerHelper.disableConnectivity(instanceProxy);
    }

    // Connect to Instance2 which is the only reader that is up.
    final String instanceId = instanceIDs[1];

    try (final Connection conn = connectToInstance(instanceId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      // Crash Instance2
      Proxy instanceProxy = proxyMap.get(instanceId);
      containerHelper.disableConnectivity(instanceProxy);

      assertFirstQueryThrows(conn, "08S02");

      // Assert that we are currently connected to the writer Instance1.
      final String writerId = instanceIDs[0];
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(writerId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));

      // Stop Crashing reader Instance2 and Instance3
      final String readerAId = instanceIDs[1];
      instanceProxy = proxyMap.get(readerAId);
      containerHelper.enableConnectivity(instanceProxy);

      final String readerBId = instanceIDs[2];
      instanceProxy = proxyMap.get(readerBId);
      containerHelper.enableConnectivity(instanceProxy);

      // Crash writer Instance1.
      failoverClusterToATargetAndWaitUntilWriterChanged(writerId, readerBId);

      assertFirstQueryThrows(conn, "08S02");

      // Assert that we are connected to one of the available instances.
      currentConnectionId = queryInstanceId(conn);
      assertTrue(
          readerAId.equals(currentConnectionId) || readerBId.equals(currentConnectionId));
    }
  }

  /* Failure when within a transaction tests. */

  /** Writer fails within a transaction. Open transaction with "SET autocommit = 0" */
  @Test
  public void test_writerFailWithinTransaction_setAutocommitSqlZero()
      throws SQLException, InterruptedException {

    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, initDefaultProps())) {
      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_1");
      testStmt1.executeUpdate(
          "CREATE TABLE test3_1 (id int not null primary key, test3_1_field varchar(255) not null)");
      testStmt1.executeUpdate("SET autocommit = 0"); // open a new transaction

      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeUpdate("INSERT INTO test3_1 VALUES (1, 'test field string 1')");

      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      // If there is an active transaction, roll it back and return an error with SQLState 08007.
      final SQLException exception =
          assertThrows(
              SQLException.class,
              () -> testStmt2.executeUpdate(
                  "INSERT INTO test3_1 VALUES (2, 'test field string 2')"));
      assertEquals("08007", exception.getSQLState());

      // Attempt to query the instance id.
      final String currentConnectionId = queryInstanceId(conn);
      // Assert that we are connected to the new writer after failover happens.
      assertTrue(isDBInstanceWriter(currentConnectionId));
      final String nextClusterWriterId = getDBClusterWriterInstanceId();
      assertEquals(currentConnectionId, nextClusterWriterId);
      assertNotEquals(initialWriterId, nextClusterWriterId);

      // testStmt2 can NOT be used anymore since it's invalid
      final Statement testStmt3 = conn.createStatement();
      final ResultSet rs = testStmt3.executeQuery("SELECT count(*) from test3_1");
      rs.next();
      // Assert that NO row has been inserted to the table;
      assertEquals(0, rs.getInt(1));

      testStmt3.executeUpdate("DROP TABLE IF EXISTS test3_1");
    }
  }

  /** Writer fails within a transaction. Open transaction with setAutoCommit(false) */
  @Test
  public void test_writerFailWithinTransaction_setAutoCommitFalse()
      throws SQLException, InterruptedException {

    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, initDefaultProps())) {
      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_2");
      testStmt1.executeUpdate(
          "CREATE TABLE test3_2 (id int not null primary key, test3_2_field varchar(255) not null)");
      conn.setAutoCommit(false); // open a new transaction

      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeUpdate("INSERT INTO test3_2 VALUES (1, 'test field string 1')");

      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      // If there is an active transaction, roll it back and return an error with SQLState 08007.
      final SQLException exception =
          assertThrows(
              SQLException.class,
              () -> testStmt2.executeUpdate("INSERT INTO test3_2 VALUES (2, 'test field string 2')"));
      assertEquals("08007", exception.getSQLState());

      // Attempt to query the instance id.
      final String currentConnectionId = queryInstanceId(conn);
      // Assert that we are connected to the new writer after failover happens.
      assertTrue(isDBInstanceWriter(currentConnectionId));
      final String nextClusterWriterId = getDBClusterWriterInstanceId();
      assertEquals(currentConnectionId, nextClusterWriterId);
      assertNotEquals(initialWriterId, nextClusterWriterId);

      // testStmt2 can NOT be used anymore since it's invalid

      final Statement testStmt3 = conn.createStatement();
      final ResultSet rs = testStmt3.executeQuery("SELECT count(*) from test3_2");
      rs.next();
      // Assert that NO row has been inserted to the table;
      assertEquals(0, rs.getInt(1));

      testStmt3.executeUpdate("DROP TABLE IF EXISTS test3_2");
    }
  }

  /** Writer fails within a transaction. Open transaction with "START TRANSACTION". */
  @Test
  public void test_writerFailWithinTransaction_startTransaction()
      throws SQLException, InterruptedException {

    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, initDefaultProps())) {
      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_3");
      testStmt1.executeUpdate(
          "CREATE TABLE test3_3 (id int not null primary key, test3_3_field varchar(255) not null)");
      testStmt1.executeUpdate("START TRANSACTION"); // open a new transaction

      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeUpdate("INSERT INTO test3_3 VALUES (1, 'test field string 1')");

      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      // If there is an active transaction, roll it back and return an error with SQLState 08007.
      final SQLException exception =
          assertThrows(
              SQLException.class,
              () -> testStmt2.executeUpdate("INSERT INTO test3_3 VALUES (2, 'test field string 2')"));
      assertEquals("08007", exception.getSQLState());

      // Attempt to query the instance id.
      final String currentConnectionId = queryInstanceId(conn);
      // Assert that we are connected to the new writer after failover happens.
      assertTrue(isDBInstanceWriter(currentConnectionId));
      final String nextClusterWriterId = getDBClusterWriterInstanceId();
      assertEquals(currentConnectionId, nextClusterWriterId);
      assertNotEquals(initialWriterId, nextClusterWriterId);

      // testStmt2 can NOT be used anymore since it's invalid

      final Statement testStmt3 = conn.createStatement();
      final ResultSet rs = testStmt3.executeQuery("SELECT count(*) from test3_3");
      rs.next();
      // Assert that NO row has been inserted to the table;
      assertEquals(0, rs.getInt(1));

      testStmt3.executeUpdate("DROP TABLE IF EXISTS test3_3");
    }
  }

  /** Writer fails within NO transaction. */
  @Test
  public void test_writerFailWithNoTransaction() throws SQLException, InterruptedException {

    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, initDefaultProps())) {
      final Statement testStmt1 = conn.createStatement();
      testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_4");
      testStmt1.executeUpdate(
          "CREATE TABLE test3_4 (id int not null primary key, test3_4_field varchar(255) not null)");

      final Statement testStmt2 = conn.createStatement();
      testStmt2.executeUpdate("INSERT INTO test3_4 VALUES (1, 'test field string 1')");

      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      final SQLException exception =
          assertThrows(
              SQLException.class,
              () -> testStmt2.executeUpdate("INSERT INTO test3_4 VALUES (2, 'test field string 2')"));
      assertEquals("08S02", exception.getSQLState());

      // Attempt to query the instance id.
      final String currentConnectionId = queryInstanceId(conn);
      // Assert that we are connected to the new writer after failover happens.
      assertTrue(isDBInstanceWriter(currentConnectionId));
      final String nextClusterWriterId = getDBClusterWriterInstanceId();
      assertEquals(currentConnectionId, nextClusterWriterId);
      assertNotEquals(initialWriterId, nextClusterWriterId);

      // testStmt2 can NOT be used anymore since it's invalid
      final Statement testStmt3 = conn.createStatement();
      final ResultSet rs = testStmt3.executeQuery("SELECT count(*) from test3_4 WHERE id = 1");
      rs.next();
      // Assert that row with id=1 has been inserted to the table;
      assertEquals(1, rs.getInt(1));

      final ResultSet rs1 = testStmt3.executeQuery("SELECT count(*) from test3_4 WHERE id = 2");
      rs1.next();
      // Assert that row with id=2 has NOT been inserted to the table;
      assertEquals(0, rs1.getInt(1));

      testStmt3.executeUpdate("DROP TABLE IF EXISTS test3_4");
    }
  }

  /* Pooled connection tests. */

  /** Writer connection failover within the connection pool. */
  @Test
  public void test_pooledWriterConnection_BasicFailover()
      throws SQLException, InterruptedException {

    final String initialWriterId = instanceIDs[0];
    final String nominatedWriterId = instanceIDs[1];

    try (final Connection conn = createPooledConnectionWithInstanceId(initialWriterId)) {
      // Crash writer Instance1 and nominate Instance2 as the new writer
      failoverClusterToATargetAndWaitUntilWriterChanged(initialWriterId, nominatedWriterId);

      assertFirstQueryThrows(conn, "08S02");

      // Execute Query again to get the current connection id;
      final String currentConnectionId = queryInstanceId(conn);

      // Assert that we are connected to the new writer after failover happens.
      assertTrue(isDBInstanceWriter(currentConnectionId));
      final String nextWriterId = getDBClusterWriterInstanceId();
      assertEquals(nextWriterId, currentConnectionId);
      assertEquals(nominatedWriterId, currentConnectionId);

      // Assert that the pooled connection is valid.
      assertTrue(conn.isValid(IS_VALID_TIMEOUT));
    }
  }

  @Test
  public void test_takeOverConnectionProperties() throws SQLException, InterruptedException {

    final String initialWriterId = instanceIDs[0];

    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "false");

    // Establish the topology cache so that we can later assert that testConnection does not inherit properties from
    // establishCacheConnection either before or after failover
    final Connection establishCacheConnection = DriverManager.getConnection(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL, props);
    establishCacheConnection.close();

    props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "true");
    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, MYSQL_PORT, props)) {
      // Verify that connection accepts multi-statement sql
      final Statement myStmt = conn.createStatement();
      myStmt.executeQuery("select @@aurora_server_id; select 1; select 2;");

      // Crash Instance1 and nominate a new writer
      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      assertFirstQueryThrows(conn, "08S02");

      // Verify that connection still accepts multi-statement sql
      final Statement myStmt1 = conn.createStatement();
      myStmt1.executeQuery("select @@aurora_server_id; select 1; select 2;");
    }
  }

  // Helpers
  private void failoverClusterAndWaitUntilWriterChanged(String clusterWriterId)
      throws InterruptedException {
    failoverCluster();
    waitUntilWriterInstanceChanged(clusterWriterId);
  }

  private void failoverCluster() throws InterruptedException {
    waitUntilClusterHasRightState();
    final FailoverDBClusterRequest request =
        new FailoverDBClusterRequest().withDBClusterIdentifier(DB_CLUSTER_IDENTIFIER);

    while (true) {
      try {
        rdsClient.failoverDBCluster(request);
        break;
      } catch (final Exception e) {
        TimeUnit.MILLISECONDS.sleep(1000);
      }
    }
  }

  private void failoverClusterToATargetAndWaitUntilWriterChanged(
      String clusterWriterId, String targetInstanceId) throws InterruptedException {
    failoverClusterWithATargetInstance(targetInstanceId);
    waitUntilWriterInstanceChanged(clusterWriterId);
  }

  private void failoverClusterWithATargetInstance(String targetInstanceId)
      throws InterruptedException {
    waitUntilClusterHasRightState();
    final FailoverDBClusterRequest request =
        new FailoverDBClusterRequest()
            .withDBClusterIdentifier(DB_CLUSTER_IDENTIFIER)
            .withTargetDBInstanceIdentifier(targetInstanceId);

    while (true) {
      try {
        rdsClient.failoverDBCluster(request);
        break;
      } catch (final Exception e) {
        TimeUnit.MILLISECONDS.sleep(1000);
      }
    }
  }

  private void waitUntilWriterInstanceChanged(String initialWriterInstanceId)
      throws InterruptedException {
    String nextClusterWriterId = getDBClusterWriterInstanceId();
    while (initialWriterInstanceId.equals(nextClusterWriterId)) {
      TimeUnit.MILLISECONDS.sleep(3000);
      // Calling the RDS API to get writer Id.
      nextClusterWriterId = getDBClusterWriterInstanceId();
    }
  }

  private void waitUntilClusterHasRightState() throws InterruptedException {
    String status = getDBCluster().getStatus();
    while (!"available".equalsIgnoreCase(status)) {
      TimeUnit.MILLISECONDS.sleep(1000);
      status = getDBCluster().getStatus();
    }
  }
}
