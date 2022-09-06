// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0
// (GPLv2), as published by the Free Software Foundation, with the
// following additional permissions:
//
// This program is distributed with certain software that is licensed
// under separate terms, as designated in a particular file or component
// or in the license documentation. Without limiting your rights under
// the GPLv2, the authors of this program hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have included with the program.
//
// Without limiting the foregoing grant of rights under the GPLv2 and
// additional permission as to separately licensed software, this
// program is also subject to the Universal FOSS Exception, version 1.0,
// a copy of which can be found along with its FAQ at
// http://oss.oracle.com/licenses/universal-foss-exception.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see 
// http://www.gnu.org/licenses/gpl-2.0.html.

package testsuite.integration.container.aurora;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import eu.rekawek.toxiproxy.Proxy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class AuroraMysqlReadWriteSplittingTest extends AuroraMysqlIntegrationBaseTest {

  private static Stream<Arguments> testParameters() {
    return Stream.of(
            Arguments.of(getProps_allPlugins()),
            Arguments.of(getProps_readWritePlugin())
    );
  }

  private static Stream<Arguments> proxiedTestParameters() {
    return Stream.of(
            Arguments.of(getProxiedProps_allPlugins()),
            Arguments.of(getProxiedProps_readWritePlugin())
    );
  }

  @ParameterizedTest(name = "test_connectToWriter_setReadOnlyTrueFalseTrue")
  @MethodSource("testParameters")
  public void test_connectToWriter_setReadOnlyTrueFalseTrue(Properties props) throws SQLException {
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      conn.setReadOnly(false);
      writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);

      conn.setReadOnly(true);
      String newReaderConnectionId = queryInstanceId(conn);
      assertEquals(readerConnectionId, newReaderConnectionId);
    }
  }

  @ParameterizedTest(name = "test_connectToReader_setReadOnlyTrueFalse")
  @MethodSource("testParameters")
  public void test_connectToReader_setReadOnlyTrueFalse(Properties props) throws SQLException {
    final String initialReaderId = instanceIDs[1];

    try (final Connection conn = connectToInstance(initialReaderId + DB_CONN_STR_SUFFIX, MYSQL_PORT, props)) {
      String readerConnectionId = queryInstanceId(conn);
      assertEquals(initialReaderId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      conn.setReadOnly(true);
      readerConnectionId = queryInstanceId(conn);
      assertEquals(initialReaderId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      conn.setReadOnly(false);
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(instanceIDs[0], writerConnectionId);
      assertNotEquals(initialReaderId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));
    }
  }

  @ParameterizedTest(name = "test_connectToReaderCluster_setReadOnlyTrueFalse")
  @MethodSource("testParameters")
  public void test_connectToReaderCluster_setReadOnlyTrueFalse(Properties props) throws SQLException {
    try (final Connection conn = connectToInstance(MYSQL_RO_CLUSTER_URL, MYSQL_PORT, props)) {
      String initialReaderId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(initialReaderId));

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertEquals(initialReaderId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      conn.setReadOnly(false);
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(instanceIDs[0], writerConnectionId);
      assertNotEquals(initialReaderId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));
    }
  }

  @ParameterizedTest(name = "test_connectToReaderIP_setReadOnlyTrueFalse")
  @MethodSource("testParameters")
  public void test_connectToReaderIP_setReadOnlyTrueFalse(Properties props) throws SQLException, UnknownHostException {
    String instanceHostPattern = "?" + DB_CONN_STR_SUFFIX;
    props.setProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), instanceHostPattern);
    final String hostIp = hostToIP(MYSQL_RO_CLUSTER_URL);
    try (final Connection conn = connectToInstance(hostIp, MYSQL_PORT, props)) {
      String initialReaderId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(initialReaderId));

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertEquals(initialReaderId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      conn.setReadOnly(false);
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(instanceIDs[0], writerConnectionId);
      assertNotEquals(initialReaderId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyFalseInReadOnlyTransaction")
  @MethodSource("testParameters")
  public void test_setReadOnlyFalseInReadOnlyTransaction(Properties props) throws SQLException{
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      final Statement stmt1 = conn.createStatement();
      stmt1.executeUpdate("DROP TABLE IF EXISTS test_splitting_readonly_transaction");
      stmt1.executeUpdate("CREATE TABLE test_splitting_readonly_transaction (id int not null primary key, text_field varchar(255) not null)");
      stmt1.executeUpdate("INSERT INTO test_splitting_readonly_transaction VALUES (1, 'test_field value 1')");

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(readerConnectionId));

      final Statement stmt2 = conn.createStatement();
      stmt2.execute("START TRANSACTION READ ONLY");
      stmt2.executeQuery("SELECT count(*) from test_splitting_readonly_transaction");

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      assertEquals(MysqlErrorNumbers.SQL_STATE_ACTIVE_SQL_TRANSACTION, exception.getSQLState());

      stmt2.execute("COMMIT");

      conn.setReadOnly(false);
      writerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      final Statement stmt3 = conn.createStatement();
      stmt3.executeUpdate("DROP TABLE IF EXISTS test_splitting_readonly_transaction");
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyFalseInTransaction_setAutocommitFalse")
  @MethodSource("testParameters")
  public void test_setReadOnlyFalseInTransaction_setAutocommitFalse(Properties props) throws SQLException{
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      final Statement stmt1 = conn.createStatement();
      stmt1.executeUpdate("DROP TABLE IF EXISTS test_splitting_readonly_transaction");
      stmt1.executeUpdate("CREATE TABLE test_splitting_readonly_transaction (id int not null primary key, text_field varchar(255) not null)");
      stmt1.executeUpdate("INSERT INTO test_splitting_readonly_transaction VALUES (1, 'test_field value 1')");

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(readerConnectionId));

      final Statement stmt2 = conn.createStatement();
      conn.setAutoCommit(false);
      stmt2.executeQuery("SELECT count(*) from test_splitting_readonly_transaction");

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      assertEquals(MysqlErrorNumbers.SQL_STATE_ACTIVE_SQL_TRANSACTION, exception.getSQLState());

      stmt2.execute("COMMIT");

      conn.setReadOnly(false);
      writerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      final Statement stmt3 = conn.createStatement();
      stmt3.executeUpdate("DROP TABLE IF EXISTS test_splitting_readonly_transaction");
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyFalseInTransaction_setAutocommitZero")
  @MethodSource("testParameters")
  public void test_setReadOnlyFalseInTransaction_setAutocommitZero(Properties props) throws SQLException{
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      final Statement stmt1 = conn.createStatement();
      stmt1.executeUpdate("DROP TABLE IF EXISTS test_splitting_readonly_transaction");
      stmt1.executeUpdate("CREATE TABLE test_splitting_readonly_transaction (id int not null primary key, text_field varchar(255) not null)");
      stmt1.executeUpdate("INSERT INTO test_splitting_readonly_transaction VALUES (1, 'test_field value 1')");

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(readerConnectionId));

      final Statement stmt2 = conn.createStatement();
      stmt2.execute("SET autocommit = 0");
      stmt2.executeQuery("SELECT count(*) from test_splitting_readonly_transaction");

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      assertEquals(MysqlErrorNumbers.SQL_STATE_ACTIVE_SQL_TRANSACTION, exception.getSQLState());

      stmt2.execute("COMMIT");

      conn.setReadOnly(false);
      writerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      final Statement stmt3 = conn.createStatement();
      stmt3.executeUpdate("DROP TABLE IF EXISTS test_splitting_readonly_transaction");
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyTrueInTransaction")
  @MethodSource("testParameters")
  public void test_setReadOnlyTrueInTransaction(Properties props) throws SQLException{
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      final Statement stmt1 = conn.createStatement();
      stmt1.executeUpdate("DROP TABLE IF EXISTS test_splitting_readonly_transaction");
      stmt1.executeUpdate("CREATE TABLE test_splitting_readonly_transaction (id int not null primary key, text_field varchar(255) not null)");
      stmt1.execute("SET autocommit = 0");

      final Statement stmt2 = conn.createStatement();
      stmt2.executeUpdate("INSERT INTO test_splitting_readonly_transaction VALUES (1, 'test_field value 1')");

      assertDoesNotThrow(() -> conn.setReadOnly(true));
      writerConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      stmt2.execute("COMMIT");
      final ResultSet rs = stmt2.executeQuery("SELECT count(*) from test_splitting_readonly_transaction");
      rs.next();
      assertEquals(1, rs.getInt(1));

      conn.setReadOnly(false);
      stmt2.executeUpdate("DROP TABLE IF EXISTS test_splitting_readonly_transaction");
    }
  }

  @ParameterizedTest(name = "test_readerLoadBalancing_autocommitTrue")
  @MethodSource("testParameters")
  public void test_readerLoadBalancing_autocommitTrue(Properties props) throws SQLException {
    final String initialWriterId = instanceIDs[0];

    props.setProperty(PropertyKey.loadBalanceReadOnlyTraffic.getKeyName(), "true");
    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      String readerId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerId);
      assertTrue(isDBInstanceReader(readerId));

      // Assert switch after each execute
      String nextReaderId;
      for (int i = 0; i < 5; i++) {
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
        assertTrue(isDBInstanceReader(readerId));
        readerId = nextReaderId;
      }

      // Verify behavior for transactions started while autocommit is on (autocommit is implicitly disabled)
      // Connection should not be switched while inside a transaction
      Statement stmt = conn.createStatement();
      for (int i = 0; i < 5; i++) {
        stmt.execute("  bEgiN ");
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        stmt.execute(" CommIT;");
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }
    }
  }


  @ParameterizedTest(name = "test_readerLoadBalancing_autocommitFalse")
  @MethodSource("testParameters")
  public void test_readerLoadBalancing_autocommitFalse(Properties props) throws SQLException {
    final String initialWriterId = instanceIDs[0];

    props.setProperty(PropertyKey.loadBalanceReadOnlyTraffic.getKeyName(), "true");
    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setAutoCommit(false);
      conn.setReadOnly(true);

      // Connection should not be switched while inside a transaction
      String readerId, nextReaderId;
      Statement stmt = conn.createStatement();
      for (int i = 0; i < 5; i++) {
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        conn.commit();
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }

      for (int i = 0; i < 5; i++) {
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        stmt.execute("commit");
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }

      for (int i = 0; i < 5; i++) {
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        conn.rollback();
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }

      for (int i = 0; i < 5; i++) {
        readerId = queryInstanceId(conn);
        nextReaderId = queryInstanceId(conn);
        assertEquals(readerId, nextReaderId);
        stmt.execute(" roLLback ; ");
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
      }
    }
  }

  @ParameterizedTest(name = "test_readerLoadBalancing_switchAutoCommitInTransaction")
  @MethodSource("testParameters")
  public void test_readerLoadBalancing_switchAutoCommitInTransaction(Properties props) throws SQLException {
    final String initialWriterId = instanceIDs[0];

    props.setProperty(PropertyKey.loadBalanceReadOnlyTraffic.getKeyName(), "true");
    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      String readerId, nextReaderId;
      Statement stmt = conn.createStatement();

      // Start transaction while autocommit is on (autocommit is implicitly disabled)
      // Connection should not be switched while inside a transaction
      stmt.execute("  StarT   TRanSACtion  REad onLy  ; ");
      readerId = queryInstanceId(conn);
      nextReaderId = queryInstanceId(conn);
      assertEquals(readerId, nextReaderId);
      conn.setAutoCommit(false); // Switch autocommit value while inside the transaction
      nextReaderId = queryInstanceId(conn);
      assertEquals(readerId, nextReaderId);
      conn.commit();

      assertFalse(conn.getAutoCommit());
      nextReaderId = queryInstanceId(conn);
      assertNotEquals(readerId, nextReaderId); // Connection should have switched after committing

      readerId = nextReaderId;
      nextReaderId = queryInstanceId(conn);
      assertEquals(readerId, nextReaderId); // Since autocommit is now off, we should be in a transaction; connection should not be switching
      assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      
      conn.setAutoCommit(true); // Switch autocommit value while inside the transaction
      stmt.execute("commit");

      assertTrue(conn.getAutoCommit());
      readerId = queryInstanceId(conn);
      
      // Autocommit is now on; connection should switch after each execute
      for (int i = 0; i < 5; i++) {
        nextReaderId = queryInstanceId(conn);
        assertNotEquals(readerId, nextReaderId);
        readerId = nextReaderId;
      }
    }
  }

  @ParameterizedTest(name = "test_readerLoadBalancing_remainingStateTransitions")
  @MethodSource("testParameters")
  public void test_readerLoadBalancing_remainingStateTransitions(Properties props) throws SQLException {
    // Main functionality has been tested in the other tests
    // This test executes state transitions not covered by other tests to verify no unexpected errors are thrown
    final String initialWriterId = instanceIDs[0];

    props.setProperty(PropertyKey.loadBalanceReadOnlyTraffic.getKeyName(), "true");
    try (final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      conn.setReadOnly(false);
      conn.setReadOnly(true);
      conn.setAutoCommit(false);
      conn.setAutoCommit(true);
      Statement stmt = conn.createStatement();
      stmt.execute("commit");
      stmt.execute("commit");
      stmt.execute("begin");
      stmt.execute("commit");
      conn.setAutoCommit(false);
      conn.commit();
      conn.commit();
      stmt.execute("begin");
      stmt.execute("SELECT 1");
      conn.commit();
      conn.setReadOnly(false);
      conn.setReadOnly(true);
      conn.setReadOnly(false);
      conn.setAutoCommit(true);
      conn.setReadOnly(false);
      stmt.execute("commit");
      conn.setReadOnly(false);
      conn.setReadOnly(true);
      conn.setAutoCommit(false);
      conn.commit();
      conn.setAutoCommit(true);
    }
  }

  @ParameterizedTest(name = "test_readerLoadBalancing_lostConnectivity")
  @MethodSource("proxiedTestParameters")
  public void test_readerLoadBalancing_lostConnectivity(Properties props) throws SQLException, IOException {
    String initialWriterId = instanceIDs[0];

    // autocommit on transaction (autocommit implicitly disabled)
    try (Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
      conn.setReadOnly(true);
      final Statement stmt1 = conn.createStatement();
      stmt1.execute("BEGIN");
      String readerId = queryInstanceId(conn);

      Proxy proxyInstance = proxyMap.get(readerId);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", readerId));
      }
      
      SQLException e = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      containerHelper.enableConnectivity(proxyInstance);

      if (pluginChainIncludesFailoverPlugin(props)) {
        assertEquals(MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN, e.getSQLState());
      } else {
        assertEquals(MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE, e.getSQLState());
      }

      if (pluginChainIncludesFailoverPlugin(props)) {
        Statement stmt2 = conn.createStatement();
        stmt2.execute("SELECT 1");
        ResultSet rs = stmt2.getResultSet();
        rs.next();
        assertEquals(1, rs.getInt(1));
      }
    }

    // autocommit off transaction
    try (Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
      conn.setReadOnly(true);
      conn.setAutoCommit(false);
      String readerId = queryInstanceId(conn);
      final Statement stmt1 = conn.createStatement();

      Proxy proxyInstance = proxyMap.get(readerId);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", readerId));
      }
      
      SQLException e = assertThrows(SQLException.class, () -> stmt1.execute("SELECT 1"));
      containerHelper.enableConnectivity(proxyInstance);

      if (pluginChainIncludesFailoverPlugin(props)) {
        assertEquals(MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN, e.getSQLState());
      } else {
        assertEquals(MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE, e.getSQLState());
        return;
      }

      Statement stmt2 = conn.createStatement();
      stmt2.execute("SELECT 1");
      ResultSet rs = stmt2.getResultSet();
      rs.next();
      assertEquals(1, rs.getInt(1));
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyTrue_allReadersDown")
  @MethodSource("proxiedTestParameters")
  public void test_setReadOnlyTrue_allReadersDown(Properties props) throws SQLException, IOException {
    String initialWriterId = instanceIDs[0];

    try (Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));

      // Kill all reader instances
      for (int i = 1; i < clusterSize; i++) {
        final String instanceId = instanceIDs[i];
        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      assertDoesNotThrow(() -> conn.setReadOnly(true));
      currentConnectionId = assertDoesNotThrow(() -> queryInstanceId(conn));
      assertTrue(isDBInstanceWriter(currentConnectionId));

      assertDoesNotThrow(() -> conn.setReadOnly(false));
      currentConnectionId = assertDoesNotThrow(() -> queryInstanceId(conn));
      assertTrue(isDBInstanceWriter(currentConnectionId));
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyTrue_allInstancesDown")
  @MethodSource("proxiedTestParameters")
  public void test_setReadOnlyTrue_allInstancesDown(Properties props) throws SQLException, IOException {
    final String initialWriterId = instanceIDs[0];

    props.setProperty(PropertyKey.failoverTimeoutMs.getKeyName(), "10");
    try (Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));

      // Kill all instances
      for (int i = 0; i < clusterSize; i++) {
        final String instanceId = instanceIDs[i];
        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      final SQLException e = assertThrows(SQLException.class, () -> conn.setReadOnly(true));
      if (pluginChainIncludesFailoverPlugin(props)) {
        assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e.getSQLState());
      } else {
        assertEquals(MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE, e.getSQLState());
      }
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyTrue_allInstancesDown_writerClosed")
  @MethodSource("proxiedTestParameters")
  public void test_setReadOnlyTrue_allInstancesDown_writerClosed(Properties props) throws SQLException, IOException {
    final String initialWriterId = instanceIDs[0];

    try (Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));
      conn.close();

      // Kill all instances
      for (int i = 0; i < clusterSize; i++) {
        final String instanceId = instanceIDs[i];
        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(true));
      assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, exception.getSQLState());
    }
  }

  @ParameterizedTest(name = "test_setReadOnlyFalse_allInstancesDown")
  @MethodSource("proxiedTestParameters")
  public void test_setReadOnlyFalse_allInstancesDown(Properties props) throws SQLException, IOException {
    final String initialReaderId = instanceIDs[1];

    try (Connection conn = connectToInstance(initialReaderId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(initialReaderId, currentConnectionId);
      assertTrue(isDBInstanceReader(currentConnectionId));

      // Kill all instances
      for (int i = 0; i < clusterSize; i++) {
        final String instanceId = instanceIDs[i];
        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, exception.getSQLState());
    }
  }

  @Test
  public void test_failoverToNewWriter_setReadOnlyTrueFalse() throws SQLException, InterruptedException, IOException {
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, getProxiedProps_allPlugins())) {
      // Kill all reader instances
      for (int i = 1; i < clusterSize; i++) {
        final String instanceId = instanceIDs[i];
        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      // Force internal reader connection to the writer instance
      conn.setReadOnly(true);
      String currentConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(currentConnectionId));
      conn.setReadOnly(false);

      enableAllProxies();

      // Crash Instance1 and nominate a new writer
      failoverClusterAndWaitUntilWriterChanged(initialWriterId);

      // Failure occurs on Connection invocation
      assertFirstQueryThrows(conn, "08S02");

      // Assert that we are connected to the new writer after failover happens.
      currentConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(currentConnectionId));
      assertNotEquals(currentConnectionId, initialWriterId);

      conn.setReadOnly(true);
      currentConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(currentConnectionId));

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceWriter(currentConnectionId));
    }
  }

  @Test
  public void test_failoverToNewReader_setReadOnlyFalseTrue() throws SQLException, IOException {
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, getProxiedProps_allPlugins())) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      String otherReaderId = "";
      for (int i = 1; i < instanceIDs.length; i ++) {
        if (!instanceIDs[i].equals(readerConnectionId)) {
          otherReaderId = instanceIDs[i];
          break;
        }
      }
      if (otherReaderId.equals("")) {
        fail("could not acquire new reader ID");
      }

      // Kill all instances except one other reader
      for (int i = 0; i < clusterSize; i++) {
        final String instanceId = instanceIDs[i];
        if (otherReaderId.equals(instanceId)) {
          continue;
        }

        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      assertFirstQueryThrows(conn, "08S02");
      assertFalse(conn.isClosed());
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(otherReaderId, currentConnectionId);
      assertNotEquals(readerConnectionId, currentConnectionId);
      assertTrue(isDBInstanceReader(currentConnectionId));

      enableAllProxies();

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));

      conn.setReadOnly(true);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(otherReaderId, currentConnectionId);
      assertTrue(isDBInstanceReader(currentConnectionId));
    }
  }

  @Test
  public void test_failoverReaderToWriter_setReadOnlyTrueFalse() throws SQLException, IOException {
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, getProxiedProps_allPlugins())) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);
      assertTrue(isDBInstanceReader(readerConnectionId));

      // Kill all instances except the writer
      for (int i = 1; i < clusterSize; i++) {
        final String instanceId = instanceIDs[i];

        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      assertFirstQueryThrows(conn, "08S02");
      assertFalse(conn.isClosed());
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));

      enableAllProxies();

      conn.setReadOnly(true);
      currentConnectionId = queryInstanceId(conn);
      assertNotEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceReader(currentConnectionId));

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));
    }
  }

  @Test
  public void test_multiHostUrl_topologyOverridesHostList() throws SQLException {
    final String initialWriterId = instanceIDs[0];

    try (Connection conn = DriverManager.getConnection(DB_CONN_STR_PREFIX + initialWriterId + DB_CONN_STR_SUFFIX + ",non-existent-host", getProps_allPlugins())) {
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceWriter(currentConnectionId));

      conn.setReadOnly(true);
      currentConnectionId = queryInstanceId(conn);
      assertNotEquals(initialWriterId, currentConnectionId);
      assertTrue(isDBInstanceReader(currentConnectionId));
    }
  }

  @Test
  public void test_transactionResolutionUnknown_readWriteSplittingPluginOnly() throws SQLException, IOException {
    final String initialWriterId = instanceIDs[0];

    Properties props = getProxiedProps_readWritePlugin();
    props.setProperty(PropertyKey.loadBalanceReadOnlyTraffic.getKeyName(), "true");
    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      conn.setAutoCommit(false);
      String readerId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerId);
      assertTrue(isDBInstanceReader(readerId));

      final Statement stmt = conn.createStatement();
      stmt.executeQuery("SELECT 1");
      final Proxy proxyInstance = proxyMap.get(readerId);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", readerId));
      }

      SQLException e = assertThrows(SQLException.class, conn::rollback);
      assertEquals(MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN, e.getSQLState());

      try (final Connection newConn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
        newConn.setReadOnly(true);
        Statement newStmt = newConn.createStatement();
        ResultSet rs = newStmt.executeQuery("SELECT 1");
        rs.next();
        assertEquals(1, rs.getInt(1));
      }
    }
  }

  private static Properties getProps_allPlugins() {
    Properties props = initDefaultProps();
    addAllPlugins(props);
    return props;
  }

  private static Properties getProxiedProps_allPlugins() {
    Properties props = initDefaultProxiedProps();
    addAllPlugins(props);
    return props;
  }

  private static Properties getProps_readWritePlugin() {
    Properties props = initDefaultProps();
    addReadWritePlugin(props);
    return props;
  }

  private static Properties getProxiedProps_readWritePlugin() {
    Properties props = initDefaultProxiedProps();
    addReadWritePlugin(props);
    return props;
  }

  private static void addAllPlugins(Properties props) {
    props.setProperty(PropertyKey.connectionPluginFactories.getKeyName(),
            "com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory," +
                    "com.mysql.cj.jdbc.ha.plugins.failover.FailoverConnectionPluginFactory," +
                    "com.mysql.cj.jdbc.ha.plugins.NodeMonitoringConnectionPluginFactory");
  }

  private static void addReadWritePlugin(Properties props) {
    props.setProperty(PropertyKey.connectionPluginFactories.getKeyName(),
            "com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory");
  }

  private boolean pluginChainIncludesFailoverPlugin(Properties props) {
    String plugins = props.getProperty(PropertyKey.connectionPluginFactories.getKeyName());
    return plugins.contains("FailoverConnectionPlugin");
  }

  private void enableAllProxies() {
    proxyMap.forEach((instance, proxy) -> {
      assertNotNull(proxy, "Proxy isn't found for " + instance);
      containerHelper.enableConnectivity(proxy);
    });
  }
}
