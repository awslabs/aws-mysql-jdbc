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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class AuroraMysqlReadWriteSplittingTest extends AuroraMysqlIntegrationBaseTest {

  @Test
  public void test_connectToWriter_setReadOnlyTrueFalseTrue() throws SQLException {
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, getPropsWithReadWritePlugin())) {
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

  @Test
  public void test_connectToReader_setReadOnlyTrueFalse() throws SQLException {
    final String initialReaderId = instanceIDs[1];

    try (final Connection conn = connectToInstance(initialReaderId + DB_CONN_STR_SUFFIX, MYSQL_PORT, getPropsWithReadWritePlugin())) {
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

  @Test
  public void test_failoverToNewWriter_setReadOnlyTrueFalse() throws SQLException, InterruptedException, IOException {
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, getProxiedPropsWithReadWritePlugin())) {
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

      proxyMap.forEach((instance, proxy) -> {
        assertNotNull(proxy, "Proxy isn't found for " + instance);
        containerHelper.enableConnectivity(proxy);
      });

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

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, getProxiedPropsWithReadWritePlugin())) {
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

      proxyMap.forEach((instance, proxy) -> {
        assertNotNull(proxy, "Proxy isn't found for " + instance);
        containerHelper.enableConnectivity(proxy);
      });

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

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, getProxiedPropsWithReadWritePlugin())) {
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

      proxyMap.forEach((instance, proxy) -> {
        assertNotNull(proxy, "Proxy isn't found for " + instance);
        containerHelper.enableConnectivity(proxy);
      });

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
  public void test_setReadOnlyFalseInReadOnlyTransaction() throws SQLException{
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, getPropsWithReadWritePlugin())) {
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

  @Test
  public void test_setReadOnlyFalseInTransaction_setAutocommitFalse() throws SQLException{
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, getPropsWithReadWritePlugin())) {
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

  @Test
  public void test_setReadOnlyFalseInTransaction_setAutocommitZero() throws SQLException{
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, getPropsWithReadWritePlugin())) {
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

  @Test
  public void test_setReadOnlyTrueInTransaction() throws SQLException{
    final String initialWriterId = instanceIDs[0];

    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, getPropsWithReadWritePlugin())) {
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

  @Test
  public void test_setReadOnlyTrue_allReadersDown() throws SQLException, IOException {
    String initialWriterId = instanceIDs[0];

    try (Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, getProxiedPropsWithReadWritePlugin())) {
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

  @Test
  public void test_setReadOnlyTrue_allInstancesDown() throws SQLException, IOException {
    final String initialWriterId = instanceIDs[0];

    Properties props = getProxiedPropsWithReadWritePlugin();
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

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(true));
      assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, exception.getSQLState());
    }
  }

  @Test
  public void test_setReadOnlyTrue_allInstancesDown_writerClosed() throws SQLException, IOException {
    final String initialWriterId = instanceIDs[0];

    try (Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, getProxiedPropsWithReadWritePlugin())) {
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

  @Test
  public void test_setReadOnlyFalse_allInstancesDown() throws SQLException, IOException {
    final String initialReaderId = instanceIDs[1];

    try (Connection conn = connectToInstance(initialReaderId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, getProxiedPropsWithReadWritePlugin())) {
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
  public void test_multiHostUrl_topologyOverridesHostList() throws SQLException {
    final String initialWriterId = instanceIDs[0];

    try (Connection conn = DriverManager.getConnection(DB_CONN_STR_PREFIX + initialWriterId + DB_CONN_STR_SUFFIX + ",non-existent-host", getPropsWithReadWritePlugin())) {
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
  public void test_readerLoadBalancing_autocommitTrue() throws SQLException {
    final String initialWriterId = instanceIDs[0];

    Properties props = getPropsWithReadWritePlugin();
    props.setProperty(PropertyKey.loadBalanceReadOnlyTraffic.getKeyName(), "true");
    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      String previousReaderId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, previousReaderId);
      assertTrue(isDBInstanceReader(previousReaderId));

      for (int i = 0; i < 5; i++) {
        String nextReaderId = queryInstanceId(conn);
        assertNotEquals(previousReaderId, nextReaderId);
        assertTrue(isDBInstanceReader(previousReaderId));
        previousReaderId = nextReaderId;
      }

      for (int i = 0; i < 5; i++) {
        Statement stmt = conn.createStatement();
        stmt.executeQuery("SELECT " + i);

        ResultSet rs = stmt.getResultSet();
        rs.next();
        assertEquals(i, rs.getInt(1));
      }
    }
  }

  @Test
  public void test_readerLoadBalancing_autocommitFalse() throws SQLException {
    final String initialWriterId = instanceIDs[0];

    Properties props = getPropsWithReadWritePlugin();
    props.setProperty(PropertyKey.loadBalanceReadOnlyTraffic.getKeyName(), "true");
    try (final Connection conn = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX, MYSQL_PORT, props)) {
      String writerConnectionId = queryInstanceId(conn);
      assertEquals(initialWriterId, writerConnectionId);
      assertTrue(isDBInstanceWriter(writerConnectionId));

      conn.setReadOnly(true);
      conn.setAutoCommit(false);
      String previousReaderId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, previousReaderId);
      assertTrue(isDBInstanceReader(previousReaderId));

      Statement stmt = conn.createStatement();
      for (int i = 0; i < 5; i++) {
        stmt.executeQuery("SELECT " + i);
        conn.commit();
        String nextReaderId = queryInstanceId(conn);
        assertNotEquals(previousReaderId, nextReaderId);
        assertTrue(isDBInstanceReader(nextReaderId));
        previousReaderId = nextReaderId;
      }

      for (int i = 0; i < 5; i++) {
        stmt.executeQuery("SELECT " + i);
        stmt.execute("COMMIT");
        String nextReaderId = queryInstanceId(conn);
        assertNotEquals(previousReaderId, nextReaderId);
        assertTrue(isDBInstanceReader(nextReaderId));
        previousReaderId = nextReaderId;
      }

      for (int i = 0; i < 5; i++) {
        stmt.executeQuery("SELECT " + i);
        conn.rollback();
        String nextReaderId = queryInstanceId(conn);
        assertNotEquals(previousReaderId, nextReaderId);
        assertTrue(isDBInstanceReader(nextReaderId));
        previousReaderId = nextReaderId;
      }

      for (int i = 0; i < 5; i++) {
        stmt.executeQuery("SELECT " + i);
        stmt.execute("ROLLBACK");
        String nextReaderId = queryInstanceId(conn);
        assertNotEquals(previousReaderId, nextReaderId);
        assertTrue(isDBInstanceReader(nextReaderId));
        previousReaderId = nextReaderId;
      }

      for (int i = 0; i < 5; i++) {
        stmt.executeQuery("SELECT " + i);
        conn.commit();

        ResultSet rs = stmt.getResultSet();
        rs.next();
        assertEquals(i, rs.getInt(1));
      }
    }
  }

  private Properties getPropsWithReadWritePlugin() {
    Properties props = initDefaultProps();
    addReadWriteSplittingPlugin(props);
    return props;
  }

  private Properties getProxiedPropsWithReadWritePlugin() {
    Properties props = initDefaultProxiedProps();
    addReadWriteSplittingPlugin(props);
    return props;
  }

  private void addReadWriteSplittingPlugin(Properties props) {
    props.setProperty(PropertyKey.connectionPluginFactories.getKeyName(),
            "com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory," +
                    "com.mysql.cj.jdbc.ha.plugins.failover.FailoverConnectionPluginFactory," +
                    "com.mysql.cj.jdbc.ha.plugins.NodeMonitoringConnectionPluginFactory");
  }
}
