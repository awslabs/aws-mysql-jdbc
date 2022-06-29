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

package testsuite.integration.container.standard;

import com.mysql.cj.exceptions.MysqlErrorNumbers;
import eu.rekawek.toxiproxy.Proxy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class StandardMysqlReadWriteSplittingTest extends StandardMysqlBaseTest {

  @Test
  public void test_connectToWriter_setReadOnlyTrueTrueFalseFalseTrue() throws SQLException {
    try (final Connection conn = connect()) {
      String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      conn.setReadOnly(true);
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(readerConnectionId, currentConnectionId);

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);

      conn.setReadOnly(true);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(readerConnectionId, currentConnectionId);
    }
  }

  @Test
  public void test_setReadOnlyFalseInReadOnlyTransaction() throws SQLException{
    try (final Connection conn = connect()) {
      String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      final Statement stmt = conn.createStatement();
      stmt.execute("START TRANSACTION READ ONLY");
      stmt.executeQuery("SELECT @@hostname");

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(MysqlErrorNumbers.SQL_STATE_ACTIVE_SQL_TRANSACTION, exception.getSQLState());
      assertEquals(readerConnectionId, currentConnectionId);

      stmt.execute("COMMIT");

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @Test
  public void test_setReadOnlyFalseInTransaction_setAutocommitFalse() throws SQLException{
    try (final Connection conn = connect()) {
      String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      final Statement stmt = conn.createStatement();
      conn.setAutoCommit(false);
      stmt.executeQuery("SELECT COUNT(*) FROM information_schema.tables");

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(MysqlErrorNumbers.SQL_STATE_ACTIVE_SQL_TRANSACTION, exception.getSQLState());
      assertEquals(readerConnectionId, currentConnectionId);

      stmt.execute("COMMIT");

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @Test
  public void test_setReadOnlyFalseInTransaction_setAutocommitZero() throws SQLException{
    try (final Connection conn = connect()) {
      String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

      final Statement stmt = conn.createStatement();
      stmt.execute("SET autocommit = 0");
      stmt.executeQuery("SELECT COUNT(*) FROM information_schema.tables");

      final SQLException exception = assertThrows(SQLException.class, () -> conn.setReadOnly(false));
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(MysqlErrorNumbers.SQL_STATE_ACTIVE_SQL_TRANSACTION, exception.getSQLState());
      assertEquals(readerConnectionId, currentConnectionId);

      stmt.execute("COMMIT");

      conn.setReadOnly(false);
      currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @Test
  public void test_setReadOnlyTrueInTransaction() throws SQLException{
    try (final Connection conn = connect()) {
      String writerConnectionId = queryInstanceId(conn);

      final Statement stmt1 = conn.createStatement();
      stmt1.executeUpdate("DROP TABLE IF EXISTS test_splitting_readonly_transaction");
      stmt1.executeUpdate("CREATE TABLE test_splitting_readonly_transaction (id int not null primary key, text_field varchar(255) not null)");
      stmt1.execute("SET autocommit = 0");

      final Statement stmt2 = conn.createStatement();
      stmt2.executeUpdate("INSERT INTO test_splitting_readonly_transaction VALUES (1, 'test_field value 1')");

      assertDoesNotThrow(() -> conn.setReadOnly(true));
      String currentConnectionId = queryInstanceId(conn);
      assertEquals(writerConnectionId, currentConnectionId);

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
    try (Connection conn = connectWithProxy()) {
      String writerConnectionId = queryInstanceId(conn);

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
      String currentConnectionId = assertDoesNotThrow(() -> queryInstanceId(conn));
      assertEquals(writerConnectionId, currentConnectionId);

      assertDoesNotThrow(() -> conn.setReadOnly(false));
      currentConnectionId = assertDoesNotThrow(() -> queryInstanceId(conn));
      assertEquals(writerConnectionId, currentConnectionId);
    }
  }

  @Test
  public void test_setReadOnlyTrue_allInstancesDown() throws SQLException, IOException {
    try (Connection conn = connectWithProxy()) {
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
      
      assertDoesNotThrow(() -> conn.setReadOnly(true));
      final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, exception.getSQLState());
    }
  }

  @Test
  public void test_setReadOnlyTrue_allInstancesDown_writerClosed() throws SQLException, IOException {
    try (Connection conn = connectWithProxy()) {
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
    try (Connection conn = connectWithProxy()) {
      String writerConnectionId = queryInstanceId(conn);

      conn.setReadOnly(true);
      String readerConnectionId = queryInstanceId(conn);
      assertNotEquals(writerConnectionId, readerConnectionId);

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
}
