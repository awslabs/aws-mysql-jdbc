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

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.LogFactory;
import com.mysql.cj.log.StandardLogger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import testsuite.UnreliableSocketFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration testing with Aurora MySQL failover logic during network failures.
 * The following tests require a cluster with the following instance names:
 * mysql-instance-1
 * mysql-instance-2
 * mysql-instance-3
 * mysql-instance-4
 * mysql-instance-5
 */

@Disabled
@TestMethodOrder(MethodOrderer.MethodName.class)
public class NetworkFailuresFailoverIntegrationTest {

  private final Log log;

  // Examples below refers to standard Aurora RDS endpoint: "XXX.cluster-YYY.ZZZ.rds.amazonaws.com"
  private static final String DB_CONN_STR_SUFFIX = System.getenv("DB_CONN_STR_SUFFIX"); // "YYY.ZZZ.rds.amazonaws.com"
  private static final String DB_CONN_HOST_BASE = DB_CONN_STR_SUFFIX.startsWith(".") ? DB_CONN_STR_SUFFIX.substring(1) : DB_CONN_STR_SUFFIX; // "YYY.ZZZ.rds.amazonaws.com"
  private static final String DB_CLUSTER_NAME = System.getenv("TEST_DB_CLUSTER_IDENTIFIER"); // "XXX"
  private static final String DB_DATABASE = "test";

  private static final String DB_INSTANCE_1 = "mysql-instance-1";
  private static final String DB_INSTANCE_2 = "mysql-instance-2";
  private static final String DB_INSTANCE_3 = "mysql-instance-3";
  private static final String DB_INSTANCE_4 = "mysql-instance-4";
  private static final String DB_INSTANCE_5 = "mysql-instance-5";
  private final String[] allInstances = {DB_INSTANCE_1, DB_INSTANCE_2, DB_INSTANCE_3, DB_INSTANCE_4, DB_INSTANCE_5};

  private static final String DB_HOST_CLUSTER = DB_CLUSTER_NAME + ".cluster-" + DB_CONN_HOST_BASE;
  private static final String DB_HOST_CLUSTER_RO = DB_CLUSTER_NAME + ".cluster-ro-" + DB_CONN_HOST_BASE;
  private static final String DB_HOST_INSTANCE_PATTERN = "%s." + DB_CONN_HOST_BASE;

  private static final String DB_CONN_CLUSTER = "jdbc:mysql:aws://" + DB_HOST_CLUSTER + "/" + DB_DATABASE;
  private static final String DB_CONN_CLUSTER_RO = "jdbc:mysql:aws://" + DB_HOST_CLUSTER_RO + "/" + DB_DATABASE;
  private static final String DB_CONN_INSTANCE_PATTERN = "jdbc:mysql:aws://" + DB_HOST_INSTANCE_PATTERN + "/" + DB_DATABASE;

  private static final String DB_USER = System.getenv("TEST_USERNAME");
  private static final String DB_PASS = System.getenv("TEST_PASSWORD");

  public NetworkFailuresFailoverIntegrationTest() throws ClassNotFoundException {
    Class.forName("software.aws.rds.jdbc.mysql.Driver");
    this.log = LogFactory.getLogger(StandardLogger.class.getName(), Log.LOGGER_INSTANCE_NAME);
  }

  /**
   * Driver loses connection to a current writer host, tries to reconnect to it and fails.
   */
  @Test
  public void test_1_LostConnectionToWriter() throws SQLException {
    UnreliableSocketFactory.flushAllStaticData();

    Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), DB_USER);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), DB_PASS);
    props.setProperty(PropertyKey.gatherPerfMetrics.getKeyName(), "true");
    props.setProperty(PropertyKey.socketFactory.getKeyName(), testsuite.UnreliableSocketFactory.class.getName());
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "1000");
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "3000");
    props.setProperty(PropertyKey.failoverTimeoutMs.getKeyName(), "10000");
    final Connection testConnection = DriverManager.getConnection(DB_CONN_CLUSTER, props);

    String currentWriter = selectSingleRow(testConnection, "SELECT @@aurora_server_id");

    String lastConnectedHost = UnreliableSocketFactory.getHostFromLastConnection();
    assertEquals(UnreliableSocketFactory.STATUS_CONNECTED + DB_HOST_CLUSTER, lastConnectedHost);

    // put down current writer host
    UnreliableSocketFactory.downHost(DB_HOST_CLUSTER);
    UnreliableSocketFactory.downHost(String.format(DB_HOST_INSTANCE_PATTERN, currentWriter));

    SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
    assertEquals("08001", exception.getSQLState());

    testConnection.close();
  }

  /**
   * Driver loses connection to a reader host, tries to reconnect to another reader and achieve success.
   */
  @Test
  public void test_2_LostConnectionToReader() throws SQLException {
    UnreliableSocketFactory.flushAllStaticData();

    Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), DB_USER);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), DB_PASS);
    props.setProperty(PropertyKey.gatherPerfMetrics.getKeyName(), "true");
    props.setProperty(PropertyKey.socketFactory.getKeyName(), "testsuite.UnreliableSocketFactory");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "1000");
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "3000");
    final Connection testConnection = DriverManager.getConnection(DB_CONN_CLUSTER_RO, props);

    String currentReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
    this.log.logInfo("Current reader: " + currentReader);

    // put down current reader host
    UnreliableSocketFactory.downHost(DB_HOST_CLUSTER_RO);
    UnreliableSocketFactory.downHost(String.format(DB_HOST_INSTANCE_PATTERN, currentReader));

    SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
    assertEquals("08S02", exception.getSQLState());

    String newReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
    this.log.logInfo("New reader: " + newReader);

    testConnection.close();
  }

  /**
   * Driver loses connection to a reader host, tries to reconnect to another reader, fails and connects to a writer.
   */
  @Test
  public void test_3_LostConnectionToReader() throws SQLException {
    UnreliableSocketFactory.flushAllStaticData();

    Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), DB_USER);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), DB_PASS);
    props.setProperty(PropertyKey.enableClusterAwareFailover.getKeyName(), "false");
    final Connection checkWriterConnection = DriverManager.getConnection(DB_CONN_CLUSTER, props);
    String currentWriter = selectSingleRow(checkWriterConnection, "SELECT @@aurora_server_id");
    this.log.logInfo("Current writer: " + currentWriter);
    checkWriterConnection.close();

    props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), DB_USER);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), DB_PASS);
    props.setProperty(PropertyKey.gatherPerfMetrics.getKeyName(), "true");
    props.setProperty(PropertyKey.socketFactory.getKeyName(), "testsuite.UnreliableSocketFactory");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "1000");
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "3000");
    final Connection testConnection = DriverManager.getConnection(DB_CONN_CLUSTER_RO, props);

    String currentReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
    this.log.logInfo("Current reader: " + currentReader);

    // put down all hosts except a writer
    UnreliableSocketFactory.downHost(DB_HOST_CLUSTER_RO);
    for (String host : this.allInstances) {
      if (!host.equalsIgnoreCase(currentWriter)) {
        UnreliableSocketFactory.downHost(String.format(DB_HOST_INSTANCE_PATTERN, host));
        this.log.logInfo("Put down host: " + String.format(DB_HOST_INSTANCE_PATTERN, host));
      }
    }

    SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
    assertEquals("08S02", exception.getSQLState());

    String newReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
    this.log.logInfo("New reader: " + newReader);
    assertEquals(currentWriter, newReader);

    testConnection.close();
  }

  /**
   * Driver is connected to a reader instance.
   * When driver loses connection to this reader host, it connects to a writer.
   */
  @Test
  public void test_4_LostConnectionToReaderAndConnectToWriter() throws SQLException {
    UnreliableSocketFactory.flushAllStaticData();

    Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), DB_USER);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), DB_PASS);
    props.setProperty(PropertyKey.enableClusterAwareFailover.getKeyName(), "false");
    final Connection checkWriterConnection = DriverManager.getConnection(DB_CONN_CLUSTER, props);
    String currentWriter = selectSingleRow(checkWriterConnection, "SELECT @@aurora_server_id");
    this.log.logInfo("Current writer: " + currentWriter);
    checkWriterConnection.close();

    ArrayList<String> readers = new ArrayList<>(Arrays.asList(this.allInstances));
    readers.remove(currentWriter);
    Collections.shuffle(readers);
    String anyReader = readers.get(0);
    this.log.logInfo("Test connection to reader: " + anyReader);

    props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), DB_USER);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), DB_PASS);
    props.setProperty(PropertyKey.gatherPerfMetrics.getKeyName(), "true");
    props.setProperty(PropertyKey.socketFactory.getKeyName(), "testsuite.UnreliableSocketFactory");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "1000");
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "3000");
    final Connection testConnection = DriverManager.getConnection(String.format(DB_CONN_INSTANCE_PATTERN, anyReader), props);

    String currentReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
    this.log.logInfo("Current reader: " + currentReader);
    assertEquals(anyReader, currentReader);

    // put down current reader
    UnreliableSocketFactory.downHost(String.format(DB_HOST_INSTANCE_PATTERN, currentReader));

    SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
    assertEquals("08S02", exception.getSQLState());

    String newInstance = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
    this.log.logInfo("New connection to: " + newInstance);
    assertEquals(currentWriter, newInstance);

    testConnection.close();
  }

  /**
   * Driver is connected to a reader instance and connection is read-only.
   * When driver loses connection to this reader host, it connects to another reader.
   */
  @Test
  public void test_5_LostConnectionToReaderAndConnectToReader() throws SQLException {
    UnreliableSocketFactory.flushAllStaticData();

    Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), DB_USER);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), DB_PASS);
    props.setProperty(PropertyKey.enableClusterAwareFailover.getKeyName(), "false");
    final Connection checkWriterConnection = DriverManager.getConnection(DB_CONN_CLUSTER, props);
    String currentWriter = selectSingleRow(checkWriterConnection, "SELECT @@aurora_server_id");
    this.log.logInfo("Current writer: " + currentWriter);
    checkWriterConnection.close();

    ArrayList<String> readers = new ArrayList<>(Arrays.asList(this.allInstances));
    readers.remove(currentWriter);
    Collections.shuffle(readers);
    String anyReader = readers.get(0);
    this.log.logInfo("Test connection to reader: " + anyReader);

    props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), DB_USER);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), DB_PASS);
    props.setProperty(PropertyKey.gatherPerfMetrics.getKeyName(), "true");
    props.setProperty(PropertyKey.socketFactory.getKeyName(), "testsuite.UnreliableSocketFactory");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "1000");
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "3000");
    final Connection testConnection = DriverManager.getConnection(String.format(DB_CONN_INSTANCE_PATTERN, anyReader), props);

    String currentReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
    this.log.logInfo("Current reader: " + currentReader);
    assertEquals(anyReader, currentReader);

    testConnection.setReadOnly(true);

    // put down current reader
    UnreliableSocketFactory.downHost(String.format(DB_HOST_INSTANCE_PATTERN, currentReader));

    SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
    assertEquals("08S02", exception.getSQLState());

    String newInstance = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
    this.log.logInfo("New connection to: " + newInstance);
    assertNotEquals(currentWriter, newInstance);

    testConnection.close();
  }

  private String selectSingleRow(Connection connection, String sql) throws SQLException {
    try (Statement myStmt = connection.createStatement();
         ResultSet result = myStmt.executeQuery(sql)) {
      if (result.next()) {
        return result.getString(1);
      }
      return null;
    }
  }

}
