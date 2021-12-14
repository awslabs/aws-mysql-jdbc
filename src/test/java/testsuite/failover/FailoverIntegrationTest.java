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

import com.mysql.cj.log.StandardLogger;
import testsuite.UnreliableSocketFactory;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DBCluster;
import com.amazonaws.services.rds.model.DBClusterMember;
import com.amazonaws.services.rds.model.DescribeDBClustersRequest;
import com.amazonaws.services.rds.model.DescribeDBClustersResult;
import com.amazonaws.services.rds.model.FailoverDBClusterRequest;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.LogFactory;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import software.aws.rds.jdbc.mysql.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Integration testing with Aurora MySQL failover logic. */
@Disabled
@TestMethodOrder(MethodOrderer.MethodName.class)
public class FailoverIntegrationTest {

  private static final String DB_CONN_STR_PREFIX = "jdbc:mysql:aws://";
  private static final String DB_CONN_STR_SUFFIX = System.getenv("DB_CONN_STR_SUFFIX");
  private static final String DB_READONLY_CONN_STR_SUFFIX =
      System.getenv("DB_READONLY_CONN_STR_SUFFIX");

  private static final String TEST_DB_CLUSTER_IDENTIFIER =
      System.getenv("TEST_DB_CLUSTER_IDENTIFIER");
  private static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  private static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");

  private static final String TEST_DATABASE = "test";
  private static final String DB_HOST_PATTERN = "%s" + DB_CONN_STR_SUFFIX;

  protected int clusterSize = 0;
  protected String[] instanceIDs; // index 0 is always writer!
  protected final HashSet<String> instancesToCrash = new HashSet<>();
  protected ExecutorService crashInstancesExecutorService;

  private static final int CP_MIN_IDLE = 5;
  private static final int CP_MAX_IDLE = 10;
  private static final int CP_MAX_OPEN_PREPARED_STATEMENTS = 100;
  private static final int IS_VALID_TIMEOUT = 5;
  private static final String NO_SUCH_CLUSTER_MEMBER =
      "Cannot find cluster member whose db instance identifier is ";
  private static final String NO_WRITER_AVAILABLE =
      "Cannot get the id of the writer Instance in the cluster.";

  protected static final String SOCKET_TIMEOUT_VAL = "1000"; //ms
  protected static final String CONNECT_TIMEOUT_VAL = "3000"; //ms

  private final Log log;

  private final AmazonRDS rdsClient = AmazonRDSClientBuilder.standard().build();
  private Connection testConnection;

  /**
   * FailoverIntegrationTest constructor.
   * */
  public FailoverIntegrationTest() throws ClassNotFoundException {
    Class.forName("software.aws.rds.jdbc.mysql.Driver");
    this.log = LogFactory.getLogger(StandardLogger.class.getName(), Log.LOGGER_INSTANCE_NAME);

    initiateInstanceNames();
  }

  /* Writer connection failover tests. */

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new
   * writer. Driver failover occurs when executing a method against the connection
   */
  @Test
  public void test1_1_failFromWriterToNewWriter_failOnConnectionInvocation()
      throws SQLException, InterruptedException {
    final String initialWriterId = instanceIDs[0];

    testConnection = connectToWriterInstance(initialWriterId);

    // Crash Instance1 and nominate a new writer
    failoverClusterAndWaitUntilWriterChanged(initialWriterId);

    // Failure occurs on Connection invocation
    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to the new writer after failover happens.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceWriter(currentConnectionId));
    assertNotEquals(currentConnectionId, initialWriterId);
  }

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new
   * writer Driver failover occurs when executing a method against an object bound to the connection
   * (eg a Statement object created by the connection).
   */
  @Test
  public void test1_2_failFromWriterToNewWriter_failOnConnectionBoundObjectInvocation()
      throws SQLException, InterruptedException {
    final String initialWriterId = instanceIDs[0];

    testConnection = connectToWriterInstance(initialWriterId);
    Statement stmt = testConnection.createStatement();

    // Crash Instance1 and nominate a new writer
    failoverClusterAndWaitUntilWriterChanged(initialWriterId);

    // Failure occurs on Statement invocation
    assertFirstQueryThrows(stmt, "08S02");

    // Assert that the driver is connected to the new writer after failover happens.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceWriter(currentConnectionId));
    assertNotEquals(initialWriterId, currentConnectionId);
  }

  /** Current writer dies, no available reader instance, connection fails. */
  @Test
  public void test1_3_writerConnectionFailsDueToNoReader()
      throws SQLException {
    final String initialWriterId = instanceIDs[0];

    Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), CONNECT_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketFactory.getKeyName(), testsuite.UnreliableSocketFactory.class.getName());
    props.setProperty(PropertyKey.failoverTimeoutMs.getKeyName(), "10000");
    testConnection = connectToWriterInstance(initialWriterId, props);

    // Crash all reader instances (2 - 5).
    for (int i = 1; i < instanceIDs.length; i++) {
      UnreliableSocketFactory.downHost(String.format(DB_HOST_PATTERN, instanceIDs[i]));
    }

    // Crash the writer Instance1.
    UnreliableSocketFactory.downHost(String.format(DB_HOST_PATTERN, initialWriterId));

    // All instances should be down, assert exception thrown with SQLState code 08001
    // (SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE)
    assertFirstQueryThrows(testConnection, "08001");
  }

  /* ReadOnly connection failover tests. */

  /** Current reader dies, the driver failover to another reader. */
  @Test
  public void test2_1_failFromReaderToAnotherReader() throws SQLException {
    assertTrue(clusterSize >= 3, "Minimal cluster configuration: 1 writer + 2 readers");

    Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), CONNECT_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketFactory.getKeyName(), testsuite.UnreliableSocketFactory.class.getName());
    testConnection = connectToReaderInstance(instanceIDs[1], props);

    UnreliableSocketFactory.downHost(String.format(DB_HOST_PATTERN, instanceIDs[1]));

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are now connected to a new reader instance.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(currentConnectionId));
    assertNotEquals(currentConnectionId, instanceIDs[1]);
  }

  /** Current reader dies, other known reader instances do not respond, failover to writer. */
  @Test
  public void test2_2_failFromReaderToWriterWhenAllReadersAreDown()
      throws SQLException, InterruptedException {
    assertTrue(clusterSize >= 2, "Minimal cluster configuration: 1 writer + 1 reader");

    Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), CONNECT_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketFactory.getKeyName(), testsuite.UnreliableSocketFactory.class.getName());
    testConnection = connectToReaderInstance(instanceIDs[1], props);

    // Fist kill instances other than writer and connected reader
    for (int i = 2; i < instanceIDs.length; i++) {
      UnreliableSocketFactory.downHost(String.format(DB_HOST_PATTERN, instanceIDs[i]));
    }

    TimeUnit.SECONDS.sleep(3);

    // Then kill connected reader.
    UnreliableSocketFactory.downHost(String.format(DB_HOST_PATTERN, instanceIDs[1]));

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that the driver failed over to the writer instance (Instance1).
    final String currentConnectionId = queryInstanceId(testConnection);
    assertEquals(instanceIDs[0], currentConnectionId);
    assertTrue(isDBInstanceWriter(currentConnectionId));
  }

  /**
   * Current reader dies, after failing to connect to several reader instances, failover to another
   * reader.
   */
  @Test
  public void test2_3_failFromReaderToReaderWithSomeReadersAreDown()
      throws SQLException, InterruptedException {
    assertTrue(clusterSize >= 3, "Minimal cluster configuration: 1 writer + 2 readers");

    Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), CONNECT_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketFactory.getKeyName(), testsuite.UnreliableSocketFactory.class.getName());
    testConnection = connectToReaderInstance(instanceIDs[1], props);

    // First kill all reader instances except one
    for (int i = 1; i < instanceIDs.length - 1; i++) {
      UnreliableSocketFactory.downHost(String.format(DB_HOST_PATTERN, instanceIDs[i]));
    }

    TimeUnit.SECONDS.sleep(2);
    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we failed over to the only remaining reader instance (Instance5) OR Writer
    // instance (Instance1).
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(
        currentConnectionId.equals(instanceIDs[instanceIDs.length - 1]) || currentConnectionId.equals(instanceIDs[0]));
  }

  /**
   * Current reader dies, failover to another reader repeat to loop through instances in the cluster
   * testing ability to revive previously down reader instance.
   */
  @Test
  public void test2_4_failoverBackToThePreviouslyDownReader()
      throws Exception {
    assertTrue(clusterSize >= 5, "Minimal cluster configuration: 1 writer + 4 readers");

    final String firstReaderInstanceId = instanceIDs[1];

    // Connect to reader (Instance2).
    Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), CONNECT_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketFactory.getKeyName(), testsuite.UnreliableSocketFactory.class.getName());
    testConnection = connectToReaderInstance(firstReaderInstanceId, props);

    // Start crashing reader (Instance2).
    UnreliableSocketFactory.downHost(String.format(DB_HOST_PATTERN, firstReaderInstanceId));

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to another reader instance.
    final String secondReaderInstanceId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(secondReaderInstanceId));
    assertNotEquals(firstReaderInstanceId, secondReaderInstanceId);

    // Crash the second reader instance.
    UnreliableSocketFactory.downHost(String.format(DB_HOST_PATTERN, secondReaderInstanceId));

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to the third reader instance.
    final String thirdReaderInstanceId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(thirdReaderInstanceId));
    assertNotEquals(firstReaderInstanceId, thirdReaderInstanceId);
    assertNotEquals(secondReaderInstanceId, thirdReaderInstanceId);

    // Grab the id of the fourth reader instance.
    final HashSet<String> readerInstanceIds = new HashSet<>(Arrays.asList(instanceIDs));
    readerInstanceIds.remove(instanceIDs[0]);
    readerInstanceIds.remove(firstReaderInstanceId);
    readerInstanceIds.remove(secondReaderInstanceId);
    readerInstanceIds.remove(thirdReaderInstanceId);

    final String fourthInstanceId = readerInstanceIds.stream().findFirst().orElseThrow(() -> new Exception("Empty instance Id"));

    // Crash the fourth reader instance.
    UnreliableSocketFactory.downHost(String.format(DB_HOST_PATTERN, fourthInstanceId));

    // Stop crashing the first and second.
    UnreliableSocketFactory.dontDownHost(String.format(DB_HOST_PATTERN, firstReaderInstanceId));
    UnreliableSocketFactory.dontDownHost(String.format(DB_HOST_PATTERN, secondReaderInstanceId));

    final String currentInstanceId = queryInstanceId(testConnection);
    assertEquals(thirdReaderInstanceId, currentInstanceId);

    // Start crashing the third instance.
    UnreliableSocketFactory.downHost(String.format(DB_HOST_PATTERN, thirdReaderInstanceId));

    assertFirstQueryThrows(testConnection, "08S02");

    final String lastInstanceId = queryInstanceId(testConnection);

    assertTrue(
        firstReaderInstanceId.equals(lastInstanceId)
            || secondReaderInstanceId.equals(lastInstanceId));
  }

  /**
   * Current reader dies, no other reader instance, failover to writer, then writer dies, failover
   * to another available reader instance.
   */
  @Test
  public void test2_5_failFromReaderToWriterToAnyAvailableInstance()
      throws SQLException, InterruptedException {

    assertTrue(clusterSize >= 3, "Minimal cluster configuration: 1 writer + 2 readers");

    // Crashing all readers except the first one
    for (int i = 2; i < instanceIDs.length; i++) {
      UnreliableSocketFactory.downHost(String.format(DB_HOST_PATTERN, instanceIDs[i]));
    }

    // Connect to Instance2 which is the only reader that is up.
    Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), CONNECT_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketFactory.getKeyName(), testsuite.UnreliableSocketFactory.class.getName());
    testConnection = connectToReaderInstance(instanceIDs[1], props);

    // Crash Instance2
    UnreliableSocketFactory.downHost(String.format(DB_HOST_PATTERN, instanceIDs[1]));

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are currently connected to the writer Instance1.
    String currentConnectionId = queryInstanceId(testConnection);
    assertEquals(instanceIDs[0], currentConnectionId);
    assertTrue(isDBInstanceWriter(currentConnectionId));

    // Stop Crashing reader Instance2 and Instance3
    UnreliableSocketFactory.dontDownHost(String.format(DB_HOST_PATTERN, instanceIDs[1]));
    UnreliableSocketFactory.dontDownHost(String.format(DB_HOST_PATTERN, instanceIDs[2]));

    // Crash writer Instance1.
    failoverClusterToATargetAndWaitUntilWriterChanged(instanceIDs[0], instanceIDs[2]);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to one of the available instances.
    currentConnectionId = queryInstanceId(testConnection);
    assertTrue(
        instanceIDs[1].equals(currentConnectionId) || instanceIDs[2].equals(currentConnectionId));
  }

  /** Current reader dies, execute a “write“ statement, an exception should be thrown. */
  @Test
  public void test2_6_readerDiesAndExecuteWriteQuery() throws SQLException, InterruptedException {
    // Connect to Instance2 which is a reader.
    testConnection = connectToReaderInstance(instanceIDs[1]);

    // Crash Instance2.
    startCrashingInstances(instanceIDs[1]);
    makeSureInstancesDown(instanceIDs[1]);

    final Statement testStmt1 = testConnection.createStatement();

    // Assert that an exception with SQLState code S1009 is thrown.
    SQLException exception =
        assertThrows(
            SQLException.class, () -> testStmt1.executeUpdate("DROP TABLE IF EXISTS test2_6"));
    assertEquals("S1009", exception.getSQLState());

    // This ensures that the write statement above did not kick off a driver side failover.
    assertFirstQueryThrows(testConnection, "08S02");
  }

  /** Connect to a readonly cluster endpoint and ensure that we are doing a reader failover. */
  @Test
  public void test2_7_clusterEndpointReadOnlyFailover() throws SQLException {
    Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), CONNECT_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PropertyKey.socketFactory.getKeyName(), testsuite.UnreliableSocketFactory.class.getName());
    testConnection = createConnectionToReadonlyClusterEndpoint(props);

    final String initialConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(initialConnectionId));

    UnreliableSocketFactory.downHost(TEST_DB_CLUSTER_IDENTIFIER + DB_READONLY_CONN_STR_SUFFIX);
    UnreliableSocketFactory.downHost(String.format(DB_HOST_PATTERN, initialConnectionId));

    assertFirstQueryThrows(testConnection, "08S02");

    final String newConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(newConnectionId));
    assertNotEquals(newConnectionId, initialConnectionId);
  }

  /* Failure when within a transaction tests. */

  /** Writer fails within a transaction. Open transaction with "SET autocommit = 0" */
  @Test
  public void test3_1_writerFailWithinTransaction_setAutocommitSqlZero()
      throws SQLException, InterruptedException {
    final String initialClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(instanceIDs[0], initialClusterWriterId);

    testConnection = connectToWriterInstance(initialClusterWriterId);

    final Statement testStmt1 = testConnection.createStatement();
    testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_1");
    testStmt1.executeUpdate(
        "CREATE TABLE test3_1 (id int not null primary key, test3_1_field varchar(255) not null)");
    testStmt1.executeUpdate("SET autocommit = 0"); // open a new transaction

    final Statement testStmt2 = testConnection.createStatement();
    testStmt2.executeUpdate("INSERT INTO test3_1 VALUES (1, 'test field string 1')");

    failoverClusterAndWaitUntilWriterChanged(initialClusterWriterId);

    // If there is an active transaction, roll it back and return an error with SQLState 08007.
    final SQLException exception =
        assertThrows(
            SQLException.class,
            () -> testStmt2.executeUpdate("INSERT INTO test3_1 VALUES (2, 'test field string 2')"));
    assertEquals("08007", exception.getSQLState());

    // Attempt to query the instance id.
    final String currentConnectionId = queryInstanceId(testConnection);
    // Assert that we are connected to the new writer after failover happens.
    assertTrue(isDBInstanceWriter(currentConnectionId));
    final String nextClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(currentConnectionId, nextClusterWriterId);
    assertNotEquals(initialClusterWriterId, nextClusterWriterId);

    // testStmt2 can NOT be used anymore since it's invalid
    final Statement testStmt3 = testConnection.createStatement();
    final ResultSet rs = testStmt3.executeQuery("SELECT count(*) from test3_1");
    rs.next();
    // Assert that NO row has been inserted to the table;
    assertEquals(0, rs.getInt(1));

    testStmt3.executeUpdate("DROP TABLE IF EXISTS test3_1");
  }

  /** Writer fails within a transaction. Open transaction with setAutoCommit(false) */
  @Test
  public void test3_2_writerFailWithinTransaction_setAutoCommitFalse()
      throws SQLException, InterruptedException {
    final String initialClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(instanceIDs[0], initialClusterWriterId);

    testConnection = connectToWriterInstance(initialClusterWriterId);

    final Statement testStmt1 = testConnection.createStatement();
    testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_2");
    testStmt1.executeUpdate(
        "CREATE TABLE test3_2 (id int not null primary key, test3_2_field varchar(255) not null)");
    testConnection.setAutoCommit(false); // open a new transaction

    final Statement testStmt2 = testConnection.createStatement();
    testStmt2.executeUpdate("INSERT INTO test3_2 VALUES (1, 'test field string 1')");

    failoverClusterAndWaitUntilWriterChanged(initialClusterWriterId);

    // If there is an active transaction, roll it back and return an error with SQLState 08007.
    final SQLException exception =
        assertThrows(
            SQLException.class,
            () -> testStmt2.executeUpdate("INSERT INTO test3_2 VALUES (2, 'test field string 2')"));
    assertEquals("08007", exception.getSQLState());

    // Attempt to query the instance id.
    final String currentConnectionId = queryInstanceId(testConnection);
    // Assert that we are connected to the new writer after failover happens.
    assertTrue(isDBInstanceWriter(currentConnectionId));
    final String nextClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(currentConnectionId, nextClusterWriterId);
    assertNotEquals(initialClusterWriterId, nextClusterWriterId);

    // testStmt2 can NOT be used anymore since it's invalid

    final Statement testStmt3 = testConnection.createStatement();
    final ResultSet rs = testStmt3.executeQuery("SELECT count(*) from test3_2");
    rs.next();
    // Assert that NO row has been inserted to the table;
    assertEquals(0, rs.getInt(1));

    testStmt3.executeUpdate("DROP TABLE IF EXISTS test3_2");
  }

  /** Writer fails within a transaction. Open transaction with "START TRANSACTION". */
  @Test
  public void test3_3_writerFailWithinTransaction_startTransaction()
      throws SQLException, InterruptedException {
    final String initialClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(instanceIDs[0], initialClusterWriterId);

    testConnection = connectToWriterInstance(initialClusterWriterId);

    final Statement testStmt1 = testConnection.createStatement();
    testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_3");
    testStmt1.executeUpdate(
        "CREATE TABLE test3_3 (id int not null primary key, test3_3_field varchar(255) not null)");
    testStmt1.executeUpdate("START TRANSACTION"); // open a new transaction

    final Statement testStmt2 = testConnection.createStatement();
    testStmt2.executeUpdate("INSERT INTO test3_3 VALUES (1, 'test field string 1')");

    failoverClusterAndWaitUntilWriterChanged(initialClusterWriterId);

    // If there is an active transaction, roll it back and return an error with SQLState 08007.
    final SQLException exception =
        assertThrows(
            SQLException.class,
            () -> testStmt2.executeUpdate("INSERT INTO test3_3 VALUES (2, 'test field string 2')"));
    assertEquals("08007", exception.getSQLState());

    // Attempt to query the instance id.
    final String currentConnectionId = queryInstanceId(testConnection);
    // Assert that we are connected to the new writer after failover happens.
    assertTrue(isDBInstanceWriter(currentConnectionId));
    final String nextClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(currentConnectionId, nextClusterWriterId);
    assertNotEquals(initialClusterWriterId, nextClusterWriterId);

    // testStmt2 can NOT be used anymore since it's invalid

    final Statement testStmt3 = testConnection.createStatement();
    final ResultSet rs = testStmt3.executeQuery("SELECT count(*) from test3_3");
    rs.next();
    // Assert that NO row has been inserted to the table;
    assertEquals(0, rs.getInt(1));

    testStmt3.executeUpdate("DROP TABLE IF EXISTS test3_3");
  }

  /** Writer fails within NO transaction. */
  @Test
  public void test3_4_writerFailWithNoTransaction() throws SQLException, InterruptedException {
    final String initialClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(instanceIDs[0], initialClusterWriterId);

    testConnection = connectToWriterInstance(initialClusterWriterId);

    final Statement testStmt1 = testConnection.createStatement();
    testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_4");
    testStmt1.executeUpdate(
        "CREATE TABLE test3_4 (id int not null primary key, test3_4_field varchar(255) not null)");

    final Statement testStmt2 = testConnection.createStatement();
    testStmt2.executeUpdate("INSERT INTO test3_4 VALUES (1, 'test field string 1')");

    failoverClusterAndWaitUntilWriterChanged(initialClusterWriterId);

    final SQLException exception =
        assertThrows(
            SQLException.class,
            () -> testStmt2.executeUpdate("INSERT INTO test3_4 VALUES (2, 'test field string 2')"));
    assertEquals("08S02", exception.getSQLState());

    // Attempt to query the instance id.
    final String currentConnectionId = queryInstanceId(testConnection);
    // Assert that we are connected to the new writer after failover happens.
    assertTrue(isDBInstanceWriter(currentConnectionId));
    final String nextClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(currentConnectionId, nextClusterWriterId);
    assertNotEquals(initialClusterWriterId, nextClusterWriterId);

    // testStmt2 can NOT be used anymore since it's invalid
    final Statement testStmt3 = testConnection.createStatement();
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

  /* Pooled connection tests. */

  /** Writer connection failover within the connection pool. */
  @Test
  public void test4_1_pooledWriterConnection_BasicFailover()
      throws SQLException, InterruptedException {
    final String initialWriterId = getDBClusterWriterInstanceId();
    assertEquals(instanceIDs[0], initialWriterId);

    testConnection = createPooledConnectionWithInstanceId(initialWriterId);

    // Crash writer Instance1 and nominate Instance2 as the new writer
    failoverClusterToATargetAndWaitUntilWriterChanged(initialWriterId, instanceIDs[1]);

    assertFirstQueryThrows(testConnection, "08S02");

    // Execute Query again to get the current connection id;
    final String currentConnectionId = queryInstanceId(testConnection);

    // Assert that we are connected to the new writer after failover happens.
    assertTrue(isDBInstanceWriter(currentConnectionId));
    final String nextWriterId = getDBClusterWriterInstanceId();
    assertEquals(nextWriterId, currentConnectionId);
    assertEquals(instanceIDs[1], currentConnectionId);

    // Assert that the pooled connection is valid.
    assertTrue(testConnection.isValid(IS_VALID_TIMEOUT));
  }

  /** Reader connection failover within the connection pool. */
  @Test
  public void test4_2_pooledReaderConnection_BasicFailover()
      throws SQLException, InterruptedException {
    testConnection = createPooledConnectionWithInstanceId(instanceIDs[1]);
    testConnection.setReadOnly(true);

    startCrashingInstances(instanceIDs[1]);
    makeSureInstancesDown(instanceIDs[1]);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are now connected to a new reader instance.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(currentConnectionId));
    assertNotEquals(currentConnectionId, instanceIDs[1]);

    // Assert that the pooled connection is valid.
    assertTrue(testConnection.isValid(IS_VALID_TIMEOUT));
  }

  @Test
  public void test5_1_takeOverConnectionProperties() throws SQLException, InterruptedException {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "false");

    // Establish the topology cache so that we can later assert that testConnection does not inherit properties from
    // establishCacheConnection either before or after failover
    final Connection establishCacheConnection = DriverManager.getConnection(getClusterEndpoint(), props);
    establishCacheConnection.close();

    props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "true");
    testConnection = DriverManager.getConnection(getClusterEndpoint(), props);

    // Verify that connection accepts multi-statement sql
    final Statement myStmt = testConnection.createStatement();
    myStmt.executeQuery("select @@aurora_server_id; select 1; select 2;");

    // Crash Instance1 and nominate a new writer
    failoverClusterAndWaitUntilWriterChanged(instanceIDs[0]);

    assertFirstQueryThrows(testConnection, "08S02");

    // Verify that connection still accepts multi-statement sql
    final Statement myStmt1 = testConnection.createStatement();
    myStmt1.executeQuery("select @@aurora_server_id; select 1; select 2;");
  }

  /* Helper functions. */
  private String getClusterEndpoint() {
    String suffix = DB_CONN_STR_SUFFIX.startsWith(".") ? DB_CONN_STR_SUFFIX.substring(1) : DB_CONN_STR_SUFFIX;
    return DB_CONN_STR_PREFIX + TEST_DB_CLUSTER_IDENTIFIER + ".cluster-" + suffix;
  }

  private DBCluster getDBCluster() {
    DescribeDBClustersRequest dbClustersRequest =
        new DescribeDBClustersRequest().withDBClusterIdentifier(FailoverIntegrationTest.TEST_DB_CLUSTER_IDENTIFIER);
    DescribeDBClustersResult dbClustersResult = rdsClient.describeDBClusters(dbClustersRequest);
    List<DBCluster> dbClusterList = dbClustersResult.getDBClusters();
    return dbClusterList.get(0);
  }

  private void initiateInstanceNames() {
    this.log.logDebug("Initiating db instance names.");
    List<DBClusterMember> dbClusterMembers = getDBClusterMemberList();

    clusterSize = dbClusterMembers.size();
    assertTrue(clusterSize >= 2); // many tests assume that cluster contains at least a writer and a reader

    instanceIDs = dbClusterMembers.stream()
      .sorted(Comparator.comparing((DBClusterMember x) -> !x.isClusterWriter())
                .thenComparing(DBClusterMember::getDBInstanceIdentifier))
      .map(DBClusterMember::getDBInstanceIdentifier)
      .toArray(String[]::new);
  }

  private List<DBClusterMember> getDBClusterMemberList() {
    DBCluster dbCluster = getDBCluster();
    return dbCluster.getDBClusterMembers();
  }

  private DBClusterMember getMatchedDBClusterMember(String instanceId) {
    List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList().stream()
            .filter(dbClusterMember -> dbClusterMember.getDBInstanceIdentifier().equals(instanceId))
            .collect(Collectors.toList());
    if (matchedMemberList.isEmpty()) {
      throw new RuntimeException(NO_SUCH_CLUSTER_MEMBER + instanceId);
    }
    return matchedMemberList.get(0);
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

  private Boolean isDBInstanceWriter(String instanceId) {
    return getMatchedDBClusterMember(instanceId).isClusterWriter();
  }

  private Boolean isDBInstanceReader(String instanceId) {
    return !getMatchedDBClusterMember(instanceId).isClusterWriter();
  }

  private void failoverClusterAndWaitUntilWriterChanged(String clusterWriterId)
      throws InterruptedException {
    failoverCluster();
    waitUntilWriterInstanceChanged(clusterWriterId);
  }

  private void failoverCluster() throws InterruptedException {
    waitUntilClusterHasRightState();
    FailoverDBClusterRequest request =
        new FailoverDBClusterRequest().withDBClusterIdentifier(TEST_DB_CLUSTER_IDENTIFIER);

    while (true) {
      try {
        rdsClient.failoverDBCluster(request);
        break;
      } catch (Exception e) {
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
        TimeUnit.MILLISECONDS.sleep(1000);
      }
    }

    this.log.logDebug("Cluster failover request successful.");
  }

  private void waitUntilWriterInstanceChanged(String initialWriterInstanceId)
      throws InterruptedException {
    this.log.logDebug("Wait until the writer is not " + initialWriterInstanceId + " anymore.");
    String nextClusterWriterId = getDBClusterWriterInstanceId();
    while (initialWriterInstanceId.equals(nextClusterWriterId)) {
      TimeUnit.MILLISECONDS.sleep(3000);
      // Calling the RDS API to get writer Id.
      nextClusterWriterId = getDBClusterWriterInstanceId();
    }

    this.log.logDebug("Writer is now " + nextClusterWriterId);
  }

  private void waitUntilClusterHasRightState() throws InterruptedException {
    this.log.logDebug("Wait until cluster is in available state.");
    String status = getDBCluster().getStatus();
    while (!"available".equalsIgnoreCase(status)) {
      TimeUnit.MILLISECONDS.sleep(1000);
      status = getDBCluster().getStatus();
    }
    this.log.logDebug("Cluster is available.");
  }

  protected Connection createConnectionToReadonlyClusterEndpoint(Properties props) throws SQLException {
    DriverManager.setLoginTimeout(30);
    return DriverManager.getConnection(DB_CONN_STR_PREFIX + TEST_DB_CLUSTER_IDENTIFIER + DB_READONLY_CONN_STR_SUFFIX, props);
  }

  protected Connection createConnectionToInstanceWithId(String instanceID) throws SQLException {
    DriverManager.setLoginTimeout(30);
    return DriverManager.getConnection(
        DB_CONN_STR_PREFIX + instanceID + DB_CONN_STR_SUFFIX + "/" + TEST_DATABASE, TEST_USERNAME, TEST_PASSWORD);
  }

  protected Connection createConnectionToInstanceWithId(String instanceID, Properties props) throws SQLException {
    DriverManager.setLoginTimeout(30);
    return DriverManager.getConnection(
            DB_CONN_STR_PREFIX + instanceID + DB_CONN_STR_SUFFIX + "/" + TEST_DATABASE, props);
  }

  protected Connection createConnectionWithProxyDisabled(String instanceID) throws SQLException {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.enableClusterAwareFailover.getKeyName(), "false");
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "2000");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "2000");
    DriverManager.setLoginTimeout(2);

    return DriverManager.getConnection(DB_CONN_STR_PREFIX + instanceID + DB_CONN_STR_SUFFIX, props);
  }

  protected Connection createPooledConnectionWithInstanceId(String instanceID) throws SQLException {
    BasicDataSource ds = new BasicDataSource();
    ds.setUrl(DB_CONN_STR_PREFIX + instanceID + DB_CONN_STR_SUFFIX);
    ds.setUsername(TEST_USERNAME);
    ds.setPassword(TEST_PASSWORD);
    ds.setMinIdle(CP_MIN_IDLE);
    ds.setMaxIdle(CP_MAX_IDLE);
    ds.setMaxOpenPreparedStatements(CP_MAX_OPEN_PREPARED_STATEMENTS);

    return ds.getConnection();
  }

  protected Connection connectToReaderInstance(String readerInstanceId) throws SQLException {
    final Connection testConnection = createConnectionToInstanceWithId(readerInstanceId);
    testConnection.setReadOnly(true);
    assertTrue(isDBInstanceReader(queryInstanceId(testConnection)));
    return testConnection;
  }

  protected Connection connectToReaderInstance(String readerInstanceId, Properties props) throws SQLException {
    final Connection testConnection = createConnectionToInstanceWithId(readerInstanceId, props);
    testConnection.setReadOnly(true);
    assertTrue(isDBInstanceReader(queryInstanceId(testConnection)));
    return testConnection;
  }

  protected Connection connectToWriterInstance(String writerInstanceId) throws SQLException {
    final Connection testConnection = createConnectionToInstanceWithId(writerInstanceId);
    assertTrue(isDBInstanceWriter(queryInstanceId(testConnection)));
    return testConnection;
  }

  protected Connection connectToWriterInstance(String writerInstanceId, Properties props) throws SQLException {
    final Connection testConnection = createConnectionToInstanceWithId(writerInstanceId, props);
    assertTrue(isDBInstanceWriter(queryInstanceId(testConnection)));
    return testConnection;
  }

  protected String queryInstanceId(Connection connection) throws SQLException {
    try (Statement myStmt = connection.createStatement()) {
      return executeInstanceIdQuery(myStmt);
    }
  }

  protected String executeInstanceIdQuery(Statement stmt) throws SQLException {
    try (ResultSet rs = stmt.executeQuery("select @@aurora_server_id")) {
      if (rs.next()) {
        return rs.getString("@@aurora_server_id");
      }
    }
    return null;
  }

  // Attempt to run a query after the instance is down.
  // This should initiate the driver failover, first query after a failover
  // should always throw with the expected error message.
  private void assertFirstQueryThrows(Connection connection, String expectedSQLErrorCode) {
    this.log.logDebug(
        "Assert that the first read query throws, "
            + "this should kick off the driver failover process..");
    SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(connection));
    assertEquals(expectedSQLErrorCode, exception.getSQLState());
  }

  private void assertFirstQueryThrows(Statement stmt, String expectedSQLErrorCode) {
    this.log.logDebug(
            "Assert that the first read query throws, "
                    + "this should kick off the driver failover process..");
    SQLException exception = assertThrows(SQLException.class, () -> executeInstanceIdQuery(stmt));
    assertEquals(expectedSQLErrorCode, exception.getSQLState());
  }

  @BeforeEach
  private void validateCluster() throws InterruptedException {
    this.log.logDebug("Validating cluster.");

    crashInstancesExecutorService = Executors.newFixedThreadPool(instanceIDs.length);
    instancesToCrash.clear();
    for (String id : instanceIDs) {
      crashInstancesExecutorService.submit(() -> {
        while (true) {
          if (instancesToCrash.contains(id)) {
            try (Connection conn = createConnectionWithProxyDisabled(id);
                 Statement myStmt = conn.createStatement()
            ) {
              myStmt.execute("ALTER SYSTEM CRASH INSTANCE");
            } catch (SQLException e) {
              // Do nothing and keep creating connection to crash instance.
            }
          }
          TimeUnit.MILLISECONDS.sleep(100);
        }
      });
    }
    crashInstancesExecutorService.shutdown();

    makeSureInstancesUp(instanceIDs);
    UnreliableSocketFactory.flushAllStaticData();

    this.log.logDebug("===================== Pre-Test init is done. Ready for test =====================");
  }

  @AfterEach
  private void reviveInstancesAndCloseTestConnection() throws InterruptedException {
    this.log.logDebug("===================== Test is over. Post-Test clean-up is below. =====================");

    try {
      testConnection.close();
    } catch (Exception ex) {
      // ignore
    }
    testConnection = null;

    instancesToCrash.clear();
    crashInstancesExecutorService.shutdownNow();

    makeSureInstancesUp(instanceIDs, false);
  }

  protected void startCrashingInstances(String... instances) {
    instancesToCrash.addAll(Arrays.asList(instances));
  }

  protected void makeSureInstancesUp(String... instances) throws InterruptedException {
    makeSureInstancesUp(instances, true);
  }

  protected void makeSureInstancesUp(String[] instances, boolean finalCheck) throws InterruptedException {
    this.log.logDebug("Wait until the following instances are up: \n" + String.join("\n", instances));
    ExecutorService executorService = Executors.newFixedThreadPool(instances.length);
    final HashSet<String> remainingInstances = new HashSet<>(Arrays.asList(instances));

    for (String id : instances) {
      executorService.submit(() -> {
        while (true) {
          try (Connection conn = createConnectionWithProxyDisabled(id)) {
            conn.close();
            remainingInstances.remove(id);
            break;
          } catch (SQLException ex) {
            // Continue waiting until instance is up.
          }
          TimeUnit.MILLISECONDS.sleep(500);
        }
        return null;
      });
    }
    executorService.shutdown();
    executorService.awaitTermination(120, TimeUnit.SECONDS);

    if (finalCheck) {
      assertTrue(remainingInstances.isEmpty(), "The following instances are still down: \n"
              + String.join("\n", remainingInstances));
    }

    this.log.logDebug("The following instances are up: \n" + String.join("\n", instances));
  }

  protected void makeSureInstancesDown(String... instances) throws InterruptedException {
    this.log.logDebug("Wait until the following instances are down: \n" + String.join("\n", instances));
    ExecutorService executorService = Executors.newFixedThreadPool(instances.length);
    final HashSet<String> remainingInstances = new HashSet<>(Arrays.asList(instances));

    for (String id : instances) {
      executorService.submit(() -> {
        while (true) {
          try (Connection conn = createConnectionWithProxyDisabled(id)) {
            // Continue waiting until instance is down.
          } catch (SQLException e) {
            remainingInstances.remove(id);
            break;
          }
          TimeUnit.MILLISECONDS.sleep(500);
        }
        return null;
      });
    }
    executorService.shutdown();
    executorService.awaitTermination(120, TimeUnit.SECONDS);

    assertTrue(remainingInstances.isEmpty(), "The following instances are still up: \n"
            + String.join("\n", remainingInstances));

    this.log.logDebug("The following instances are down: \n" + String.join("\n", instances));
  }
}
