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
import com.amazonaws.services.rds.model.DBCluster;
import com.amazonaws.services.rds.model.DBClusterMember;
import com.amazonaws.services.rds.model.DescribeDBClustersRequest;
import com.amazonaws.services.rds.model.DescribeDBClustersResult;
import com.amazonaws.services.rds.model.FailoverDBClusterRequest;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.LogFactory;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Integration testing with Aurora MySQL failover logic. */
@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class FailoverIntegrationTest {
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
  private static final String DB_CONN_STR_PREFIX = "jdbc:mysql:aws://";
  private static final String DB_CONN_STR_SUFFIX = System.getenv("DB_CONN_STR_SUFFIX");
  private static final String DB_READONLY_CONN_STR_SUFFIX =
      System.getenv("DB_READONLY_CONN_STR_SUFFIX");

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
  private static final int CP_MIN_IDLE = 5;
  private static final int CP_MAX_IDLE = 10;
  private static final int CP_MAX_OPEN_PREPARED_STATEMENTS = 100;
  private static final int IS_VALID_TIMEOUT = 5;
  private static final String NO_SUCH_CLUSTER_MEMBER =
      "Cannot find cluster member whose db instance identifier is ";
  private static final String NO_WRITER_AVAILABLE =
      "Cannot get the id of the writer Instance in the cluster.";

  private final Log log;

  private final AmazonRDS rdsClient = AmazonRDSClientBuilder.standard().build();
  private Connection testConnection;

  private CrashInstanceRunnable instanceCrasher1;
  private CrashInstanceRunnable instanceCrasher2;
  private CrashInstanceRunnable instanceCrasher3;
  private CrashInstanceRunnable instanceCrasher4;
  private CrashInstanceRunnable instanceCrasher5;

  private Map<String, CrashInstanceRunnable> instanceCrasherMap = new HashMap<>();

  /**
   * FailoverIntegrationTest constructor.
   * */
  public FailoverIntegrationTest() throws SQLException {
    DriverManager.registerDriver(new software.aws.rds.jdbc.Driver());
    this.log = LogFactory.getLogger(software.aws.rds.jdbc.log.StandardLogger.class.getName(), Log.LOGGER_INSTANCE_NAME);

    initiateInstanceNames();

    instanceCrasher1 = new CrashInstanceRunnable(INSTANCE_ID_1);
    instanceCrasher2 = new CrashInstanceRunnable(INSTANCE_ID_2);
    instanceCrasher3 = new CrashInstanceRunnable(INSTANCE_ID_3);
    instanceCrasher4 = new CrashInstanceRunnable(INSTANCE_ID_4);
    instanceCrasher5 = new CrashInstanceRunnable(INSTANCE_ID_5);

    instanceCrasherMap.put(INSTANCE_ID_1, instanceCrasher1);
    instanceCrasherMap.put(INSTANCE_ID_2, instanceCrasher2);
    instanceCrasherMap.put(INSTANCE_ID_3, instanceCrasher3);
    instanceCrasherMap.put(INSTANCE_ID_4, instanceCrasher4);
    instanceCrasherMap.put(INSTANCE_ID_5, instanceCrasher5);
  }

  /* Writer connection failover tests. */

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new
   * writer. Driver failover occurs when executing a method against the connection
   */
  @Test
  public void test1_1_failFromWriterToNewWriter_failOnConnectionInvocation()
      throws SQLException, InterruptedException {
    final String initalWriterId = INSTANCE_ID_1;

    testConnection = connectToWriterInstance(initalWriterId);

    // Crash Instance1 and nominate a new writer
    failoverClusterAndWaitUntilWriterChanged(initalWriterId);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to the new writer after failover happens.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceWriter(currentConnectionId));
    assertNotEquals(currentConnectionId, initalWriterId);
  }

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new
   * writer Driver failover occurs when executing a method against an object bound to the connection
   * (eg a Statement object created by the connection).
   */
  @Test
  public void test1_2_failFromWriterToNewWriter_failOnConnectionBoundObjectInvocation()
      throws SQLException, InterruptedException {
    final String initalWriterId = INSTANCE_ID_1;

    testConnection = connectToWriterInstance(initalWriterId);

    // Crash Instance1 and nominate a new writer
    failoverClusterAndWaitUntilWriterChanged(initalWriterId);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that the driver is connected to the new writer after failover happens.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceWriter(currentConnectionId));
    assertNotEquals(initalWriterId, currentConnectionId);
  }

  /** Current writer dies, no available reader instance, connection fails. */
  @Test
  public void test1_3_writerConnectionFailsDueToNoReader()
      throws SQLException, InterruptedException {
    final String initalWriterId = INSTANCE_ID_1;

    testConnection = connectToWriterInstance(initalWriterId);

    // Crash all reader instances (2 - 5).
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_2);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_3);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_4);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_5);

    // Crash the writer Instance1.
    startCrashingInstanceAndWaitUntilDown(initalWriterId);

    // All instances should be down, assert exception thrown with SQLState code 08001
    // (SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE)
    assertFirstQueryThrows(testConnection, "08001");
  }

  /* ReadOnly connection failover tests. */

  /** Current reader dies, the driver failover to another reader. */
  @Test
  public void test2_1_failFromReaderToAnotherReader() throws SQLException, InterruptedException {
    testConnection = connectToReaderInstance(INSTANCE_ID_2);

    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_2);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are now connected to a new reader instance.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(currentConnectionId));
    assertNotEquals(currentConnectionId, INSTANCE_ID_2);
  }

  /** Current reader dies, other known reader instances do not respond, failover to writer. */
  @Test
  public void test2_2_failFromReaderToWriterWhenAllReadersAreDown()
      throws SQLException, InterruptedException {
    testConnection = connectToReaderInstance(INSTANCE_ID_2);

    // Fist kill instances 3-5.
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_3);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_4);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_5);

    // Then kill instance 2.
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_2);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that the driver failed over to the writer instance (Instance1).
    final String currentConnectionId = queryInstanceId(testConnection);
    assertEquals(INSTANCE_ID_1, currentConnectionId);
    assertTrue(isDBInstanceWriter(currentConnectionId));
  }

  /**
   * Current reader dies, after failing to connect to several reader instances, failover to another
   * reader.
   */
  @Test
  public void test2_3_failFromReaderToReaderWithSomeReadersAreDown()
      throws SQLException, InterruptedException {
    testConnection = connectToReaderInstance(INSTANCE_ID_2);

    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_3);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_4);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_2);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we failed over to the only remaining reader instance (Instance5) OR Writer
    // instance (Instance1).
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(
        currentConnectionId.equals(INSTANCE_ID_5) || currentConnectionId.equals(INSTANCE_ID_1));
  }

  /**
   * Current reader dies, failover to another reader repeat to loop through instances in the cluster
   * testing ability to revive previously down reader instance.
   */
  @Test
  public void test2_4_failoverBackToThePreviouslyDownReader()
      throws SQLException, InterruptedException {
    final String firstReaderInstanceId = INSTANCE_ID_2;

    // Connect to reader (Instance2).
    testConnection = connectToReaderInstance(firstReaderInstanceId);

    // Start crashing reader (Instance2).
    startCrashingInstanceAndWaitUntilDown(firstReaderInstanceId);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to another reader instance.
    final String secondReaderInstanceId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(secondReaderInstanceId));
    assertNotEquals(firstReaderInstanceId, secondReaderInstanceId);

    // Crash the second reader instance.
    startCrashingInstanceAndWaitUntilDown(secondReaderInstanceId);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to the third reader instance.
    final String thirdReaderInstanceId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(thirdReaderInstanceId));
    assertNotEquals(firstReaderInstanceId, thirdReaderInstanceId);
    assertNotEquals(secondReaderInstanceId, thirdReaderInstanceId);

    // Grab the id of the fourth reader instance.
    final List<String> readerInstanceIds = getDBClusterReaderInstanceIds();
    readerInstanceIds.remove(INSTANCE_ID_1);
    readerInstanceIds.remove(INSTANCE_ID_2);
    readerInstanceIds.remove(secondReaderInstanceId);
    readerInstanceIds.remove(thirdReaderInstanceId);

    final String fourthInstanceId = readerInstanceIds.get(0);

    // Crash the fourth reader instance.
    startCrashingInstanceAndWaitUntilDown(fourthInstanceId);

    // Stop crashing the first and second.
    stopCrashingInstanceAndWaitUntilUp(firstReaderInstanceId);
    stopCrashingInstanceAndWaitUntilUp(secondReaderInstanceId);

    // Sleep 30+ seconds to force cache update upon successful query.
    Thread.sleep(31000);

    final String currentInstanceId = queryInstanceId(testConnection);
    assertEquals(thirdReaderInstanceId, currentInstanceId);

    // Start crashing the third instance.
    startCrashingInstanceAndWaitUntilDown(thirdReaderInstanceId);

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
    // Crashing Instance3, Instance4 and Instance5
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_3);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_4);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_5);

    // Connect to Instance2 which is the only reader that is up.
    testConnection = connectToReaderInstance(INSTANCE_ID_2);

    // Crash Instance2
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_2);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are currently connected to the writer Instance1.
    String currentConnectionId = queryInstanceId(testConnection);
    assertEquals(INSTANCE_ID_1, currentConnectionId);
    assertTrue(isDBInstanceWriter(currentConnectionId));

    // Stop Crashing reader Instance2 and Instance3
    stopCrashingInstanceAndWaitUntilUp(INSTANCE_ID_2);
    stopCrashingInstanceAndWaitUntilUp(INSTANCE_ID_3);

    // Crash writer Instance1.
    failoverClusterToATargetAndWaitUntilWriterChanged(INSTANCE_ID_1, INSTANCE_ID_3);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to one of the available instances.
    currentConnectionId = queryInstanceId(testConnection);
    assertTrue(
        INSTANCE_ID_2.equals(currentConnectionId) || INSTANCE_ID_3.equals(currentConnectionId));
  }

  /** Current reader dies, execute a “write“ statement, an exception should be thrown. */
  @Test
  public void test2_6_readerDiesAndExecuteWriteQuery() throws SQLException, InterruptedException {
    // Connect to Instance2 which is a reader.
    testConnection = connectToReaderInstance(INSTANCE_ID_2);

    // Crash Instance2.
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_2);

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
  public void test2_7_clusterEndpointReadOnlyFailover() throws SQLException, InterruptedException {
    testConnection = createConnectionToReadonlyClusterEndpoint();

    final String initialConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(initialConnectionId));

    startCrashingInstanceAndWaitUntilDown(initialConnectionId);

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
    assertEquals(INSTANCE_ID_1, initialClusterWriterId);

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
    assertEquals(INSTANCE_ID_1, initialClusterWriterId);

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
    assertEquals(INSTANCE_ID_1, initialClusterWriterId);

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
    assertEquals(INSTANCE_ID_1, initialClusterWriterId);

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
  public void test4_1_pooledWriterConnection_basicfailover()
      throws SQLException, InterruptedException {
    final String initalWriterId = getDBClusterWriterInstanceId();
    assertEquals(INSTANCE_ID_1, initalWriterId);

    testConnection = createPooledConnectionWithInstanceId(initalWriterId);

    // Crash writer Instance1 and nominate Instance2 as the new writer
    failoverClusterToATargetAndWaitUntilWriterChanged(initalWriterId, INSTANCE_ID_2);

    assertFirstQueryThrows(testConnection, "08S02");

    // Execute Query again to get the current connection id;
    final String currentConnectionId = queryInstanceId(testConnection);
    // Assert that we are connected to the new writer after failover happens.
    assertTrue(isDBInstanceWriter(currentConnectionId));
    final String nextWriterId = getDBClusterWriterInstanceId();
    assertEquals(nextWriterId, currentConnectionId);
    assertEquals(INSTANCE_ID_2, currentConnectionId);

    // Assert that the pooled connection is valid.
    assertTrue(testConnection.isValid(IS_VALID_TIMEOUT));

    // Start crashing all other instances
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_3);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_4);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_5);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_1);

    // Starting crashing current connection - Instance2
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_2);

    // Assert exception thrown with SQLState code 08001 (SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE)
    assertFirstQueryThrows(testConnection, "08001");

    // Assert that the pooled connection is invalid.
    assertFalse(testConnection.isValid(IS_VALID_TIMEOUT));
  }

  /** Reader connection failover within the connection pool. */
  @Test
  public void test4_2_pooledReaderConnection_basicfailover()
      throws SQLException, InterruptedException {
    testConnection = createPooledConnectionWithInstanceId(INSTANCE_ID_2);
    testConnection.setReadOnly(true);

    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_2);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are now connected to a new reader instance.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(currentConnectionId));
    assertNotEquals(currentConnectionId, INSTANCE_ID_2);

    // Assert that the pooled connection is valid.
    assertTrue(testConnection.isValid(IS_VALID_TIMEOUT));

    // Start crashing all other instances
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_1);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_3);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_4);
    startCrashingInstanceAndWaitUntilDown(INSTANCE_ID_5);

    // Assert exception thrown with SQLState code 08001 (SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE)
    assertFirstQueryThrows(testConnection, "08001");

    // Assert that the pooled connection is invalid.
    assertFalse(testConnection.isValid(IS_VALID_TIMEOUT));
  }

  @Test
  public void test5_1_takeOverConnectionProperties() throws SQLException, InterruptedException {
    final String initalWriterId = getDBClusterWriterInstanceId();
    assertEquals(INSTANCE_ID_1, initalWriterId);

    final Properties props = new Properties();
    props.setProperty("user", TEST_USERNAME);
    props.setProperty("password", TEST_PASSWORD);
    props.setProperty("allowMultiQueries", "true");
    props.setProperty("gatherPerfMetrics", "true");

    testConnection =
        DriverManager.getConnection(
            DB_CONN_STR_PREFIX + initalWriterId + DB_CONN_STR_SUFFIX, props);

    // Verify that connection accepts multi-statement sql
    final Statement myStmt = testConnection.createStatement();
    myStmt.executeQuery("select @@aurora_server_id; select 1; select 2;");

    // Crash Instance1 and nominate a new writer
    failoverClusterAndWaitUntilWriterChanged(initalWriterId);

    assertFirstQueryThrows(testConnection, "08S02");

    // Verify that connection still accepts multi-statement sql
    final Statement myStmt1 = testConnection.createStatement();
    myStmt1.executeQuery("select @@aurora_server_id; select 1; select 2;");
  }

  /* Helper functions. */

  private DBCluster getDBCluster(String dbClusterIdentifier) {
    DescribeDBClustersRequest dbClustersRequest =
        new DescribeDBClustersRequest().withDBClusterIdentifier(dbClusterIdentifier);
    DescribeDBClustersResult dbClustersResult = rdsClient.describeDBClusters(dbClustersRequest);
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
    DBCluster dbCluster = getDBCluster(TEST_DB_CLUSTER_IDENTIFIER);
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

  private List<String> getDBClusterReaderInstanceIds() {
    List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList().stream()
            .filter(dbClusterMember -> !dbClusterMember.isClusterWriter())
            .collect(Collectors.toList());
    return matchedMemberList.stream()
        .map(DBClusterMember::getDBInstanceIdentifier)
        .collect(Collectors.toList());
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
        Thread.sleep(3000);
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
        Thread.sleep(3000);
      }
    }

    this.log.logDebug("Cluster failover request successful.");
  }

  private void waitUntilWriterInstanceChanged(String initialWriterInstanceId)
      throws InterruptedException {
    this.log.logDebug("Wait until the writer is not " + initialWriterInstanceId + " anymore.");
    String nextClusterWriterId = getDBClusterWriterInstanceId();
    while (initialWriterInstanceId.equals(nextClusterWriterId)) {
      Thread.sleep(3000);
      // Calling the RDS API to get writer Id.
      nextClusterWriterId = getDBClusterWriterInstanceId();
    }

    this.log.logDebug("Writer is now " + nextClusterWriterId);
  }

  private void waitUntilClusterHasRightState() throws InterruptedException {
    this.log.logDebug("Wait until cluster is in available state.");
    String status = getDBCluster(TEST_DB_CLUSTER_IDENTIFIER).getStatus();
    while (!"available".equalsIgnoreCase(status)) {
      Thread.sleep(3000);
      status = getDBCluster(TEST_DB_CLUSTER_IDENTIFIER).getStatus();
    }
    this.log.logDebug("Cluster is available.");
  }

  private Connection createConnectionToReadonlyClusterEndpoint() throws SQLException {
    return DriverManager.getConnection(
        DB_CONN_STR_PREFIX + TEST_DB_CLUSTER_IDENTIFIER + DB_READONLY_CONN_STR_SUFFIX,
        TEST_USERNAME,
        TEST_PASSWORD);
  }

  private Connection createConnectionToInstanceWithId(String instanceID) throws SQLException {
    return DriverManager.getConnection(
        DB_CONN_STR_PREFIX + instanceID + DB_CONN_STR_SUFFIX, TEST_USERNAME, TEST_PASSWORD);
  }

  private Connection createConnectionWithProxyDisabled(String instanceID) throws SQLException {
    return DriverManager.getConnection(
        DB_CONN_STR_PREFIX + instanceID + DB_CONN_STR_SUFFIX + "?enableClusterAwareFailover=false",
        TEST_USERNAME,
        TEST_PASSWORD);
  }

  private Connection createPooledConnectionWithInstanceId(String instanceID) throws SQLException {
    BasicDataSource ds = new BasicDataSource();
    ds.setUrl(DB_CONN_STR_PREFIX + instanceID + DB_CONN_STR_SUFFIX);
    ds.setUsername(TEST_USERNAME);
    ds.setPassword(TEST_PASSWORD);
    ds.setMinIdle(CP_MIN_IDLE);
    ds.setMaxIdle(CP_MAX_IDLE);
    ds.setMaxOpenPreparedStatements(CP_MAX_OPEN_PREPARED_STATEMENTS);

    return ds.getConnection();
  }

  private Connection connectToReaderInstance(String readerInstanceId) throws SQLException {
    final Connection testConnection = createConnectionToInstanceWithId(readerInstanceId);
    testConnection.setReadOnly(true);
    assertTrue(isDBInstanceReader(queryInstanceId(testConnection)));
    return testConnection;
  }

  private Connection connectToWriterInstance(String writerInstanceId) throws SQLException {
    final Connection testConnection = createConnectionToInstanceWithId(writerInstanceId);
    assertTrue(isDBInstanceWriter(queryInstanceId(testConnection)));
    return testConnection;
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
   * Block until the specified instance is inaccessible.
   * */
  public void waitUntilInstanceIsDown(String instanceId) throws InterruptedException {
    this.log.logDebug("Wait until " + instanceId + " is down.");
    while (true) {
      try (Connection conn = createConnectionWithProxyDisabled(instanceId)) {
        // Continue waiting until instance is down.
      } catch (SQLException e) {
        break;
      }
      Thread.sleep(3000);
    }
    this.log.logDebug(instanceId + " is down.");
  }

  /**
   * Block until the specified instance is accessible again.
   * */
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
  private void resetCluster() throws InterruptedException, SQLException {
    this.log.logDebug("Resetting cluster.");
    waitUntilFirstInstanceIsWriter();
    waitUntilInstanceIsUp(INSTANCE_ID_1);
    waitUntilInstanceIsUp(INSTANCE_ID_2);
    waitUntilInstanceIsUp(INSTANCE_ID_3);
    waitUntilInstanceIsUp(INSTANCE_ID_4);
    waitUntilInstanceIsUp(INSTANCE_ID_5);
  }

  @AfterEach
  private void reviveInstancesAndCloseTestConnection() throws SQLException {
    reviveAllInstances();
    testConnection.close();
  }

  private void reviveAllInstances() {
    this.log.logDebug("Revive all crashed instances in the test and wait until they are up.");
    instanceCrasherMap.forEach(
        (instanceId, instanceCrasher) -> {
          try {
            stopCrashingInstanceAndWaitUntilUp(instanceId);
          } catch (InterruptedException ex) {
            this.log.logDebug("Exception occured while trying to stop crashing an instance.");
          }
        });
  }

  private void stopCrashingInstanceAndWaitUntilUp(String instanceId)
      throws InterruptedException {
    this.log.logDebug("Stop crashing " + instanceId + ".");
    CrashInstanceRunnable instanceCrasher = instanceCrasherMap.get(instanceId);
    instanceCrasher.stopCrashingInstance();
    waitUntilInstanceIsUp(instanceId);
  }

  /**
   * Start crashing the specified instance and wait until its inaccessible.
   * */
  private void startCrashingInstanceAndWaitUntilDown(String instanceId)
      throws InterruptedException {
    this.log.logDebug("Start crashing " + instanceId + ".");
    CrashInstanceRunnable instanceCrasher = instanceCrasherMap.get(instanceId);
    instanceCrasher.startCrashingInstance();
    Thread instanceCrasherThread = new Thread(instanceCrasher);
    instanceCrasherThread.start();
    waitUntilInstanceIsDown(instanceId);
  }

  /**
   * Runnable class implementation that is used to crash an instance.
   * */
  public class CrashInstanceRunnable implements Runnable {
    static final String CRASH_QUERY = "ALTER SYSTEM CRASH INSTANCE";
    private final String instanceId;
    private boolean keepCrashingInstance = false;

    CrashInstanceRunnable(String instanceId) {
      System.out.println("create runnable for " + instanceId);
      this.instanceId = instanceId;
    }

    public String getInstanceId() {
      return this.instanceId;
    }

    public synchronized void stopCrashingInstance() {
      this.keepCrashingInstance = false;
    }

    public synchronized void startCrashingInstance() {
      this.keepCrashingInstance = true;
    }

    @Override
    public void run() {
      while (keepCrashingInstance) {
        try (Connection conn = createConnectionWithProxyDisabled(instanceId);
             Statement myStmt = conn.createStatement()
        ) {
          myStmt.execute(CRASH_QUERY);
        } catch (SQLException e) {
          // Do nothing and keep creating connection to crash instance.
        }
      }
      // Run the garbage collector in the Java Virtual Machine to abandon thread.
      System.gc();
    }
  }
}
