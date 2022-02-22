/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package testsuite.integration.container;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ha.plugins.failover.IClusterAwareMetricsReporter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import eu.rekawek.toxiproxy.Proxy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class AuroraMysqlIntegrationTest extends AuroraMysqlIntegrationBaseTest {

  protected String currWriter;
  protected String currReader;

  @ParameterizedTest(name = "test_ConnectionString")
  @MethodSource("generateConnectionString")
  public void test_ConnectionString(String connStr, int port) throws SQLException {
    final Connection conn = connectToInstance(connStr, port);
    assertTrue(conn.isValid(5));
    conn.close();
  }

  private static Stream<Arguments> generateConnectionString() {
    return Stream.of(
        Arguments.of(MYSQL_INSTANCE_1_URL, MYSQL_PORT),
        Arguments.of(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT),
        Arguments.of(MYSQL_CLUSTER_URL, MYSQL_PORT),
        Arguments.of(MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT),
        Arguments.of(MYSQL_RO_CLUSTER_URL, MYSQL_PORT),
        Arguments.of(MYSQL_RO_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)
    );
  }

  @Test
  public void test_ValidateConnectionWhenNetworkDown() throws SQLException, IOException {
    final Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    assertTrue(conn.isValid(5));

    containerHelper.disableConnectivity(proxyInstance_1);

    assertFalse(conn.isValid(5));

    containerHelper.enableConnectivity(proxyInstance_1);

    conn.close();
  }

  @Test
  public void test_ConnectWhenNetworkDown() throws SQLException, IOException {
    containerHelper.disableConnectivity(proxyInstance_1);

    assertThrows(Exception.class, () -> {
      // expected to fail since communication is cut
      connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    });

    containerHelper.enableConnectivity(proxyInstance_1);

    final Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    conn.close();
  }

  @Test
  public void test_LostConnectionToWriter() throws SQLException, IOException {

    final String initialWriterId = instanceIDs[0];

    final Properties props = initDefaultProxiedProps();
    props.setProperty(PropertyKey.failoverTimeoutMs.getKeyName(), "10000");

    // Connect to cluster
    try (final Connection testConnection = connectToInstance(initialWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
      // Get writer
      currWriter = queryInstanceId(testConnection);

      // Put cluster & writer down
      final Proxy proxyInstance = proxyMap.get(currWriter);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currWriter));
      }
      containerHelper.disableConnectivity(proxyCluster);

      assertFirstQueryThrows(testConnection, "08001");

    } finally {
      final Proxy proxyInstance = proxyMap.get(currWriter);
      assertNotNull(proxyInstance, "Proxy isn't found for " + currWriter);
      containerHelper.enableConnectivity(proxyInstance);
      containerHelper.enableConnectivity(proxyCluster);
    }
  }

  @Test
  public void test_LostConnectionToAllReaders() throws SQLException {

    String currentWriterId = instanceIDs[0];
    String anyReaderId = instanceIDs[1];

    // Get Writer
    try (final Connection checkWriterConnection = connectToInstance(currentWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      currWriter = queryInstanceId(checkWriterConnection);
    }

    // Connect to cluster
    try (final Connection testConnection = connectToInstance(anyReaderId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = queryInstanceId(testConnection);
      assertNotEquals(currWriter, currReader);

      // Put all but writer down
      proxyMap.forEach((instance, proxy) -> {
        if (!instance.equalsIgnoreCase(currWriter)) {
          try {
            containerHelper.disableConnectivity(proxy);
          } catch (IOException e) {
            fail("Toxics were already set, should not happen");
          }
        }
      });

      assertFirstQueryThrows(testConnection, "08S02");

      final String newReader = queryInstanceId(testConnection);
      assertEquals(currWriter, newReader);
    } finally {
        proxyMap.forEach((instance, proxy) -> {
          assertNotNull(proxy, "Proxy isn't found for " + instance);
          containerHelper.enableConnectivity(proxy);
        });
    }
  }

  @Test
  public void test_LostConnectionToReaderInstance() throws SQLException, IOException {

    String currentWriterId = instanceIDs[0];
    String anyReaderId = instanceIDs[1];

    // Get Writer
    try (final Connection checkWriterConnection = connectToInstance(currentWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      currWriter = queryInstanceId(checkWriterConnection);
    } catch (SQLException e) {
      fail(e);
    }

    // Connect to instance
    try (final Connection testConnection = connectToInstance(anyReaderId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = queryInstanceId(testConnection);

      // Put down current reader
      final Proxy proxyInstance = proxyMap.get(currReader);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currReader));
      }

      assertFirstQueryThrows(testConnection, "08S02");

      final String newInstance = queryInstanceId(testConnection);
      assertEquals(currWriter, newInstance);
    } finally {
      final Proxy proxyInstance = proxyMap.get(currReader);
      assertNotNull(proxyInstance, "Proxy isn't found for " + currReader);
      containerHelper.enableConnectivity(proxyInstance);
    }
  }

  @Test
  public void test_LostConnectionReadOnly() throws SQLException, IOException {

    String currentWriterId = instanceIDs[0];
    String anyReaderId = instanceIDs[1];

    // Get Writer
    try (final Connection checkWriterConnection = connectToInstance(currentWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      currWriter = queryInstanceId(checkWriterConnection);
    }

    // Connect to instance
    try (final Connection testConnection = connectToInstance(anyReaderId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = queryInstanceId(testConnection);

      testConnection.setReadOnly(true);

      // Put down current reader
      final Proxy proxyInstance = proxyMap.get(currReader);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currReader));
      }

      assertFirstQueryThrows(testConnection, "08S02");

      final String newInstance = queryInstanceId(testConnection);
      assertNotEquals(currWriter, newInstance);
    } finally {
      final Proxy proxyInstance = proxyMap.get(currReader);
      assertNotNull(proxyInstance, "Proxy isn't found for " + currReader);
      containerHelper.enableConnectivity(proxyInstance);
    }
  }

  @Test
  void test_ValidInvalidValidConnections() throws SQLException {
    final Properties validProp = initDefaultProps();
    validProp.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    validProp.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    final Connection validConn = connectToInstance(MYSQL_INSTANCE_1_URL, MYSQL_PORT, validProp);
    validConn.close();

    final Properties invalidProp = initDefaultProps();
    invalidProp.setProperty(PropertyKey.USER.getKeyName(), "INVALID_" + TEST_USERNAME);
    invalidProp.setProperty(PropertyKey.PASSWORD.getKeyName(), "INVALID_" + TEST_PASSWORD);
    assertThrows(
            SQLException.class,
            () -> connectToInstance(MYSQL_INSTANCE_1_URL, MYSQL_PORT, invalidProp)
    );

    final Connection validConn2 = connectToInstance(MYSQL_INSTANCE_1_URL, MYSQL_PORT, validProp);
    validConn2.close();
  }

  /**
   * Attempt to connect using the wrong database username.
   */
  @Test
  public void test_AwsIam_WrongDatabaseUsername() {
    final Properties props = initAwsIamProps("WRONG_" + TEST_DB_USER + "_USER", TEST_PASSWORD);

    Assertions.assertThrows(
        SQLException.class,
        () -> DriverManager.getConnection(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL, props)
    );
  }

  /**
   * Attempt to connect without specifying a database username.
   */
  @Test
  public void test_AwsIam_NoDatabaseUsername() {
    final Properties props = initAwsIamProps("", TEST_PASSWORD);

    Assertions.assertThrows(
        SQLException.class,
        () -> DriverManager.getConnection(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL, props)
    );
  }

  /**
   * Attempt to connect using IP address instead of a hostname.
   */
  @Test
  public void test_AwsIam_UsingIPAddress() throws UnknownHostException {
    final Properties props = initAwsIamProps(TEST_DB_USER, TEST_PASSWORD);

    final String hostIp = hostToIP(MYSQL_CLUSTER_URL);
    Assertions.assertThrows(
        SQLException.class,
        () -> connectToInstance(hostIp, MYSQL_PORT, props)
    );
  }

  /**
   * Attempt to connect using valid database username/password & valid Amazon RDS hostname.
   */
  @Test
  public void test_AwsIam_ValidConnectionProperties() throws SQLException {
    final Properties props = initAwsIamProps(TEST_DB_USER, TEST_PASSWORD);

    final Connection conn = DriverManager.getConnection(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL, props);
    Assertions.assertDoesNotThrow(conn::close);
  }

  /**
   * Attempt to connect using valid database username, valid Amazon RDS hostname, but no password.
   */
  @Test
  public void test_AwsIam_ValidConnectionPropertiesNoPassword() throws SQLException {
    final Properties props = initAwsIamProps(TEST_DB_USER, "");
    final Connection conn = DriverManager.getConnection(DB_CONN_STR_PREFIX + MYSQL_CLUSTER_URL, props);
    Assertions.assertDoesNotThrow(conn::close);
  }

  /**
   * Attempts a valid connection followed by invalid connection
   * without the AWS protocol in Connection URL.
   */
  @Test
  void test_AwsIam_NoAwsProtocolConnection() throws SQLException {
    final String dbConn = "jdbc:mysql://" + MYSQL_CLUSTER_URL;
    final Properties validProp = initAwsIamProps(TEST_DB_USER, TEST_PASSWORD);
    final Properties invalidProp = initAwsIamProps("WRONG_" + TEST_DB_USER + "_USER", TEST_PASSWORD);

    final Connection conn = DriverManager.getConnection(dbConn, validProp);
    Assertions.assertDoesNotThrow(conn::close);
    Assertions.assertThrows(
        SQLException.class,
        () -> DriverManager.getConnection(dbConn, invalidProp)
    );
  }

  /**
   * Attempts a valid connection followed by an invalid connection
   * with Username in Connection URL.
   */
  @Test
  void test_AwsIam_UserInConnStr() throws SQLException {
    final String dbConn = "jdbc:mysql://" + MYSQL_CLUSTER_URL;
    final Properties awsIamProp = initDefaultProps();
    awsIamProp.remove(PropertyKey.USER.getKeyName());
    awsIamProp.setProperty(PropertyKey.useAwsIam.getKeyName(), Boolean.TRUE.toString());

    final Connection validConn = DriverManager.getConnection(dbConn + "?user=" + TEST_DB_USER, awsIamProp);
    Assertions.assertNotNull(validConn);
    Assertions.assertThrows(
        SQLException.class,
        () -> DriverManager.getConnection(dbConn + "?user=WRONG_" + TEST_DB_USER, awsIamProp)
    );
  }

  /**
   * Test collecting performance metrics for cluster
   */
  @Test
  public void test_CollectClusterMetrics() throws SQLException {

    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.gatherPerfMetrics.getKeyName(), "TRUE");
    props.setProperty(PropertyKey.gatherAdditionalMetricsOnInstance.getKeyName(), "TRUE");
    IClusterAwareMetricsReporter.resetMetrics();

    final Connection conn = connectToInstance(MYSQL_CLUSTER_URL, MYSQL_PORT, props);
    conn.close();

    final TestLogger logger = new TestLogger();
    final List<String> logs = logger.getLogs();

    // Not collecting for instances
    for (int i = 0; i < clusterSize; i++) {
      final String instanceUrl = instanceIDs[i] + DB_CONN_STR_SUFFIX;
      IClusterAwareMetricsReporter.reportMetrics(instanceUrl + ":" + MYSQL_PORT, logger);
      Assertions.assertEquals("** No metrics collected for '" + instanceUrl + ":" + MYSQL_PORT + "' **\n", logs.get(i));
    }

    logs.clear();
    IClusterAwareMetricsReporter.reportMetrics(MYSQL_CLUSTER_URL + ":" + MYSQL_PORT, logger);
    Assertions.assertTrue(logs.size() > 1);
  }

  /**
   * Test collecting performance metrics for instances as well
   */
  @Test
  public void test_CollectInstanceMetrics() throws SQLException {
    String anyReaderId = instanceIDs[1];

    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.gatherPerfMetrics.getKeyName(), "TRUE");
    props.setProperty(PropertyKey.gatherAdditionalMetricsOnInstance.getKeyName(), "TRUE");
    IClusterAwareMetricsReporter.resetMetrics();

    final Connection conn = connectToInstance(anyReaderId + DB_CONN_STR_SUFFIX, MYSQL_PORT, props);
    conn.close();

    final TestLogger logger = new TestLogger();
    final List<String> logs = logger.getLogs();

    IClusterAwareMetricsReporter.reportMetrics(anyReaderId + DB_CONN_STR_SUFFIX + ":" + MYSQL_PORT, logger, true);
    Assertions.assertTrue(logs.size() > 1);
  }

  /** Current writer dies, no available reader instance, connection fails. */
  @Test
  public void test_writerConnectionFailsDueToNoReader()
      throws SQLException, IOException {

    final String currentWriterId = instanceIDs[0];

    Properties props = initDefaultProxiedProps();
    props.setProperty(PropertyKey.failoverTimeoutMs.getKeyName(), "10000");
    try (Connection conn = connectToInstance(currentWriterId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
      // Put all but writer down first
      proxyMap.forEach((instance, proxy) -> {
        if (!instance.equalsIgnoreCase(currentWriterId)) {
          try {
            containerHelper.disableConnectivity(proxy);
          } catch (IOException e) {
            fail("Toxics were already set, should not happen");
          }
        }
      });

      // Crash the writer now
      final Proxy proxyInstance = proxyMap.get(currentWriterId);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currentWriterId));
      }

      // All instances should be down, assert exception thrown with SQLState code 08001
      // (SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE)
      assertFirstQueryThrows(conn, "08001");
    } finally {
      proxyMap.forEach((instance, proxy) -> {
        assertNotNull(proxy, "Proxy isn't found for " + instance);
        containerHelper.enableConnectivity(proxy);
      });
    }
  }

  /**
   * Current reader dies, after failing to connect to several reader instances, failover to another
   * reader.
   */
  @Test
  public void test_failFromReaderToReaderWithSomeReadersAreDown()
      throws SQLException, IOException {
    assertTrue(clusterSize >= 3, "Minimal cluster configuration: 1 writer + 2 readers");
    final String readerNode = instanceIDs[1];

    Properties props = initDefaultProxiedProps();
    try (Connection conn = connectToInstance(readerNode + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
      // First kill all reader instances except one
      for (int i = 1; i < clusterSize - 1; i++) {
        final String instanceId = instanceIDs[i];
        final Proxy proxyInstance = proxyMap.get(instanceId);
        if (proxyInstance != null) {
          containerHelper.disableConnectivity(proxyInstance);
        } else {
          fail(String.format("%s does not have a proxy setup.", instanceId));
        }
      }

      assertFirstQueryThrows(conn, "08S02");

      // Assert that we failed over to the only remaining reader instance (Instance5) OR Writer
      // instance (Instance1).
      final String currentConnectionId = queryInstanceId(conn);
      assertTrue(
          currentConnectionId.equals(instanceIDs[clusterSize - 1]) // Last reader
              || currentConnectionId.equals(instanceIDs[0])); // Writer
    }
  }

  /**
   * Current reader dies, failover to another reader repeat to loop through instances in the cluster
   * testing ability to revive previously down reader instance.
   */
  @Test
  public void test_failoverBackToThePreviouslyDownReader()
      throws Exception {

    assertTrue(clusterSize >= 5, "Minimal cluster configuration: 1 writer + 4 readers");

    final String writerInstanceId = instanceIDs[0];
    final String firstReaderInstanceId = instanceIDs[1];

    // Connect to reader (Instance2).
    Properties props = initDefaultProxiedProps();
    try (Connection conn = connectToInstance(firstReaderInstanceId + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
      conn.setReadOnly(true);

      // Start crashing reader (Instance2).
      Proxy proxyInstance = proxyMap.get(firstReaderInstanceId);
      containerHelper.disableConnectivity(proxyInstance);

      assertFirstQueryThrows(conn, "08S02");

      // Assert that we are connected to another reader instance.
      final String secondReaderInstanceId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(secondReaderInstanceId));
      assertNotEquals(firstReaderInstanceId, secondReaderInstanceId);

      // Crash the second reader instance.
      proxyInstance = proxyMap.get(secondReaderInstanceId);
      containerHelper.disableConnectivity(proxyInstance);

      assertFirstQueryThrows(conn, "08S02");

      // Assert that we are connected to the third reader instance.
      final String thirdReaderInstanceId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(thirdReaderInstanceId));
      assertNotEquals(firstReaderInstanceId, thirdReaderInstanceId);
      assertNotEquals(secondReaderInstanceId, thirdReaderInstanceId);

      // Grab the id of the fourth reader instance.
      final HashSet<String> readerInstanceIds = new HashSet<>(Arrays.asList(instanceIDs));
      readerInstanceIds.remove(writerInstanceId); // Writer
      readerInstanceIds.remove(firstReaderInstanceId);
      readerInstanceIds.remove(secondReaderInstanceId);
      readerInstanceIds.remove(thirdReaderInstanceId);

      final String fourthInstanceId = readerInstanceIds.stream().findFirst().orElseThrow(() -> new Exception("Empty instance Id"));

      // Crash the fourth reader instance.
      proxyInstance = proxyMap.get(fourthInstanceId);
      containerHelper.disableConnectivity(proxyInstance);

      // Stop crashing the first and second.
      proxyInstance = proxyMap.get(firstReaderInstanceId);
      containerHelper.enableConnectivity(proxyInstance);

      proxyInstance = proxyMap.get(secondReaderInstanceId);
      containerHelper.enableConnectivity(proxyInstance);

      final String currentInstanceId = queryInstanceId(conn);
      assertEquals(thirdReaderInstanceId, currentInstanceId);

      // Start crashing the third instance.
      proxyInstance = proxyMap.get(thirdReaderInstanceId);
      containerHelper.disableConnectivity(proxyInstance);

      assertFirstQueryThrows(conn, "08S02");

      final String lastInstanceId = queryInstanceId(conn);

      assertTrue(
          firstReaderInstanceId.equals(lastInstanceId)
              || secondReaderInstanceId.equals(lastInstanceId));
    }
  }

  /** Connect to a readonly cluster endpoint and ensure that we are doing a reader failover. */
  @Test
  public void test_ClusterEndpointReadOnlyFailover() throws SQLException, IOException {
    try (final Connection conn = connectToInstance(MYSQL_RO_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      final String initialConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(initialConnectionId));

      final Proxy instanceProxy = proxyMap.get(initialConnectionId);
      containerHelper.disableConnectivity(instanceProxy);
      containerHelper.disableConnectivity(proxyReadOnlyCluster);

      assertFirstQueryThrows(conn, "08S02");

      final String newConnectionId = queryInstanceId(conn);
      assertTrue(isDBInstanceReader(newConnectionId));
      assertNotEquals(newConnectionId, initialConnectionId);
    }
  }

}
