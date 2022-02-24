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
import com.mysql.cj.log.Log;
import com.mysql.cj.log.LogFactory;
import com.mysql.cj.log.StandardLogger;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import eu.rekawek.toxiproxy.Proxy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class HikariCPIntegrationTest extends AuroraMysqlIntegrationBaseTest {

  private static Log log = null;
  private static final String URL_SUFFIX = PROXIED_DOMAIN_NAME_SUFFIX + ":" + MYSQL_PROXY_PORT;
  private static HikariDataSource data_source = null;
  private final List<String> clusterTopology = fetchTopology();

  private List<String> fetchTopology() {
    try {
      List<String> topology = getTopologyEndpoints();
      // topology should contain a writer and at least one reader
      if (topology == null || topology.size() < 2) {
        fail("Topology does not contain the required instances");
      }
      return topology;
    } catch (SQLException e) {
      fail("Couldn't fetch cluster topology");
    }

    return null;
  }

  @BeforeAll
  static void setup() throws ClassNotFoundException {
    Class.forName("software.aws.rds.jdbc.mysql.Driver");
    log = LogFactory.getLogger(StandardLogger.class.getName(), Log.LOGGER_INSTANCE_NAME);

    System.setProperty("com.zaxxer.hikari.blockUntilFilled", "true");
  }

  @AfterEach
  public void teardown() {
    data_source.close();
  }

  @BeforeEach
  public void setUpTest() throws SQLException {
    String writerEndpoint = clusterTopology.get(0);

    String jdbcUrl = DB_CONN_STR_PREFIX + writerEndpoint + URL_SUFFIX;
    log.logDebug("Writer endpoint: " + jdbcUrl);

    final HikariConfig config = new HikariConfig();

    config.setJdbcUrl(jdbcUrl);
    config.setUsername(TEST_USERNAME);
    config.setPassword(TEST_PASSWORD);
    config.setMaximumPoolSize(3);
    config.setReadOnly(true);
    config.setExceptionOverrideClassName(HikariCPSQLException.class.getName());
    config.setInitializationFailTimeout(75000);
    config.setConnectionTimeout(1000);
    config.addDataSourceProperty(PropertyKey.failoverTimeoutMs.getKeyName(), "5000");
    config.addDataSourceProperty(PropertyKey.failoverReaderConnectTimeoutMs.getKeyName(), "1000");
    config.addDataSourceProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), PROXIED_CLUSTER_TEMPLATE);
    config.addDataSourceProperty(PropertyKey.failureDetectionTime.getKeyName(), "3000");
    config.addDataSourceProperty(PropertyKey.failureDetectionInterval.getKeyName(), "1500");

    data_source = new HikariDataSource(config);

    final HikariPoolMXBean hikariPoolMXBean = data_source.getHikariPoolMXBean();

    log.logDebug("Starting idle connections: " + hikariPoolMXBean.getIdleConnections());
    log.logDebug("Starting active connections: " + hikariPoolMXBean.getActiveConnections());
    log.logDebug("Starting total connections: " + hikariPoolMXBean.getTotalConnections());
  }

  /**
   * After getting successful connections from the pool, the cluster becomes unavailable
   */
  @Test
  public void test_1_1_hikariCP_lost_connection() throws SQLException {
    try (Connection conn = data_source.getConnection()) {
      assertTrue(conn.isValid(5));

      putDownAllInstances(true);

      final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      assertEquals("08001", exception.getSQLState());
      assertFalse(conn.isValid(5));
    }

    assertThrows(SQLTransientConnectionException.class, () -> data_source.getConnection());
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance. A connection is then retrieved to check that connections
   * to failed instances are not returned
   */
  @Test
  public void test_1_2_hikariCP_get_dead_connection() throws SQLException {
    putDownAllInstances(false);

    String writer = clusterTopology.get(0);
    String reader = clusterTopology.get(1);
    String writerIdentifier = writer.split("\\.")[0];
    String readerIdentifier = reader.split("\\.")[0];
    log.logDebug("Instance to connect to: " + writerIdentifier);
    log.logDebug("Instance to fail over to: " + readerIdentifier);

    bringUpInstance(writerIdentifier);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn = data_source.getConnection()) {
      assertTrue(conn.isValid(5));
      String currentInstance = queryInstanceId(conn);
      assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
      bringUpInstance(readerIdentifier);
      putDownInstance(currentInstance);

      final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      assertEquals("08S02", exception.getSQLState());

      // Check the connection is valid after connecting to a different instance
      assertTrue(conn.isValid(5));
      currentInstance = queryInstanceId(conn);
      log.logDebug("Connected to instance: " + currentInstance);
      assertTrue(currentInstance.equalsIgnoreCase(readerIdentifier));

      // Try to get a new connection to the failed instance, which times out
      assertThrows(SQLTransientConnectionException.class, () -> data_source.getConnection());
    }
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance through the Enhanced Failure Monitor
   */
  @Test
  public void test_2_1_hikariCP_efm_failover() throws SQLException {
    putDownAllInstances(false);

    String writer = clusterTopology.get(0);
    String reader = clusterTopology.get(1);
    String writerIdentifier = writer.split("\\.")[0];
    String readerIdentifier = reader.split("\\.")[0];
    log.logDebug("Instance to connect to: " + writerIdentifier);
    log.logDebug("Instance to fail over to: " + readerIdentifier);

    bringUpInstance(writerIdentifier);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn = data_source.getConnection()) {
      assertTrue(conn.isValid(5));
      String currentInstance = queryInstanceId(conn);
      assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
      log.logDebug("Connected to instance: " + currentInstance);

      bringUpInstance(readerIdentifier);
      putDownInstance(writerIdentifier);

      final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(conn));
      assertEquals("08S02", exception.getSQLState());

      // Check the connection is valid after connecting to a different instance
      assertTrue(conn.isValid(5));
      currentInstance = queryInstanceId(conn);
      log.logDebug("Connected to instance: " + currentInstance);
      assertTrue(currentInstance.equalsIgnoreCase(readerIdentifier));
    }
  }

  private void putDownInstance(String targetInstance) {
    Proxy toPutDown = proxyMap.get(targetInstance);
    disableInstanceConnection(toPutDown);
    log.logDebug("Took down " + targetInstance);
  }

  private void putDownAllInstances(Boolean putDownClusters) {
    log.logDebug("Putting down all instances");
    proxyMap.forEach((instance, proxy) -> {
      if (putDownClusters || (proxy != proxyCluster && proxy != proxyReadOnlyCluster)) {
        disableInstanceConnection(proxy);
      }
    });
  }

  private void disableInstanceConnection(Proxy proxy) {
    try {
      containerHelper.disableConnectivity(proxy);
    } catch (IOException e) {
      fail("Couldn't disable proxy connectivity");
    }
  }

  private void bringUpInstance(String targetInstance) {
    Proxy toBringUp = proxyMap.get(targetInstance);
    containerHelper.enableConnectivity(toBringUp);
    log.logDebug("Brought up " + targetInstance);
  }
}
