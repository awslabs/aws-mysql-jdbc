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
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import software.aws.rds.jdbc.mysql.Driver;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class AuroraMySqlIntegrationTest {

  private static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  private static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");
  private static final String QUERY_FOR_INSTANCE = "SELECT @@aurora_server_id";

  private static final String PROXIED_DOMAIN_NAME_SUFFIX = System.getenv("PROXIED_DOMAIN_NAME_SUFFIX");
  private static final String PROXIED_CLUSTER_TEMPLATE = System.getenv("PROXIED_CLUSTER_TEMPLATE");
  private static final String PROXIED_ENDPOINT_PATTERN = PROXIED_CLUSTER_TEMPLATE.replace("?", "%s");

  private static final String MYSQL_INSTANCE_1_URL = System.getenv("MYSQL_INSTANCE_1_URL");
  private static final String MYSQL_INSTANCE_2_URL = System.getenv("MYSQL_INSTANCE_2_URL");
  private static final String MYSQL_INSTANCE_3_URL = System.getenv("MYSQL_INSTANCE_3_URL");
  private static final String MYSQL_INSTANCE_4_URL = System.getenv("MYSQL_INSTANCE_4_URL");
  private static final String MYSQL_INSTANCE_5_URL = System.getenv("MYSQL_INSTANCE_5_URL");
  private static final String MYSQL_CLUSTER_URL = System.getenv("DB_CLUSTER_CONN");
  private static final String MYSQL_RO_CLUSTER_URL = System.getenv("DB_RO_CLUSTER_CONN");

  private static final int MYSQL_PORT = Integer.parseInt(System.getenv("MYSQL_PORT"));
  private static final int MYSQL_PROXY_PORT = Integer.parseInt(System.getenv("MYSQL_PROXY_PORT"));

  private static final String TOXIPROXY_INSTANCE_1_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_1_NETWORK_ALIAS");
  private static final String TOXIPROXY_INSTANCE_2_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_2_NETWORK_ALIAS");
  private static final String TOXIPROXY_INSTANCE_3_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_3_NETWORK_ALIAS");
  private static final String TOXIPROXY_INSTANCE_4_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_4_NETWORK_ALIAS");
  private static final String TOXIPROXY_INSTANCE_5_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_5_NETWORK_ALIAS");
  private static final String TOXIPROXY_CLUSTER_NETWORK_ALIAS = System.getenv("TOXIPROXY_CLUSTER_NETWORK_ALIAS");
  private static final String TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS = System.getenv("TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS");
  private static final int TOXIPROXY_CONTROL_PORT = 8474;

  private static ToxiproxyClient toxiproxyClientInstance_1;
  private static ToxiproxyClient toxiproxyClientInstance_2;
  private static ToxiproxyClient toxiproxyClientInstance_3;
  private static ToxiproxyClient toxiproxyClientInstance_4;
  private static ToxiproxyClient toxiproxyClientInstance_5;
  private static ToxiproxyClient toxiproxyCluster;
  private static ToxiproxyClient toxiproxyReadOnlyCluster;

  private static Proxy proxyInstance_1;
  private static Proxy proxyInstance_2;
  private static Proxy proxyInstance_3;
  private static Proxy proxyInstance_4;
  private static Proxy proxyInstance_5;
  private static Proxy proxyCluster;
  private static Proxy proxyReadOnlyCluster;
  private static final Map<String, Proxy> proxyMap = new HashMap<>(7);

  private static final int REPEAT_TIMES = 5;
  private static final List<List<Integer>> dataList = new ArrayList<>();

  private static String currWriter;
  private static String currReader;

  @BeforeAll
  public static void setUp() throws IOException, SQLException {
    toxiproxyClientInstance_1 = new ToxiproxyClient(TOXIPROXY_INSTANCE_1_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxiproxyClientInstance_2 = new ToxiproxyClient(TOXIPROXY_INSTANCE_2_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxiproxyClientInstance_3 = new ToxiproxyClient(TOXIPROXY_INSTANCE_3_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxiproxyClientInstance_4 = new ToxiproxyClient(TOXIPROXY_INSTANCE_4_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxiproxyClientInstance_5 = new ToxiproxyClient(TOXIPROXY_INSTANCE_5_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxiproxyCluster = new ToxiproxyClient(TOXIPROXY_CLUSTER_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxiproxyReadOnlyCluster = new ToxiproxyClient(TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);

    proxyInstance_1 = getProxy(toxiproxyClientInstance_1, MYSQL_INSTANCE_1_URL, MYSQL_PORT);
    proxyInstance_2 = getProxy(toxiproxyClientInstance_2, MYSQL_INSTANCE_2_URL, MYSQL_PORT);
    proxyInstance_3 = getProxy(toxiproxyClientInstance_3, MYSQL_INSTANCE_3_URL, MYSQL_PORT);
    proxyInstance_4 = getProxy(toxiproxyClientInstance_4, MYSQL_INSTANCE_4_URL, MYSQL_PORT);
    proxyInstance_5 = getProxy(toxiproxyClientInstance_5, MYSQL_INSTANCE_5_URL, MYSQL_PORT);
    proxyCluster = getProxy(toxiproxyCluster, MYSQL_CLUSTER_URL, MYSQL_PORT);
    proxyReadOnlyCluster = getProxy(toxiproxyReadOnlyCluster, MYSQL_RO_CLUSTER_URL, MYSQL_PORT);

    proxyMap.put(MYSQL_INSTANCE_1_URL.substring(0, MYSQL_INSTANCE_1_URL.indexOf('.')), proxyInstance_1);
    proxyMap.put(MYSQL_INSTANCE_2_URL.substring(0, MYSQL_INSTANCE_2_URL.indexOf('.')), proxyInstance_2);
    proxyMap.put(MYSQL_INSTANCE_3_URL.substring(0, MYSQL_INSTANCE_3_URL.indexOf('.')), proxyInstance_3);
    proxyMap.put(MYSQL_INSTANCE_4_URL.substring(0, MYSQL_INSTANCE_4_URL.indexOf('.')), proxyInstance_4);
    proxyMap.put(MYSQL_INSTANCE_5_URL.substring(0, MYSQL_INSTANCE_5_URL.indexOf('.')), proxyInstance_5);
    proxyMap.put(MYSQL_CLUSTER_URL, proxyCluster);
    proxyMap.put(MYSQL_RO_CLUSTER_URL, proxyReadOnlyCluster);

    DriverManager.registerDriver(new Driver());
  }

  private static Proxy getProxy(ToxiproxyClient proxyClient, String host, int port) throws IOException {
    final String upstream = host + ":" + port;
    return proxyClient.getProxy(upstream);
  }

  @AfterAll
  public static void cleanUp() throws IOException {
    try (XSSFWorkbook workbook = new XSSFWorkbook()) {
      // Title
      final XSSFSheet sheet = workbook.createSheet("FailureDetectionResults");

      // Header
      Row row = sheet.createRow(0);
      Cell cell;
      final String[] headers = {"FailureDetectionGraceTime", "FailureDetectionInterval", "FailureDetectionCount", "SleepDelayMS", "MinFailureDetectionTime", "MaxFailureDetectionTime", "AvgFailureDetectionTime"};
      for (int i = 0; i < headers.length; i++) {
        cell = row.createCell(i);
        cell.setCellValue(headers[i]);
      }

      // Data
      for (int rows = 0; rows < dataList.size(); rows++) {
        List<Integer> dataPoints = dataList.get(rows);
        row = sheet.createRow(rows + 1);
        for (int data = 0; data < dataPoints.size(); data++) {
          cell = row.createCell(data);
          cell.setCellValue(dataPoints.get(data));
        }
      }

      // Write to file
      final File newExcelFile = new File("./build/reports/tests/FailureDetectionResults.xlsx");
      newExcelFile.createNewFile();
      try (FileOutputStream fileOut = new FileOutputStream(newExcelFile)) {
        workbook.write(fileOut);
      }
    }
  }

  @BeforeEach
  public void setUpEach() {
    proxyMap.forEach((instance, proxy) -> {
      try {
        enableConnectivity(proxy);
      } catch (IOException e) {
        // Ignore as toxics were not set
      }
    });
  }

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

    disableConnectivity(proxyInstance_1);

    assertFalse(conn.isValid(5));

    enableConnectivity(proxyInstance_1);

    conn.close();
  }

  @Test
  public void test_ConnectWhenNetworkDown() throws SQLException, IOException {
    disableConnectivity(proxyInstance_1);

    assertThrows(Exception.class, () -> {
      // expected to fail since communication is cut
      final Connection tmp = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    });

    enableConnectivity(proxyInstance_1);

    final Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    conn.close();
  }

  @ParameterizedTest
  @MethodSource("generateFailureDetectionTimeParams")
  public void test_FailureDetectionTime(int detectionTime, int detectionInterval, int detectionCount, int sleepDelayMS) {
    final Properties props = initDefaultProps();
    props.remove(PropertyKey.connectTimeout.getKeyName());
    props.remove(PropertyKey.socketTimeout.getKeyName());
    props.setProperty(PropertyKey.failureDetectionTime.getKeyName(), Integer.toString(detectionTime));
    props.setProperty(PropertyKey.failureDetectionInterval.getKeyName(), Integer.toString(detectionInterval));
    props.setProperty(PropertyKey.failureDetectionCount.getKeyName(), Integer.toString(detectionCount));
    props.setProperty(PropertyKey.enableClusterAwareFailover.getKeyName(), Boolean.FALSE.toString());

    final AtomicLong downtime = new AtomicLong();
    boolean hasFailConnect = false;
    final List<Integer> avg = new ArrayList<>(REPEAT_TIMES);
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    for (int i = 0; i < REPEAT_TIMES; i++) {
      downtime.set(0);
      // Thread to stop network
      final Thread thread = new Thread(() -> {
        try {
          Thread.sleep(sleepDelayMS);
          // Kill network
          disableConnectivity(proxyInstance_1);
          downtime.set(System.currentTimeMillis());
        } catch (IOException ioException) {
          fail("Toxics were already set, should not happen");
        } catch (InterruptedException interruptedException) {
          // Ignore, stop the thread
        }
      });
      try (final Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props);
          final Statement statement = conn.createStatement()) {
        hasFailConnect = false;

        // Execute long query
        try {
          final String QUERY = "select sleep(600)"; // 600s -> 10min
          thread.start();
          final ResultSet result = statement.executeQuery(QUERY);
          fail("Sleep query finished, should not be possible with network downed.");
        } catch (SQLException throwables) { // Catching executing query
          // Calculate and add detection time
          final int failureTime = (int) (System.currentTimeMillis() - downtime.get());
          avg.add(failureTime);
          min = Math.min(min, failureTime);
          max = Math.max(max, failureTime);
        }
      } catch (SQLException throwables) { // Catching Connection connect & Statement creations
        if (hasFailConnect) {
          // Already failed trying to connect twice in a row
          fail("Exception occurred twice when trying to connect or create statement");
        }
        i--; // Retry
        hasFailConnect = true;
      } finally {
        thread.interrupt(); // Ensure thread has stopped running
        try {
          enableConnectivity(proxyInstance_1);
        } catch (IOException e) {
          // Ignore as toxics were never set
        }
      }
    }

    final Integer[] arr = {detectionTime, detectionInterval, detectionCount, sleepDelayMS, min, max, (int)avg.stream().mapToInt(a -> a).summaryStatistics().getAverage()};
    final List<Integer> data = new ArrayList<>(Arrays.asList(arr));
    dataList.add(data);
  }

  private static Stream<Arguments> generateFailureDetectionTimeParams() {
    // detectionTime, detectionInterval, detectionCount, sleepDelayMS
    return Stream.of(
        Arguments.of(30000, 5000, 3, 5000), // Defaults
        Arguments.of(6000, 1000, 1, 5000) // Aggressive
    );
  }

  @Test
  public void test_LostConnectionToWriter() throws SQLException, IOException {
    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.failoverTimeoutMs.getKeyName(), "10000");

    // Connect to cluster
    try (final Connection testConnection = connectToInstance(MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
      // Get writer
      currWriter = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);

      // Put cluster & writer down
      final Proxy proxyInstance = proxyMap.get(currWriter);
      if (proxyInstance != null) {
        disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currWriter));
      }
      disableConnectivity(proxyCluster);

      SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08001", exception.getSQLState());

    } finally {
      try {
        final Proxy proxyInstance = proxyMap.get(currWriter);
        enableConnectivity(proxyInstance);
        enableConnectivity(proxyCluster);
      } catch (IOException e) {
        // Ignore as toxics were not set
      }
    }
  }

  @Test
  public void test_LostConnectionToReader() throws SQLException, IOException {
    // Connect to cluster
    try (final Connection testConnection = connectToInstance(MYSQL_RO_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);

      // Put cluster & reader down
      final Proxy proxyInstance = proxyMap.get(currReader);
      if (proxyInstance != null) {
        disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currReader));
      }
      disableConnectivity(proxyReadOnlyCluster);

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());
    } finally {
      try {
        final Proxy proxyInstance = proxyMap.get(currReader);
        enableConnectivity(proxyInstance);
        enableConnectivity(proxyReadOnlyCluster);
      } catch (IOException e) {
        // Ignore as toxics were not set
      }
    }
  }

  @Test
  public void test_LostConnectionToAllReaders() throws SQLException {
    // Get Writer
    try (final Connection checkWriterConnection = connectToInstance(MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      currWriter = selectSingleRow(checkWriterConnection, QUERY_FOR_INSTANCE);
    }

    // Connect to cluster
    try (final Connection testConnection = connectToInstance(MYSQL_RO_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);
      assertNotEquals(currWriter, currReader);

      // Put all but writer down
      proxyMap.forEach((instance, proxy) -> {
        if (!instance.equalsIgnoreCase(currWriter)) {
          try {
            disableConnectivity(proxy);
          } catch (IOException e) {
            fail("Toxics were already set, should not happen");
          }
        }
      });

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());

      final String newReader = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);
      assertEquals(currWriter, newReader);
    } finally {
        proxyMap.forEach((instance, proxy) -> {
          try {
            enableConnectivity(proxy);
          } catch (IOException e) {
            // Ignore as toxics were not set
          }
        });
    }
  }

  @Test
  public void test_LostConnectionToReaderInstance() throws SQLException, IOException {
    // Get Writer
    try (final Connection checkWriterConnection = connectToInstance(MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      currWriter = selectSingleRow(checkWriterConnection, QUERY_FOR_INSTANCE);
    } catch (SQLException e) {
      fail(e);
    }

    // Get instance name that is not WRITER
    final Set<String> readers = new HashSet<>(proxyMap.keySet());
    readers.remove(currWriter);
    readers.remove(MYSQL_CLUSTER_URL);
    readers.remove(MYSQL_RO_CLUSTER_URL);
    final String anyReader = readers.iterator().next();

    // Connect to instance
    try (final Connection testConnection = connectToInstance(String.format(PROXIED_ENDPOINT_PATTERN, anyReader), MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);
      assertEquals(anyReader, currReader);

      // Put down current reader
      final Proxy proxyInstance = proxyMap.get(currReader);
      if (proxyInstance != null) {
        disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currReader));
      }

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());

      final String newInstance = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);
      assertEquals(currWriter, newInstance);
    } finally {
        final Proxy proxyInstance = proxyMap.get(currReader);
        try {
          enableConnectivity(proxyInstance);
        } catch (IOException e) {
          // Ignore as toxics were not set
        }
    }
  }

  @Test
  public void test_LostConnectionReadOnly() throws SQLException, IOException {
    // Get Writer
    try (final Connection checkWriterConnection = connectToInstance(MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      currWriter = selectSingleRow(checkWriterConnection, QUERY_FOR_INSTANCE);
    }

    // Get instance name that is not WRITER
    final Set<String> readers = new HashSet<>(proxyMap.keySet());
    readers.remove(currWriter);
    readers.remove(MYSQL_CLUSTER_URL);
    readers.remove(MYSQL_RO_CLUSTER_URL);
    final String anyReader = readers.iterator().next();

    // Connect to instance
    try (final Connection testConnection = connectToInstance(String.format(PROXIED_ENDPOINT_PATTERN, anyReader), MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);
      assertEquals(anyReader, currReader);

      testConnection.setReadOnly(true);

      // Put down current reader
      final Proxy proxyInstance = proxyMap.get(currReader);
      if (proxyInstance != null) {
        disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currReader));
      }

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());

      final String newInstance = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
      assertNotEquals(currWriter, newInstance);
    } finally {
        final Proxy proxyInstance = proxyMap.get(currReader);
        try {
          enableConnectivity(proxyInstance);
        } catch (IOException e) {
          // Ignore as toxics were not set
        }
    }
  }

  private String selectSingleRow(Connection connection, String sql) throws SQLException {
    try (final Statement myStmt = connection.createStatement();
        final ResultSet result = myStmt.executeQuery(sql)) {
      if (result.next()) {
        return result.getString(1);
      }
      return null;
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
   * Stops all traffic to and from server
   */
  private static void disableConnectivity(Proxy proxy) throws IOException {
    proxy.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
    proxy.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql server
  }

  /**
   * Allow traffic to and from server
   */
  private static void enableConnectivity(Proxy proxy) throws IOException {
    proxy.toxics().get("DOWN-STREAM").remove();
    proxy.toxics().get("UP-STREAM").remove();
  }

  private static Properties initDefaultProps() {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "3000");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "3000");
    props.setProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), PROXIED_CLUSTER_TEMPLATE);
    props.setProperty(PropertyKey.tcpKeepAlive.getKeyName(), "FALSE");

    return props;
  }

  private static Connection connectToInstance(String instanceUrl, int port) throws SQLException {
    return connectToInstance(instanceUrl, port, initDefaultProps());
  }

  private static Connection connectToInstance(String instanceUrl, int port, Properties props) throws SQLException {
    return DriverManager.getConnection("jdbc:mysql:aws://" + instanceUrl + ":" + port, props);
  }
}
