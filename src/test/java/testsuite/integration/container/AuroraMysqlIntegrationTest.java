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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import eu.rekawek.toxiproxy.Proxy;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class AuroraMysqlIntegrationTest extends AuroraMysqlIntegrationBaseTest {

  protected String currWriter;
  protected String currReader;

  @BeforeAll
  public static void setUp() throws IOException, SQLException {
    AuroraMysqlIntegrationBaseTest.setUp();
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
      final Connection tmp = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    });

    containerHelper.enableConnectivity(proxyInstance_1);

    final Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    conn.close();
  }

  @Test
  public void test_LostConnectionToWriter() throws SQLException, IOException {

    List<String> currentClusterTopology = getTopology();
    String currentWriterEndpoint = currentClusterTopology.stream().findFirst().orElse(null);
    assertNotNull(currentWriterEndpoint);

    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.failoverTimeoutMs.getKeyName(), "10000");

    // Connect to cluster
    try (final Connection testConnection = connectToInstance(currentWriterEndpoint + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
      // Get writer
      currWriter = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);

      // Put cluster & writer down
      final Proxy proxyInstance = proxyMap.get(currWriter);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currWriter));
      }
      containerHelper.disableConnectivity(proxyCluster);

      SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08001", exception.getSQLState());

    } finally {
      final Proxy proxyInstance = proxyMap.get(currWriter);
      assertNotNull(proxyInstance, "Proxy isn't found for " + currWriter);
      containerHelper.enableConnectivity(proxyInstance);
      containerHelper.enableConnectivity(proxyCluster);
    }
  }

  @Test
  public void test_LostConnectionToReader() throws SQLException, IOException {

    List<String> currentClusterTopology = getTopology();
    String currentWriterEndpoint = currentClusterTopology.stream().findFirst().orElse(null);
    String anyReaderEndpoint = currentClusterTopology.stream().filter((x) -> x != currentWriterEndpoint).findAny().orElse(null);
    assertNotNull(currentWriterEndpoint);
    assertNotNull(anyReaderEndpoint);

    // Connect to cluster
    try (final Connection testConnection = connectToInstance(anyReaderEndpoint + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);

      // Put cluster & reader down
      final Proxy proxyInstance = proxyMap.get(currReader);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currReader));
      }
      containerHelper.disableConnectivity(proxyReadOnlyCluster);

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());
    } finally {
      final Proxy proxyInstance = proxyMap.get(currReader);
      assertNotNull(proxyInstance, "Proxy isn't found for " + currReader);
      containerHelper.enableConnectivity(proxyInstance);
      containerHelper.enableConnectivity(proxyReadOnlyCluster);
    }
  }

  @Test
  public void test_LostConnectionToAllReaders() throws SQLException {

    List<String> currentClusterTopology = getTopology();
    String currentWriterEndpoint = currentClusterTopology.stream().findFirst().orElse(null);
    String anyReaderEndpoint = currentClusterTopology.stream().filter((x) -> x != currentWriterEndpoint).findAny().orElse(null);
    assertNotNull(currentWriterEndpoint);
    assertNotNull(anyReaderEndpoint);

    // Get Writer
    try (final Connection checkWriterConnection = connectToInstance(currentWriterEndpoint + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      currWriter = selectSingleRow(checkWriterConnection, QUERY_FOR_INSTANCE);
    }

    // Connect to cluster
    try (final Connection testConnection = connectToInstance(anyReaderEndpoint + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);
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

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());

      final String newReader = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);
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

    List<String> currentClusterTopology = getTopology();
    String currentWriterEndpoint = currentClusterTopology.stream().findFirst().orElse(null);
    String anyReaderEndpoint = currentClusterTopology.stream().filter((x) -> x != currentWriterEndpoint).findAny().orElse(null);
    assertNotNull(currentWriterEndpoint);
    assertNotNull(anyReaderEndpoint);

    // Get Writer
    try (final Connection checkWriterConnection = connectToInstance(currentWriterEndpoint + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      currWriter = selectSingleRow(checkWriterConnection, QUERY_FOR_INSTANCE);
    } catch (SQLException e) {
      fail(e);
    }

    // Connect to instance
    try (final Connection testConnection = connectToInstance(anyReaderEndpoint + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);

      // Put down current reader
      final Proxy proxyInstance = proxyMap.get(currReader);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currReader));
      }

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());

      final String newInstance = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);
      assertEquals(currWriter, newInstance);
    } finally {
      final Proxy proxyInstance = proxyMap.get(currReader);
      assertNotNull(proxyInstance, "Proxy isn't found for " + currReader);
      containerHelper.enableConnectivity(proxyInstance);
    }
  }

  @Test
  public void test_LostConnectionReadOnly() throws SQLException, IOException {

    List<String> currentClusterTopology = getTopology();
    String currentWriterEndpoint = currentClusterTopology.stream().findFirst().orElse(null);
    String anyReaderEndpoint = currentClusterTopology.stream().filter((x) -> x != currentWriterEndpoint).findAny().orElse(null);
    assertNotNull(currentWriterEndpoint);
    assertNotNull(anyReaderEndpoint);

    // Get Writer
    try (final Connection checkWriterConnection = connectToInstance(currentWriterEndpoint + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      currWriter = selectSingleRow(checkWriterConnection, QUERY_FOR_INSTANCE);
    }

    // Connect to instance
    try (final Connection testConnection = connectToInstance(anyReaderEndpoint + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = selectSingleRow(testConnection, QUERY_FOR_INSTANCE);

      testConnection.setReadOnly(true);

      // Put down current reader
      final Proxy proxyInstance = proxyMap.get(currReader);
      if (proxyInstance != null) {
        containerHelper.disableConnectivity(proxyInstance);
      } else {
        fail(String.format("%s does not have a proxy setup.", currReader));
      }

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());

      final String newInstance = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
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
}
