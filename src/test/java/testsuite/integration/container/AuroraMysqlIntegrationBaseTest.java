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
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import software.aws.rds.jdbc.mysql.Driver;
import testsuite.integration.utility.ContainerHelper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class AuroraMysqlIntegrationBaseTest {

  protected static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  protected static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");
  protected static final String QUERY_FOR_INSTANCE = "SELECT @@aurora_server_id";

  protected static final String PROXIED_DOMAIN_NAME_SUFFIX = System.getenv("PROXIED_DOMAIN_NAME_SUFFIX");
  protected static final String PROXIED_CLUSTER_TEMPLATE = System.getenv("PROXIED_CLUSTER_TEMPLATE");
  protected static final String DB_CONN_STR_SUFFIX = System.getenv("DB_CONN_STR_SUFFIX");

  static final String MYSQL_INSTANCE_1_URL = System.getenv("MYSQL_INSTANCE_1_URL");
  static final String MYSQL_INSTANCE_2_URL = System.getenv("MYSQL_INSTANCE_2_URL");
  static final String MYSQL_INSTANCE_3_URL = System.getenv("MYSQL_INSTANCE_3_URL");
  static final String MYSQL_INSTANCE_4_URL = System.getenv("MYSQL_INSTANCE_4_URL");
  static final String MYSQL_INSTANCE_5_URL = System.getenv("MYSQL_INSTANCE_5_URL");
  static final String MYSQL_CLUSTER_URL = System.getenv("DB_CLUSTER_CONN");
  static final String MYSQL_RO_CLUSTER_URL = System.getenv("DB_RO_CLUSTER_CONN");

  protected static final int MYSQL_PORT = Integer.parseInt(System.getenv("MYSQL_PORT"));
  protected static final int MYSQL_PROXY_PORT = Integer.parseInt(System.getenv("MYSQL_PROXY_PORT"));

  protected static final String TOXIPROXY_INSTANCE_1_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_1_NETWORK_ALIAS");
  protected static final String TOXIPROXY_INSTANCE_2_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_2_NETWORK_ALIAS");
  protected static final String TOXIPROXY_INSTANCE_3_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_3_NETWORK_ALIAS");
  protected static final String TOXIPROXY_INSTANCE_4_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_4_NETWORK_ALIAS");
  protected static final String TOXIPROXY_INSTANCE_5_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_5_NETWORK_ALIAS");
  protected static final String TOXIPROXY_CLUSTER_NETWORK_ALIAS = System.getenv("TOXIPROXY_CLUSTER_NETWORK_ALIAS");
  protected static final String TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS = System.getenv("TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS");
  protected static final int TOXIPROXY_CONTROL_PORT = 8474;

  protected static ToxiproxyClient toxiproxyClientInstance_1;
  protected static ToxiproxyClient toxiproxyClientInstance_2;
  protected static ToxiproxyClient toxiproxyClientInstance_3;
  protected static ToxiproxyClient toxiproxyClientInstance_4;
  protected static ToxiproxyClient toxiproxyClientInstance_5;
  protected static ToxiproxyClient toxiproxyCluster;
  protected static ToxiproxyClient toxiproxyReadOnlyCluster;

  protected static Proxy proxyInstance_1;
  protected static Proxy proxyInstance_2;
  protected static Proxy proxyInstance_3;
  protected static Proxy proxyInstance_4;
  protected static Proxy proxyInstance_5;
  protected static Proxy proxyCluster;
  protected static Proxy proxyReadOnlyCluster;
  protected static final Map<String, Proxy> proxyMap = new HashMap<>();

  protected final ContainerHelper containerHelper = new ContainerHelper();

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

  protected static Proxy getProxy(ToxiproxyClient proxyClient, String host, int port) throws IOException {
    final String upstream = host + ":" + port;
    return proxyClient.getProxy(upstream);
  }

  @BeforeEach
  public void setUpEach() {
    proxyMap.forEach((instance, proxy) -> {
      containerHelper.enableConnectivity(proxy);
    });
  }

  protected String selectSingleRow(Connection connection, String sql) throws SQLException {
    try (final Statement myStmt = connection.createStatement();
         final ResultSet result = myStmt.executeQuery(sql)) {
      if (result.next()) {
        return result.getString(1);
      }
      return null;
    }
  }

  protected Properties initDefaultPropsNoTimeouts() {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), PROXIED_CLUSTER_TEMPLATE);
    props.setProperty(PropertyKey.tcpKeepAlive.getKeyName(), Boolean.FALSE.toString());

    // Uncomment the following line to ease debugging
    //props.setProperty(PropertyKey.logger.getKeyName(), "com.mysql.cj.log.StandardLogger");

    return props;
  }

  protected Properties initDefaultProps() {
    final Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "3000");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "3000");

    return props;
  }

  protected Connection connectToInstance(String instanceUrl, int port) throws SQLException {
    return connectToInstance(instanceUrl, port, initDefaultProps());
  }

  protected Connection connectToInstance(String instanceUrl, int port, Properties props) throws SQLException {
    String url = "jdbc:mysql:aws://" + instanceUrl + ":" + port;
    return DriverManager.getConnection(url, props);
  }

  // Return list of instance endpoints.
  // Writer instance goes first.
  protected List<String> getTopology() throws SQLException {
    final String dbConnHostBase =
            DB_CONN_STR_SUFFIX.startsWith(".")
                    ? DB_CONN_STR_SUFFIX.substring(1)
                    : DB_CONN_STR_SUFFIX;

    String url = "jdbc:mysql://" + MYSQL_INSTANCE_1_URL + ":" + MYSQL_PORT;
    return this.containerHelper.getAuroraClusterInstances(url, TEST_USERNAME, TEST_PASSWORD, dbConnHostBase);
  }
}
