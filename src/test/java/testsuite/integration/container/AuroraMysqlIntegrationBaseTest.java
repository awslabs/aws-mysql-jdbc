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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DBCluster;
import com.amazonaws.services.rds.model.DBClusterMember;
import com.amazonaws.services.rds.model.DescribeDBClustersRequest;
import com.amazonaws.services.rds.model.DescribeDBClustersResult;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.log.NullLogger;
import com.mysql.cj.util.StringUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import software.aws.rds.jdbc.mysql.Driver;
import testsuite.integration.utility.ContainerHelper;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AuroraMysqlIntegrationBaseTest {

  protected static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  protected static final String TEST_DB_USER = System.getenv("TEST_DB_USER");
  protected static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");
  protected static final String TEST_DB =
      !StringUtils.isNullOrEmpty(System.getenv("TEST_DB")) ? System.getenv("TEST_DB") : "test";

  protected static final String QUERY_FOR_INSTANCE = "SELECT @@aurora_server_id";

  protected static final String PROXIED_DOMAIN_NAME_SUFFIX = System.getenv(
      "PROXIED_DOMAIN_NAME_SUFFIX");
  protected static final String PROXIED_CLUSTER_TEMPLATE = System.getenv(
      "PROXIED_CLUSTER_TEMPLATE");

  protected static final String DB_CONN_STR_PREFIX = "jdbc:mysql:aws://";
  protected static final String DB_CONN_STR_SUFFIX = System.getenv("DB_CONN_STR_SUFFIX");

  static final String MYSQL_INSTANCE_1_URL = System.getenv("MYSQL_INSTANCE_1_URL");
  static final String MYSQL_INSTANCE_2_URL = System.getenv("MYSQL_INSTANCE_2_URL");
  static final String MYSQL_INSTANCE_3_URL = System.getenv("MYSQL_INSTANCE_3_URL");
  static final String MYSQL_INSTANCE_4_URL = System.getenv("MYSQL_INSTANCE_4_URL");
  static final String MYSQL_INSTANCE_5_URL = System.getenv("MYSQL_INSTANCE_5_URL");
  static final String MYSQL_CLUSTER_URL = System.getenv("DB_CLUSTER_CONN");
  static final String MYSQL_RO_CLUSTER_URL = System.getenv("DB_RO_CLUSTER_CONN");

  static final String DB_CLUSTER_IDENTIFIER =
      !StringUtils.isNullOrEmpty(MYSQL_CLUSTER_URL) ? MYSQL_CLUSTER_URL.substring(0, MYSQL_CLUSTER_URL.indexOf('.')) : null;
  protected static final String DB_REGION =
      !StringUtils.isNullOrEmpty(System.getenv("DB_REGION")) ? System.getenv("DB_REGION") : "us-east-1";

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

  protected String[] instanceIDs; // index 0 is always writer!
  protected int clusterSize = 0;

  protected final ContainerHelper containerHelper = new ContainerHelper();
  protected static final AmazonRDS rdsClient = AmazonRDSClientBuilder
      .standard()
      .withRegion(DB_REGION)
      .withCredentials(new DefaultAWSCredentialsProviderChain())
      .build();

  private static final int CP_MIN_IDLE = 5;
  private static final int CP_MAX_IDLE = 10;
  private static final int CP_MAX_OPEN_PREPARED_STATEMENTS = 100;
  private static final String NO_SUCH_CLUSTER_MEMBER =
      "Cannot find cluster member whose db instance identifier is ";
  private static final String NO_WRITER_AVAILABLE =
      "Cannot get the id of the writer Instance in the cluster.";
  protected static final int IS_VALID_TIMEOUT = 5;

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
  public void setUpEach() throws InterruptedException, SQLException {
    proxyMap.forEach((instance, proxy) -> containerHelper.enableConnectivity(proxy));

    // Always get the latest topology info with writer as first
    List<String> latestTopology = getTopologyIds();
    instanceIDs = new String[latestTopology.size()];
    latestTopology.toArray(instanceIDs);

    clusterSize = instanceIDs.length;
    assertTrue(clusterSize >= 2); // many tests assume that cluster contains at least a writer and a reader
    assertTrue(isDBInstanceWriter(instanceIDs[0]));
    makeSureInstancesUp(instanceIDs);
  }

  protected Properties initDefaultPropsNoTimeouts() {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
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

  protected Properties initDefaultProxiedProps() {
    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), PROXIED_CLUSTER_TEMPLATE);

    return props;
  }

  protected Properties initAwsIamProps(String user, String password) {
    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.useAwsIam.getKeyName(), Boolean.TRUE.toString());
    props.setProperty(PropertyKey.USER.getKeyName(), user);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), password);

    return props;
  }

  protected Properties initFailoverDisabledProps() {
    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.enableClusterAwareFailover.getKeyName(), "false");

    return props;
  }

  protected Connection connectToInstance(String instanceUrl, int port) throws SQLException {
    return connectToInstance(instanceUrl, port, initDefaultProxiedProps());
  }

  protected Connection connectToInstance(String instanceUrl, int port, Properties props) throws SQLException {
    final String url = DB_CONN_STR_PREFIX + instanceUrl + ":" + port + "/" + TEST_DB;
    return DriverManager.getConnection(url, props);
  }

  protected String hostToIP(String hostname) throws UnknownHostException {
    final InetAddress inet = InetAddress.getByName(hostname);
    return inet.getHostAddress();
  }

  // Return list of instance endpoints.
  // Writer instance goes first.
  protected List<String> getTopologyEndpoints() throws SQLException {
    final String dbConnHostBase =
            DB_CONN_STR_SUFFIX.startsWith(".")
                    ? DB_CONN_STR_SUFFIX.substring(1)
                    : DB_CONN_STR_SUFFIX;

    final String url = "jdbc:mysql://" + MYSQL_INSTANCE_1_URL + ":" + MYSQL_PORT;
    return this.containerHelper.getAuroraInstanceEndpoints(url, TEST_USERNAME, TEST_PASSWORD, dbConnHostBase);
  }

  // Return list of instance Ids.
  // Writer instance goes first.
  protected List<String> getTopologyIds() throws SQLException {
    final String url = "jdbc:mysql://" + MYSQL_INSTANCE_1_URL + ":" + MYSQL_PORT;
    return this.containerHelper.getAuroraInstanceIds(url, TEST_USERNAME, TEST_PASSWORD);
  }

  protected static class TestLogger extends NullLogger {

    private final List<String> logs = new ArrayList<>();

    public TestLogger() {
      super("");
    }

    public void logInfo(final Object msg) {
      logs.add(msg.toString());
    }

    public List<String> getLogs() {
      return logs;
    }
  }

  /* Helper functions. */
  protected String queryInstanceId(Connection connection) throws SQLException {
    try (final Statement myStmt = connection.createStatement()) {
      return executeInstanceIdQuery(myStmt);
    }
  }

  protected String executeInstanceIdQuery(Statement stmt) throws SQLException {
    try (final ResultSet rs = stmt.executeQuery(QUERY_FOR_INSTANCE)) {
      if (rs.next()) {
        return rs.getString("@@aurora_server_id");
      }
    }
    return null;
  }

  // Attempt to run a query after the instance is down.
  // This should initiate the driver failover, first query after a failover
  // should always throw with the expected error message.
  protected void assertFirstQueryThrows(Connection connection, String expectedSQLErrorCode) {
    final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(connection));
    assertEquals(expectedSQLErrorCode, exception.getSQLState());
  }

  protected void assertFirstQueryThrows(Statement stmt, String expectedSQLErrorCode) {
    final SQLException exception = assertThrows(SQLException.class, () -> executeInstanceIdQuery(stmt));
    assertEquals(expectedSQLErrorCode, exception.getSQLState());
  }

  protected Connection createPooledConnectionWithInstanceId(String instanceID) throws SQLException {
    final BasicDataSource ds = new BasicDataSource();
    ds.setUrl(DB_CONN_STR_PREFIX + instanceID + DB_CONN_STR_SUFFIX);
    ds.setUsername(TEST_USERNAME);
    ds.setPassword(TEST_PASSWORD);
    ds.setMinIdle(CP_MIN_IDLE);
    ds.setMaxIdle(CP_MAX_IDLE);
    ds.setMaxOpenPreparedStatements(CP_MAX_OPEN_PREPARED_STATEMENTS);

    return ds.getConnection();
  }

  protected DBCluster getDBCluster() {
    final DescribeDBClustersRequest dbClustersRequest =
        new DescribeDBClustersRequest().withDBClusterIdentifier(DB_CLUSTER_IDENTIFIER);
    final DescribeDBClustersResult dbClustersResult = rdsClient.describeDBClusters(dbClustersRequest);
    final List<DBCluster> dbClusterList = dbClustersResult.getDBClusters();
    return dbClusterList.get(0);
  }

  protected List<DBClusterMember> getDBClusterMemberList() {
    final DBCluster dbCluster = getDBCluster();
    return dbCluster.getDBClusterMembers();
  }

  protected DBClusterMember getMatchedDBClusterMember(String instanceId) {
    final List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList().stream()
            .filter(dbClusterMember -> dbClusterMember.getDBInstanceIdentifier().equals(instanceId))
            .collect(Collectors.toList());
    if (matchedMemberList.isEmpty()) {
      throw new RuntimeException(NO_SUCH_CLUSTER_MEMBER + instanceId);
    }
    return matchedMemberList.get(0);
  }

  protected String getDBClusterWriterInstanceId() {
    final List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList().stream()
            .filter(DBClusterMember::isClusterWriter)
            .collect(Collectors.toList());
    if (matchedMemberList.isEmpty()) {
      throw new RuntimeException(NO_WRITER_AVAILABLE);
    }
    // Should be only one writer at index 0.
    return matchedMemberList.get(0).getDBInstanceIdentifier();
  }

  protected Boolean isDBInstanceWriter(String instanceId) {
    return getMatchedDBClusterMember(instanceId).isClusterWriter();
  }

  protected Boolean isDBInstanceReader(String instanceId) {
    return !getMatchedDBClusterMember(instanceId).isClusterWriter();
  }

  protected void makeSureInstancesUp(String[] instances) throws InterruptedException {
    makeSureInstancesUp(instances, true);
  }

  protected void makeSureInstancesUp(String[] instances, boolean finalCheck) throws InterruptedException {
    final ExecutorService executorService = Executors.newFixedThreadPool(instances.length);
    final HashSet<String> remainingInstances = new HashSet<>(Arrays.asList(instances));

    for (final String id : instances) {
      executorService.submit(() -> {
        while (true) {
          try (final Connection conn = connectToInstance(id + DB_CONN_STR_SUFFIX, MYSQL_PORT, initFailoverDisabledProps())) {
            conn.close();
            remainingInstances.remove(id);
            break;
          } catch (final SQLException ex) {
            // Continue waiting until instance is up.
          }
          TimeUnit.MILLISECONDS.sleep(500);
        }
        return null;
      });
    }
    executorService.shutdown();
    executorService.awaitTermination(5, TimeUnit.MINUTES);

    if (finalCheck) {
      assertTrue(remainingInstances.isEmpty(), "The following instances are still down: \n"
          + String.join("\n", remainingInstances));
    }

  }
}