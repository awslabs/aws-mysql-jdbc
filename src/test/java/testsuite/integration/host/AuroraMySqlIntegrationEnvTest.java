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

package testsuite.integration.host;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.shaded.com.google.common.collect.ObjectArrays;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import software.aws.rds.jdbc.mysql.Driver;
import testsuite.integration.utility.ConsoleConsumer;
import testsuite.integration.utility.ExecInContainerUtility;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class AuroraMySqlIntegrationEnvTest {

  private static final String DB_CONN_STR_PREFIX = "jdbc:mysql://";
  private static final String DB_CONN_STR_SUFFIX = System.getenv("DB_CONN_STR_SUFFIX");
  private static final String DB_CONN_PROP = "?enabledTLSProtocols=TLSv1.2"; // Encounters SSL errors without it on GH Actions
  private static final String TEST_DB_CLUSTER_IDENTIFIER = System.getenv("TEST_DB_CLUSTER_IDENTIFIER");

  private static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  private static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");
  private static final String TEST_DB = "test";

  private static final String RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID FROM information_schema.replica_host_status ";

  private static final String TEST_CONTAINER_NETWORK_ALIAS = "test-container";

  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";

  private static final List<ToxiproxyContainer> toxiproxyContainerList = new ArrayList<>();
  private static final List<MySQLContainer<?>> communityContainerList = new ArrayList<>();

  private static final List<String> mySqlInstances = new ArrayList<>();
  private static final int MYSQL_PORT = 3306;
  private static int mySQLProxyPort;

  private static final String TEST_CONTAINER_IMAGE_NAME = "openjdk:8-jdk-alpine";
  private static final String MYSQL_CONTAINER_IMAGE_NAME = "mysql:8.0.21";
  private static final String MYSQL_CONTAINER_NAME = "mysql-container";

  private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("shopify/toxiproxy:2.1.0");

  private static GenericContainer<?> integrationTestContainer;
  private static GenericContainer<?> communityTestContainer;

  private static String dbHostCluster = "";
  private static String dbHostClusterRo = "";

  @Test
  public void testRunTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException, SQLException {
    final String dbConnHostBase =
        DB_CONN_STR_SUFFIX.startsWith(".")
            ? DB_CONN_STR_SUFFIX.substring(1)
            : DB_CONN_STR_SUFFIX;
    dbHostCluster = TEST_DB_CLUSTER_IDENTIFIER + ".cluster-" + dbConnHostBase;
    dbHostClusterRo = TEST_DB_CLUSTER_IDENTIFIER + ".cluster-ro-" + dbConnHostBase;
    final Network network = Network.newNetwork();

    setUpToxiProxy(network);
    setUpTestContainer(network);
    runTest(integrationTestContainer, "test-integration-container-aurora");

    for (ToxiproxyContainer proxy : toxiproxyContainerList) {
      proxy.stop();
    }
    integrationTestContainer.stop();
  }

  @Test
  public void testRunCommunityTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {
    final Network communityTestNetwork = Network.newNetwork();
    setupCommunityMySQLContainers(communityTestNetwork);
    setupCommunityTestContainer(communityTestNetwork);

    runTest(communityTestContainer, "test-non-integration");

    for (final MySQLContainer<?> dockerComposeContainer : communityContainerList) {
      dockerComposeContainer.stop();
    }
    communityTestContainer.stop();
  }

  private void runTest(GenericContainer<?> container, String task)
      throws IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer();
    Integer exitCode = ExecInContainerUtility.execInContainer(container, consumer, "./gradlew", task);
    System.out.println("==== Container console feed ==== <<<<");
    assertEquals(0, exitCode, "Some tests failed.");
  }

  private static void setUpToxiProxy(Network network) {
    try {
      DriverManager.registerDriver(new Driver());
      try (final Connection conn = DriverManager.getConnection(DB_CONN_STR_PREFIX + dbHostCluster
          + DB_CONN_PROP, TEST_USERNAME, TEST_PASSWORD);
           final Statement stmt = conn.createStatement()) {
          // Get instances
          try (final ResultSet resultSet = stmt.executeQuery(RETRIEVE_TOPOLOGY_SQL)) {
            int instanceCount = 0;
            while (resultSet.next()) {
              // Get Instance endpoints
              final String hostEndpoint = resultSet.getString("SERVER_ID") + DB_CONN_STR_SUFFIX;
              mySqlInstances.add(hostEndpoint);

              // Create & Start Toxi Proxy Container
              final ToxiproxyContainer toxiProxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                  .withNetwork(network)
                  .withNetworkAliases(
                      "toxiproxy-instance-" + (++instanceCount),
                      hostEndpoint + PROXIED_DOMAIN_NAME_SUFFIX);
              toxiProxy.start();
              mySQLProxyPort = toxiProxy.getProxy(hostEndpoint, MYSQL_PORT).getOriginalProxyPort();
              toxiproxyContainerList.add(toxiProxy);
          }
        }
      }
    } catch (SQLException e) {
      Assertions.fail(String.format("Failed initialize instances. Got exception: \n%s", e.getMessage()));
    }

    // Adding cluster endpoints
    final ToxiproxyContainer toxiClusterProxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(
            "toxiproxy-instance-cluster",
            dbHostCluster + PROXIED_DOMAIN_NAME_SUFFIX);
    toxiClusterProxy.start();
    toxiClusterProxy.getProxy(dbHostCluster, MYSQL_PORT);
    toxiproxyContainerList.add(toxiClusterProxy);

    final ToxiproxyContainer toxiROClusterProxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(
            "toxiproxy-ro-instance-cluster",
            dbHostClusterRo + PROXIED_DOMAIN_NAME_SUFFIX);
    toxiROClusterProxy.start();
    toxiROClusterProxy.getProxy(dbHostClusterRo, MYSQL_PORT);
    toxiproxyContainerList.add(toxiROClusterProxy);
  }

  private static void setUpTestContainer(Network network) {
    integrationTestContainer = createTestContainerTemplate("bq/rds-test-container")
        .withNetworkAliases(TEST_CONTAINER_NETWORK_ALIAS)
        .withNetwork(network)
        .withEnv("TEST_USERNAME", TEST_USERNAME)
        .withEnv("TEST_PASSWORD", TEST_PASSWORD)
        .withEnv("DB_CLUSTER_CONN", dbHostCluster)
        .withEnv("DB_RO_CLUSTER_CONN", dbHostClusterRo)
        .withEnv("TOXIPROXY_CLUSTER_NETWORK_ALIAS", "toxiproxy-instance-cluster")
        .withEnv("TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS", "toxiproxy-ro-instance-cluster")
        .withEnv("PROXIED_CLUSTER_TEMPLATE", "?" + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX);

    // Add mysql instances & proxies to container env
    for (int i = 0; i < mySqlInstances.size(); i++) {
      // Add instance
      integrationTestContainer.addEnv(
          "MYSQL_INSTANCE_" + (i + 1) + "_URL",
          mySqlInstances.get(i));

      // Add proxies
      integrationTestContainer.addEnv(
          "TOXIPROXY_INSTANCE_" + (i + 1) + "_NETWORK_ALIAS",
          "toxiproxy-instance-" + (i + 1));
    }
    integrationTestContainer.addEnv("MYSQL_PORT", Integer.toString(MYSQL_PORT));
    integrationTestContainer.addEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX);
    integrationTestContainer.addEnv("MYSQL_PROXY_PORT", Integer.toString(mySQLProxyPort));

    System.out.println("Toxyproxy Instances port: " + mySQLProxyPort);
    System.out.println("Instances Proxied: " + mySqlInstances.size());

    integrationTestContainer.start();
  }

  private static void setupCommunityTestContainer(final Network network) {
    communityTestContainer = createTestContainerTemplate("bq/community-test-container")
        .withNetworkAliases("community-" + TEST_CONTAINER_NETWORK_ALIAS)
        .withNetwork(network);
    communityTestContainer.addEnv(
        "TEST_MYSQL_PORT",
        String.valueOf(MYSQL_PORT));
    communityTestContainer.addEnv(
        "TEST_MYSQL_DOMAIN",
        MYSQL_CONTAINER_NAME);
    communityTestContainer.start();
  }

  private static void setupCommunityMySQLContainers(Network network) {
    final MySQLContainer<?> mySQLContainer =
        createMySQLContainer(network, MYSQL_CONTAINER_NAME,
            "--log-error-verbosity=4",
            "--default-authentication-plugin=sha256_password",
            "--sha256_password_public_key_path=/home/certdir/mykey.pub",
            "--sha256_password_private_key_path=/home/certdir/mykey.pem",
            "--caching_sha2_password_public_key_path=/home/certdir/mykey.pub",
            "--caching_sha2_password_private_key_path=/home/certdir/mykey.pem");

    mySQLContainer.start();
    communityContainerList.add(mySQLContainer);
  }

  private static MySQLContainer<?> createMySQLContainer(
      Network network,
      String networkAlias,
      String... commands) {
    final String[] defaultCommands = new String[] {
        "--local_infile=1",
        "--max_allowed_packet=40M",
        "--max-connections=2048",
        "--secure-file-priv=/var/lib/mysql",
        "--ssl-key=/home/certdir/server-key.pem",
        "--ssl-cert=/home/certdir/server-cert.pem",
        "--ssl-ca=/home/certdir/ca-cert.pem",
        "--plugin_dir=/home/plugin_dir"
    };

    String[] fullCommands = defaultCommands;

    if (commands.length != 0) {
      fullCommands = ObjectArrays.concat(defaultCommands, commands, String.class);
    }

    return new MySQLContainer<>(MYSQL_CONTAINER_IMAGE_NAME)
        .withNetwork(network)
        .withNetworkAliases(networkAlias)
        .withDatabaseName(TEST_DB)
        .withPassword("root")
        .withFileSystemBind(
            "src/test/config/ssl-test-certs/",
            "/home/certdir/",
            BindMode.READ_WRITE)
        .withFileSystemBind("src/test/config/plugins/", "/home/plugin_dir/", BindMode.READ_WRITE)
        .withFileSystemBind(
            "src/test/config/docker-entrypoint-initdb.d",
            "/docker-entrypoint-initdb.d",
            BindMode.READ_WRITE)
        .withCommand(fullCommands);
  }

  private static GenericContainer<?> createTestContainerTemplate(String dockerImageName) {
    return new GenericContainer<>(
        new ImageFromDockerfile(dockerImageName, true)
            .withDockerfileFromBuilder(builder ->
                builder
                    .from(TEST_CONTAINER_IMAGE_NAME)
                    .run("mkdir", "app")
                    .workDir("/app")
                    .entryPoint("/bin/sh -c \"while true; do sleep 30; done;\"")
                    .build()))
        .withFileSystemBind("./.git", "/app/.git", BindMode.READ_WRITE)
        .withFileSystemBind("./build", "/app/build", BindMode.READ_WRITE)
        .withFileSystemBind("./config", "/app/config", BindMode.READ_WRITE)
        .withFileSystemBind("./docs", "/app/docs", BindMode.READ_WRITE)
        .withFileSystemBind("./src", "/app/src", BindMode.READ_WRITE)
        .withFileSystemBind("./gradle", "/app/gradle", BindMode.READ_WRITE)
        .withCopyFileToContainer(MountableFile.forHostPath("./gradlew"), "app/gradlew")
        .withCopyFileToContainer(MountableFile.forHostPath("./gradle.properties"), "app/gradle.properties")
        .withCopyFileToContainer(MountableFile.forHostPath("./build.gradle.kts"), "app/build.gradle.kts");
  }
}
