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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import software.aws.rds.jdbc.mysql.Driver;
import testsuite.integration.utility.ConsoleConsumer;
import testsuite.integration.utility.ExecInContainerUtility;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuroraMySqlIntegrationEnvTest {

  private static final String DB_CONN_STR_PREFIX = "jdbc:mysql://";
  private static final String DB_CONN_STR_SUFFIX = System.getenv("DB_CONN_STR_SUFFIX");
  private static final String DB_CONN_PROP = "?enabledTLSProtocols=TLSv1.2"; // Encounters SSL errors without it on GH Actions
  private static final String TEST_DB_CLUSTER_IDENTIFIER = System.getenv("TEST_DB_CLUSTER_IDENTIFIER");

  private static final String DB_CONN_HOST_BASE = DB_CONN_STR_SUFFIX.startsWith(".") ? DB_CONN_STR_SUFFIX.substring(1) : DB_CONN_STR_SUFFIX;
  private static final String DB_HOST_CLUSTER = TEST_DB_CLUSTER_IDENTIFIER + ".cluster-" + DB_CONN_HOST_BASE;
  private static final String DB_HOST_CLUSTER_RO = TEST_DB_CLUSTER_IDENTIFIER + ".cluster-ro-" + DB_CONN_HOST_BASE;

  private static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  private static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");

  private static final String RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID FROM information_schema.replica_host_status ";

  private static final String TEST_CONTAINER_NETWORK_ALIAS = "test-container";

  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";

  private static final List<ToxiproxyContainer> toxiproxyContainerList = new ArrayList<>();

  private static final List<String> mySqlInstances = new ArrayList<>();
  private static final int MYSQL_PORT = 3306;
  private static int mySQLProxyPort;

  private static final String TEST_CONTAINER_IMAGE_NAME = "openjdk:8-jdk-alpine";

  private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("shopify/toxiproxy:2.1.0");

  private static GenericContainer<?> testContainer;

  @BeforeAll
  public static void setUp() {
    Network network = Network.newNetwork();
    setUpToxiProxy(network);
    setUpTestContainer(network);
  }

  @AfterAll
  public static void tearDown() {
    for (ToxiproxyContainer proxy : toxiproxyContainerList) {
      proxy.stop();
    }
    testContainer.stop();
  }

  @Test
  public void testRunTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException, SQLException {
    System.out.println("==== Container console feed ==== >>>>");
    final Consumer<OutputFrame> consumer = new ConsoleConsumer();
    final Integer exitCode = ExecInContainerUtility.execInContainer(testContainer, consumer, "./gradlew", "test-integration-container-aurora");
    System.out.println("==== Container console feed ==== <<<<");
    assertEquals(0, exitCode, "Some tests failed.");
  }

  private static void setUpToxiProxy(Network network) {
    try {
      DriverManager.registerDriver(new Driver());
      try (final Connection conn = DriverManager.getConnection(DB_CONN_STR_PREFIX + DB_HOST_CLUSTER + DB_CONN_PROP, TEST_USERNAME, TEST_PASSWORD);
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
            DB_HOST_CLUSTER + PROXIED_DOMAIN_NAME_SUFFIX);
    toxiClusterProxy.start();
    toxiClusterProxy.getProxy(DB_HOST_CLUSTER, MYSQL_PORT);
    toxiproxyContainerList.add(toxiClusterProxy);

    final ToxiproxyContainer toxiROClusterProxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(
            "toxiproxy-ro-instance-cluster",
            DB_HOST_CLUSTER_RO + PROXIED_DOMAIN_NAME_SUFFIX);
    toxiROClusterProxy.start();
    toxiROClusterProxy.getProxy(DB_HOST_CLUSTER_RO, MYSQL_PORT);
    toxiproxyContainerList.add(toxiROClusterProxy);
  }

  private static void setUpTestContainer(Network network) {
    testContainer = new GenericContainer<>(
        new ImageFromDockerfile("bq/rds-test-container", true)
            .withDockerfileFromBuilder(builder ->
                builder
                    .from(TEST_CONTAINER_IMAGE_NAME)
                    .run("mkdir", "app")
                    .workDir("/app")
                    .entryPoint("/bin/sh -c \"while true; do sleep 30; done;\"")
                    .build()))
        .withNetworkAliases(TEST_CONTAINER_NETWORK_ALIAS)
        .withNetwork(network)
        .withFileSystemBind("./.git", "/app/.git", BindMode.READ_WRITE)
        .withFileSystemBind("./build", "/app/build", BindMode.READ_WRITE)
        .withFileSystemBind("./config", "/app/config", BindMode.READ_WRITE)
        .withFileSystemBind("./docs", "/app/docs", BindMode.READ_WRITE)
        .withFileSystemBind("./src", "/app/src", BindMode.READ_WRITE)
        .withFileSystemBind("./gradle", "/app/gradle", BindMode.READ_WRITE)
        .withCopyFileToContainer(MountableFile.forHostPath("./gradlew"), "app/gradlew")
        .withCopyFileToContainer(MountableFile.forHostPath("./gradle.properties"), "app/gradle.properties")
        .withCopyFileToContainer(MountableFile.forHostPath("./build.gradle.kts"), "app/build.gradle.kts")
        .withEnv("TEST_USERNAME", TEST_USERNAME)
        .withEnv("TEST_PASSWORD", TEST_PASSWORD)
        .withEnv("DB_CLUSTER_CONN", DB_HOST_CLUSTER)
        .withEnv("DB_RO_CLUSTER_CONN", DB_HOST_CLUSTER_RO)
        .withEnv("TOXIPROXY_CLUSTER_NETWORK_ALIAS", "toxiproxy-instance-cluster")
        .withEnv("TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS", "toxiproxy-ro-instance-cluster")
        .withEnv("PROXIED_CLUSTER_TEMPLATE", "?" + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX);

    // Add mysql instances & proxies to container env
    for (int i = 0; i < mySqlInstances.size(); i++) {
      // Add instance
      testContainer.addEnv(
          "MYSQL_INSTANCE_" + (i + 1) + "_URL",
          mySqlInstances.get(i));

      // Add proxies
      testContainer.addEnv(
          "TOXIPROXY_INSTANCE_" + (i + 1) + "_NETWORK_ALIAS",
          "toxiproxy-instance-" + (i + 1));
    }
    testContainer.addEnv("MYSQL_PORT", Integer.toString(MYSQL_PORT));
    testContainer.addEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX);
    testContainer.addEnv("MYSQL_PROXY_PORT", Integer.toString(mySQLProxyPort));

    System.out.println("Toxyproxy Instances port: " + mySQLProxyPort);
    System.out.println("Instances Proxied: " + mySqlInstances.size());

    testContainer.start();
  }
}
