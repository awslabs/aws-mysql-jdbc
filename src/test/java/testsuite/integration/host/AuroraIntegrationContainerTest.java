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

import com.mysql.cj.util.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import software.aws.rds.jdbc.mysql.Driver;
import testsuite.integration.utility.AuroraTestUtility;
import testsuite.integration.utility.ContainerHelper;

/**
 * Integration tests against RDS Aurora cluster.
 * Uses {@link AuroraTestUtility} which requires AWS Credentials to create/destroy clusters & set EC2 Whitelist.
 *
 * The following environment variables are REQUIRED for AWS IAM tests
 * - AWS_ACCESS_KEY_ID, AWS access key
 * - AWS_SECRET_ACCESS_KEY, AWS secret access key
 * - AWS_SESSION_TOKEN, AWS Session token
 *
 * The following environment variables are optional but suggested differentiating between runners
 * Provided values are just examples.
 * Assuming cluster endpoint is "database-cluster-name.XYZ.us-east-2.rds.amazonaws.com"
 *
 * TEST_DB_CLUSTER_IDENTIFIER=database-cluster-name
 * TEST_USERNAME=user-name
 * TEST_PASSWORD=user-secret-password
 */
public class AuroraIntegrationContainerTest {

  private static final int MYSQL_PORT = 3306;
  private static final String TEST_CONTAINER_NAME = "test-container";

  private static final String TEST_USERNAME =
      !StringUtils.isNullOrEmpty(System.getenv("TEST_USERNAME")) ?
        System.getenv("TEST_USERNAME") : "my_test_username";
  private static final String TEST_DB_USER =
      !StringUtils.isNullOrEmpty(System.getenv("TEST_DB_USER")) ?
        System.getenv("TEST_DB_USER") : "jane_doe";
  private static final String TEST_PASSWORD =
      !StringUtils.isNullOrEmpty(System.getenv("TEST_PASSWORD")) ?
        System.getenv("TEST_PASSWORD") : "my_test_password";
  protected static final String TEST_DB =
      !StringUtils.isNullOrEmpty(System.getenv("TEST_DB")) ? System.getenv("TEST_DB") : "test";

  private static final String AWS_ACCESS_KEY_ID = System.getenv("AWS_ACCESS_KEY_ID");
  private static final String AWS_SECRET_ACCESS_KEY = System.getenv("AWS_SECRET_ACCESS_KEY");
  private static final String AWS_SESSION_TOKEN = System.getenv("AWS_SESSION_TOKEN");

  private static final String DB_CONN_STR_PREFIX = "jdbc:mysql://";
  private static String dbConnStrSuffix = "";
  private static final String DB_CONN_PROP = "?enabledTLSProtocols=TLSv1.2";

  private static final String TEST_DB_REGION =
      !StringUtils.isNullOrEmpty(System.getenv("TEST_DB_REGION")) ?
        System.getenv("TEST_DB_REGION") : "us-east-2";
  private static final String TEST_DB_CLUSTER_IDENTIFIER =
      !StringUtils.isNullOrEmpty(System.getenv("TEST_DB_CLUSTER_IDENTIFIER")) ?
          System.getenv("TEST_DB_CLUSTER_IDENTIFIER") : "test-identifier";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  private static List<ToxiproxyContainer> proxyContainers = new ArrayList<>();
  private static List<String> mySqlInstances = new ArrayList<>();

  private static int mySQLProxyPort;
  private static GenericContainer<?> integrationTestContainer;
  private static String dbHostCluster = "";
  private static String dbHostClusterRo = "";
  private static String runnerIP = null;

  private static Network network;

  private static final ContainerHelper containerHelper = new ContainerHelper();
  private static final AuroraTestUtility auroraUtil = new AuroraTestUtility(TEST_DB_REGION);

  @BeforeAll
  static void setUp() throws SQLException, InterruptedException, UnknownHostException {
    Assertions.assertNotNull(AWS_ACCESS_KEY_ID);
    Assertions.assertNotNull(AWS_SECRET_ACCESS_KEY);
    Assertions.assertNotNull(AWS_SESSION_TOKEN);

    // Comment out below to not create a new cluster & instances
    // Note: You will need to set it to the proper DB Conn Suffix
    // i.e. For "database-cluster-name.XYZ.us-east-2.rds.amazonaws.com"
    // dbConnStrSuffix = "XYZ.us-east-2.rds.amazonaws.com"
    dbConnStrSuffix = auroraUtil.createCluster(TEST_USERNAME, TEST_PASSWORD, TEST_DB, TEST_DB_CLUSTER_IDENTIFIER);

    // Comment out getting public IP to not add & remove from EC2 whitelist
    runnerIP = auroraUtil.getPublicIPAddress();
    auroraUtil.ec2AuthorizeIP(runnerIP);

    dbHostCluster = TEST_DB_CLUSTER_IDENTIFIER + ".cluster-" + dbConnStrSuffix;
    dbHostClusterRo = TEST_DB_CLUSTER_IDENTIFIER + ".cluster-ro-" + dbConnStrSuffix;

    DriverManager.registerDriver(new Driver());

    containerHelper.addAuroraAwsIamUser(
        DB_CONN_STR_PREFIX + dbHostCluster + "/" + TEST_DB + DB_CONN_PROP,
        TEST_USERNAME,
        TEST_PASSWORD,
        dbConnStrSuffix,
        TEST_DB_USER);

    network = Network.newNetwork();
    mySqlInstances = containerHelper.getAuroraInstanceEndpoints(
            DB_CONN_STR_PREFIX + dbHostCluster + DB_CONN_PROP,
            TEST_USERNAME,
            TEST_PASSWORD,
            dbConnStrSuffix);
    proxyContainers = containerHelper.createProxyContainers(network, mySqlInstances, PROXIED_DOMAIN_NAME_SUFFIX);
    for (ToxiproxyContainer container : proxyContainers) {
      container.start();
    }
    mySQLProxyPort = containerHelper.createAuroraInstanceProxies(mySqlInstances, proxyContainers, MYSQL_PORT);

    proxyContainers.add(containerHelper.createAndStartProxyContainer(
            network,
            "toxiproxy-instance-cluster",
            dbHostCluster + PROXIED_DOMAIN_NAME_SUFFIX,
            dbHostCluster,
            MYSQL_PORT,
            mySQLProxyPort)
    );

    proxyContainers.add(containerHelper.createAndStartProxyContainer(
            network,
            "toxiproxy-ro-instance-cluster",
            dbHostClusterRo + PROXIED_DOMAIN_NAME_SUFFIX,
            dbHostClusterRo,
            MYSQL_PORT,
            mySQLProxyPort)
    );

    integrationTestContainer = initializeTestContainer(network, mySqlInstances);
  }

  @AfterAll
  static void tearDown() {
    // Comment below out if you don't want to delete cluster after tests finishes
    if (StringUtils.isNullOrEmpty(TEST_DB_CLUSTER_IDENTIFIER)) {
      auroraUtil.deleteCluster();
    } else {
      auroraUtil.deleteCluster(TEST_DB_CLUSTER_IDENTIFIER);
    }

    auroraUtil.ec2DeauthorizesIP(runnerIP);
    for (ToxiproxyContainer proxy : proxyContainers) {
      proxy.stop();
    }
    integrationTestContainer.stop();
  }

  @Test
  public void testRunTestInContainer()
    throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.runTest(integrationTestContainer, "in-container-aurora");
  }

  @Test
  public void testRunPerformanceTestInContainer()
          throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.runTest(integrationTestContainer, "in-container-aurora-performance");
  }

  @Test
  public void testDebugTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.debugTest(integrationTestContainer, "in-container-aurora");
  }

  @Test
  public void testDebugPerformanceTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.debugTest(integrationTestContainer, "in-container-aurora-performance");
  }

  protected static GenericContainer<?> initializeTestContainer(final Network network, List<String> mySqlInstances) {

    final GenericContainer<?> container = containerHelper.createTestContainer("aws/rds-test-container")
        .withNetworkAliases(TEST_CONTAINER_NAME)
        .withNetwork(network)
        .withEnv("TEST_USERNAME", TEST_USERNAME)
        .withEnv("TEST_DB_USER", TEST_DB_USER)
        .withEnv("TEST_PASSWORD", TEST_PASSWORD)
        .withEnv("TEST_DB", TEST_DB)
        .withEnv("DB_REGION", TEST_DB_REGION)
        .withEnv("DB_CLUSTER_CONN", dbHostCluster)
        .withEnv("DB_RO_CLUSTER_CONN", dbHostClusterRo)
        .withEnv("TOXIPROXY_CLUSTER_NETWORK_ALIAS", "toxiproxy-instance-cluster")
        .withEnv("TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS", "toxiproxy-ro-instance-cluster")
        .withEnv("PROXIED_CLUSTER_TEMPLATE", "?." + dbConnStrSuffix + PROXIED_DOMAIN_NAME_SUFFIX)
        .withEnv("DB_CONN_STR_SUFFIX", "." + dbConnStrSuffix)
        .withEnv("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
        .withEnv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
        .withEnv("AWS_SESSION_TOKEN", AWS_SESSION_TOKEN);

    // Add mysql instances & proxies to container env
    for (int i = 0; i < mySqlInstances.size(); i++) {
      // Add instance
      container.addEnv(
          "MYSQL_INSTANCE_" + (i + 1) + "_URL",
          mySqlInstances.get(i));

      // Add proxies
      container.addEnv(
          "TOXIPROXY_INSTANCE_" + (i + 1) + "_NETWORK_ALIAS",
          "toxiproxy-instance-" + (i + 1));
    }
    container.addEnv("MYSQL_PORT", Integer.toString(MYSQL_PORT));
    container.addEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX);
    container.addEnv("MYSQL_PROXY_PORT", Integer.toString(mySQLProxyPort));

    System.out.println("Toxiproxy Instances port: " + mySQLProxyPort);
    System.out.println("Instances Proxied: " + mySqlInstances.size());

    container.start();

    return container;
  }
}
