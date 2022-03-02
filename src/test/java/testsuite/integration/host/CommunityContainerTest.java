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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import testsuite.integration.utility.ContainerHelper;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.fail;

public class CommunityContainerTest {

  private static final int MYSQL_PORT = 3306;
  private static final String TEST_DB = "test";
  private static final String TEST_CONTAINER_NAME = "test-container";
  private static final String MYSQL_CONTAINER_NAME = "mysql-container";

  private static MySQLContainer<?> communityMysqlContainer;
  private static GenericContainer<?> communityTestContainer;
  private static Network network;
  private static final ContainerHelper containerHelper = new ContainerHelper();

  @BeforeAll
  static void setUp() {
    network = Network.newNetwork();

    communityMysqlContainer = containerHelper.createMysqlContainer(network, MYSQL_CONTAINER_NAME, TEST_DB);
    communityMysqlContainer.start();

    communityTestContainer = createTestContainer(network, TEST_CONTAINER_NAME, MYSQL_CONTAINER_NAME, MYSQL_PORT);
    communityTestContainer.start();

    try {
      communityTestContainer.execInContainer("dos2unix", "gradlew");
    } catch (InterruptedException | UnsupportedOperationException | IOException e) {
      fail("Community test container initialised incorrectly");
    }
  }

  @AfterAll
  static void tearDown() {
    communityMysqlContainer.stop();
    communityTestContainer.stop();
  }

  @Test
  public void testRunCommunityTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.runTest(communityTestContainer, "in-container-community");
  }

  @Test
  public void testDebugCommunityTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.debugTest(communityTestContainer, "in-container-community");
  }

  protected static GenericContainer<?> createTestContainer(
          final Network network, String testContainerNetworkAlias, String mysqlContainerName, int mysqlPort) {
    final GenericContainer<?> container =
      containerHelper.createTestContainer("aws/rds-test-container")
        .withNetworkAliases(testContainerNetworkAlias)
        .withNetwork(network)
        .withEnv("TEST_MYSQL_PORT", String.valueOf(mysqlPort))
        .withEnv("TEST_MYSQL_DOMAIN", mysqlContainerName);

    return container;
  }
}
