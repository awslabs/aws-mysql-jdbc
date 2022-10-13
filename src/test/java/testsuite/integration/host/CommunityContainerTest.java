/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0
 * (GPLv2), as published by the Free Software Foundation, with the
 * following additional permissions:
 *
 * This program is distributed with certain software that is licensed
 * under separate terms, as designated in a particular file or component
 * or in the license documentation. Without limiting your rights under
 * the GPLv2, the authors of this program hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with the program.
 *
 * Without limiting the foregoing grant of rights under the GPLv2 and
 * additional permission as to separately licensed software, this
 * program is also subject to the Universal FOSS Exception, version 1.0,
 * a copy of which can be found along with its FAQ at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see
 * http://www.gnu.org/licenses/gpl-2.0.html.
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
