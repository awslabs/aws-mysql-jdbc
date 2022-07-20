// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0
// (GPLv2), as published by the Free Software Foundation, with the
// following additional permissions:
//
// This program is distributed with certain software that is licensed
// under separate terms, as designated in a particular file or component
// or in the license documentation. Without limiting your rights under
// the GPLv2, the authors of this program hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have included with the program.
//
// Without limiting the foregoing grant of rights under the GPLv2 and
// additional permission as to separately licensed software, this
// program is also subject to the Universal FOSS Exception, version 1.0,
// a copy of which can be found along with its FAQ at
// http://oss.oracle.com/licenses/universal-foss-exception.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see
// http://www.gnu.org/licenses/gpl-2.0.html.

package testsuite.integration.host;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import testsuite.integration.utility.ContainerHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

public class StandardMysqlContainerTest {
  private static final String TEST_CONTAINER_NAME = "test-container";
  private static final String MYSQL_WRITER_CONTAINER_NAME = "mysql-writer";
  private static final String MYSQL_READER_CONTAINER_NAME = "mysql-reader";
  private static final List<String> mySqlInstances = Arrays.asList(MYSQL_WRITER_CONTAINER_NAME, MYSQL_READER_CONTAINER_NAME);
  private static final int MYSQL_PORT = 3306;
  private static final String TEST_DB = "test";
  private static final String MYSQL_USERNAME = "root";
  private static final String MYSQL_PASSWORD = "root";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";

  private static MySQLContainer<?> mysqlWriterContainer;
  private static MySQLContainer<?> mysqlReaderContainer;
  private static GenericContainer<?> integrationTestContainer;
  private static List<ToxiproxyContainer> proxyContainers = new ArrayList<>();
  private static int mySQLProxyPort;
  private static Network network;
  private static final ContainerHelper containerHelper = new ContainerHelper();

  @BeforeAll
  static void setUp() {
    network = Network.newNetwork();

    mysqlWriterContainer = containerHelper.createMysqlContainer(network, MYSQL_WRITER_CONTAINER_NAME, TEST_DB);
    mysqlWriterContainer.start();

    mysqlReaderContainer = containerHelper.createMysqlContainer(network, MYSQL_READER_CONTAINER_NAME, TEST_DB);
    mysqlReaderContainer.start();

    proxyContainers = containerHelper.createProxyContainers(network, mySqlInstances, PROXIED_DOMAIN_NAME_SUFFIX);
    for (ToxiproxyContainer container : proxyContainers) {
      container.start();
    }
    mySQLProxyPort = containerHelper.createMysqlInstanceProxies(mySqlInstances, proxyContainers, MYSQL_PORT);

    integrationTestContainer = createTestContainer();
    integrationTestContainer.start();

    try {
      integrationTestContainer.execInContainer("dos2unix", "gradlew");
    } catch (InterruptedException | UnsupportedOperationException | IOException e) {
      fail("Mysql integration test container initialised incorrectly");
    }
  }

  @AfterAll
  static void tearDown() {
    for (ToxiproxyContainer proxy : proxyContainers) {
      proxy.stop();
    }

    mysqlWriterContainer.stop();
    mysqlReaderContainer.stop();
    integrationTestContainer.stop();
  }

  @Test
  public void runTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.runTest(integrationTestContainer, "in-container-standard-mysql");
  }

  @Test
  public void debugTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {

    containerHelper.debugTest(integrationTestContainer, "in-container-standard-mysql");
  }

  protected static GenericContainer<?> createTestContainer() {
    return containerHelper.createTestContainer("aws/rds-test-container")
        .withNetworkAliases(TEST_CONTAINER_NAME)
        .withNetwork(network)
        .withEnv("TEST_WRITER_HOST", MYSQL_WRITER_CONTAINER_NAME)
        .withEnv("TEST_READER_HOST", MYSQL_READER_CONTAINER_NAME)
        .withEnv("TEST_PORT", String.valueOf(MYSQL_PORT))
        .withEnv("TEST_DB", TEST_DB)
        .withEnv("TEST_USERNAME", MYSQL_USERNAME)
        .withEnv("TEST_PASSWORD", MYSQL_PASSWORD)
        .withEnv("PROXY_PORT", Integer.toString(mySQLProxyPort))
        .withEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX)
        .withEnv("TOXIPROXY_WRITER", "toxiproxy-instance-1")
        .withEnv("TOXIPROXY_READER", "toxiproxy-instance-2");
  }
}
