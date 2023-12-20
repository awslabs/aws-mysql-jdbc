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

package testsuite.integration.container;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ha.plugins.failover.FailoverConnectionPluginFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class AuroraMysqlPerformanceForEfm2IntegrationTest extends AuroraMysqlPerformanceIntegrationTest {

  @BeforeAll
  public static void setUp() throws IOException, SQLException {
    AuroraMysqlPerformanceIntegrationTest.setUp();
  }

  @AfterAll
  public static void cleanUp() throws IOException {
    doWritePerfDataToFile("./build/reports/tests/EnhancedMonitoring_efm2.xlsx", enhancedFailureMonitoringPerfDataList);
    doWritePerfDataToFile("./build/reports/tests/FailoverWithEnhancedMonitoring_efm2.xlsx", failoverWithEfmPerfDataList);
    doWritePerfDataToFile("./build/reports/tests/FailoverWithSocketTimeout_efm2.xlsx", failoverWithSocketTimeoutPerfDataList);
  }

  @Override
  protected Properties initDefaultPropsNoTimeouts() {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.tcpKeepAlive.getKeyName(), Boolean.FALSE.toString());
    props.setProperty(PropertyKey.connectionPluginFactories.getKeyName(),
        FailoverConnectionPluginFactory.class.getName()
        + ","
        + com.mysql.cj.jdbc.ha.plugins.efm2.NodeMonitoringConnectionPluginFactory.class.getName());

    // Uncomment the following line to ease debugging
    //props.setProperty(PropertyKey.logger.getKeyName(), "com.mysql.cj.log.StandardLogger");

    return props;
  }
}
