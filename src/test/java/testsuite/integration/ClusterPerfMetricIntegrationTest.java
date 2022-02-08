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

package testsuite.integration;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ha.plugins.failover.IClusterAwareMetricsReporter;
import com.mysql.cj.log.NullLogger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import testsuite.integration.utility.ContainerHelper;

/** Integration test for cluster performance metrics */
@Disabled
public class ClusterPerfMetricIntegrationTest {

    private static final String DB_CONN_STR_PREFIX = "jdbc:mysql:aws://";
    private static final String DB_CONN_STR_SUFFIX =
        System.getenv("DB_CONN_STR_SUFFIX");
    private static final String TEST_DB_CLUSTER_IDENTIFIER =
        System.getenv("TEST_DB_CLUSTER_IDENTIFIER");
    private static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
    private static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");
    private static final String DB_CONN_HOST_BASE = DB_CONN_STR_SUFFIX.startsWith(".") ? DB_CONN_STR_SUFFIX.substring(1) : DB_CONN_STR_SUFFIX;
    private static final String DB_CONN_STR = DB_CONN_STR_PREFIX + TEST_DB_CLUSTER_IDENTIFIER + ".cluster-" + DB_CONN_HOST_BASE;
    private static final String PORT_SUFFIX = ":3306";

    private List<String> instanceUrls;

    public ClusterPerfMetricIntegrationTest() throws ClassNotFoundException, SQLException {
        Class.forName("software.aws.rds.jdbc.mysql.Driver");
        final ContainerHelper helper = new ContainerHelper();
        instanceUrls = helper.getAuroraClusterInstances(DB_CONN_STR, TEST_USERNAME, TEST_PASSWORD, DB_CONN_HOST_BASE);
    }

    @BeforeEach
    void init() {
        IClusterAwareMetricsReporter.resetMetrics();
    }

    /**
     * Test collecting performance metrics for cluster
     */
    @Test
    public void testCollectClusterMetrics() throws SQLException {
        final Properties props = initDefaultProps();

        final Connection conn = DriverManager.getConnection(DB_CONN_STR, props);
        conn.close();

        final TestLogger logger = new TestLogger();
        final List<String> logs = logger.getLogs();

        // Not collecting for instances
        for (int i = 0; i < instanceUrls.size(); i++) {
            final String instanceUrl = instanceUrls.get(i);
            IClusterAwareMetricsReporter.reportMetrics(instanceUrl + PORT_SUFFIX, logger);
            Assertions.assertEquals("** No metrics collected for '" + instanceUrl + PORT_SUFFIX + "' **\n", logs.get(i));
        }

        logs.clear();
        IClusterAwareMetricsReporter.reportMetrics(TEST_DB_CLUSTER_IDENTIFIER + ".cluster-" + DB_CONN_HOST_BASE + PORT_SUFFIX, logger);
        Assertions.assertTrue(logs.size() > 1);
    }

    /**
     * Test collecting for as well instances
     */
    @Test
    public void testCollectInstanceMetrics() throws SQLException {
        final Properties props = initDefaultProps();
        props.setProperty(PropertyKey.gatherAdditionalMetricsOnInstance.getKeyName(), "TRUE");

        String currWriterUrl = "";
        try (final Connection conn = DriverManager.getConnection(DB_CONN_STR, props);
            final Statement myStmt = conn.createStatement();
            final ResultSet result = myStmt.executeQuery("SELECT @@aurora_server_id")) {
            if (result.next()) {
                currWriterUrl = result.getString(1) + DB_CONN_STR_SUFFIX;
            }
        }

        final TestLogger logger = new TestLogger();
        final List<String> logs = logger.getLogs();
        IClusterAwareMetricsReporter.reportMetrics(currWriterUrl + PORT_SUFFIX, logger, true);
        Assertions.assertTrue(logs.size() > 1);
    }

    private Properties initDefaultProps() {
        final Properties properties = new Properties();
        properties.setProperty(PropertyKey.gatherPerfMetrics.getKeyName(), "TRUE");
        properties.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
        properties.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
        properties.setProperty(PropertyKey.logger.getKeyName(), NullLogger.class.getName());
        return properties;
    }

    public static class TestLogger extends NullLogger {

        private final List<String> logs = new ArrayList<>();

        public TestLogger() {
            super("");
        }

        public void logInfo(Object msg) {
            logs.add(msg.toString());
        }

        public List<String> getLogs() {
            return logs;
        }
    }
}
