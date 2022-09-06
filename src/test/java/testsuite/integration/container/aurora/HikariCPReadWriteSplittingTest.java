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

package testsuite.integration.container.aurora;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.LogFactory;
import com.mysql.cj.log.StandardLogger;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import eu.rekawek.toxiproxy.Proxy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class HikariCPReadWriteSplittingTest extends AuroraMysqlIntegrationBaseTest {

    private static Log log = null;
    private static final String URL_SUFFIX = PROXIED_DOMAIN_NAME_SUFFIX + ":" + MYSQL_PROXY_PORT;
    private static HikariDataSource dataSource = null;
    private final List<String> clusterTopology = fetchTopology();

    private List<String> fetchTopology() {
        try {
            List<String> topology = getTopologyEndpoints();
            // topology should contain a writer and at least one reader
            if (topology == null || topology.size() < 2) {
                fail("Topology does not contain the required instances");
            }
            return topology;
        } catch (SQLException e) {
            fail("Couldn't fetch cluster topology");
        }

        return null;
    }

    private static Stream<Arguments> testParameters() {
        return Stream.of(
                Arguments.of(getConfig_allPlugins()),
                Arguments.of(getConfig_readWritePlugin())
        );
    }

    @BeforeAll
    static void setup() throws ClassNotFoundException {
        Class.forName("software.aws.rds.jdbc.mysql.Driver");
        log = LogFactory.getLogger(StandardLogger.class.getName(), Log.LOGGER_INSTANCE_NAME);

        System.setProperty("com.zaxxer.hikari.blockUntilFilled", "true");
    }

    @AfterEach
    public void teardown() {
        dataSource.close();
    }

    /**
     * After getting successful connections from the pool, the cluster becomes unavailable
     */
    @ParameterizedTest(name = "test_1_1_hikariCP_lost_connection")
    @MethodSource("testParameters")
    public void test_1_1_hikariCP_lost_connection(HikariConfig config) throws SQLException {
        createDataSource(config);
        try (Connection conn = dataSource.getConnection()) {
            assertTrue(conn.isValid(5));

            putDownAllInstances(true);

            final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(conn));
            if (pluginChainIncludesFailoverPlugin(config)) {
                assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, exception.getSQLState());
            } else {
                assertEquals(MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE, exception.getSQLState());
            }
            assertFalse(conn.isValid(5));
        }

        assertThrows(SQLTransientConnectionException.class, () -> dataSource.getConnection());
    }

    /**
     * After getting a successful connection from the pool, the connected instance becomes unavailable and the
     * connection fails over to another instance. A connection is then retrieved to check that connections
     * to failed instances are not returned
     */
    @ParameterizedTest(name = "test_1_2_hikariCP_get_dead_connection")
    @MethodSource("testParameters")
    public void test_1_2_hikariCP_get_dead_connection(HikariConfig config) throws SQLException {
        putDownAllInstances(false);

        String writer = clusterTopology.get(0);
        String reader = clusterTopology.get(1);
        String writerIdentifier = writer.split("\\.")[0];
        String readerIdentifier = reader.split("\\.")[0];
        log.logDebug("Instance to connect to: " + writerIdentifier);
        log.logDebug("Instance to fail over to: " + readerIdentifier);

        bringUpInstance(writerIdentifier);
        createDataSource(config);

        // Get a valid connection, then make it fail over to a different instance
        try (Connection conn = dataSource.getConnection()) {
            assertTrue(conn.isValid(5));
            String currentInstance = queryInstanceId(conn);
            assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
            bringUpInstance(readerIdentifier);
            putDownInstance(currentInstance);

            final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(conn));
            if (pluginChainIncludesFailoverPlugin(config)) {
                assertEquals(MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_CHANGED, exception.getSQLState());
            } else {
                assertEquals(MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE, exception.getSQLState());
                return;
            }

            // Check the connection is valid after connecting to a different instance
            assertTrue(conn.isValid(5));
            currentInstance = queryInstanceId(conn);
            log.logDebug("Connected to instance: " + currentInstance);
            assertTrue(currentInstance.equalsIgnoreCase(readerIdentifier));

            // Try to get a new connection to the failed instance, which times out
            assertThrows(SQLTransientConnectionException.class, () -> dataSource.getConnection());
        }
    }

    /**
     * After getting a successful connection from the pool, the connected instance becomes unavailable and the
     * connection fails over to another instance through the Enhanced Failure Monitor
     */
    @ParameterizedTest(name = "test_2_1_hikariCP_efm_failover")
    @MethodSource("testParameters")
    public void test_2_1_hikariCP_efm_failover(HikariConfig config) throws SQLException {
        createDataSource(config);
        putDownAllInstances(false);

        String writer = clusterTopology.get(0);
        String reader = clusterTopology.get(1);
        String writerIdentifier = writer.split("\\.")[0];
        String readerIdentifier = reader.split("\\.")[0];
        log.logDebug("Instance to connect to: " + writerIdentifier);
        log.logDebug("Instance to fail over to: " + readerIdentifier);

        bringUpInstance(writerIdentifier);
        createDataSource(config);

        // Get a valid connection, then make it fail over to a different instance
        try (Connection conn = dataSource.getConnection()) {
            assertTrue(conn.isValid(5));
            String currentInstance = queryInstanceId(conn);
            assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
            log.logDebug("Connected to instance: " + currentInstance);

            bringUpInstance(readerIdentifier);
            putDownInstance(writerIdentifier);

            final SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(conn));
            if (pluginChainIncludesFailoverPlugin(config)) {
                assertEquals(MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_CHANGED, exception.getSQLState());
            } else {
                assertEquals(MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE, exception.getSQLState());
                return;
            }

            // Check the connection is valid after connecting to a different instance
            assertTrue(conn.isValid(5));
            currentInstance = queryInstanceId(conn);
            log.logDebug("Connected to instance: " + currentInstance);
            assertTrue(currentInstance.equalsIgnoreCase(readerIdentifier));
        }
    }

    @ParameterizedTest(name = "test_3_1_readerLoadBalancing_autocommitTrue")
    @MethodSource("testParameters")
    public void test_3_1_readerLoadBalancing_autocommitTrue(HikariConfig config) throws SQLException {
        config.addDataSourceProperty(PropertyKey.loadBalanceReadOnlyTraffic.getKeyName(), "true");
        createDataSource(config);
        final String initialWriterId = instanceIDs[0];

        try (final Connection conn = dataSource.getConnection()) {
            conn.setReadOnly(false);
            String writerConnectionId = queryInstanceId(conn);
            assertEquals(initialWriterId, writerConnectionId);
            assertTrue(isDBInstanceWriter(writerConnectionId));

            conn.setReadOnly(true);
            String readerId = queryInstanceId(conn);
            assertNotEquals(writerConnectionId, readerId);
            assertTrue(isDBInstanceReader(readerId));

            // Assert switch after each execute
            String nextReaderId;
            for (int i = 0; i < 5; i++) {
                nextReaderId = queryInstanceId(conn);
                assertNotEquals(readerId, nextReaderId);
                assertTrue(isDBInstanceReader(readerId));
                readerId = nextReaderId;
            }

            // Verify behavior for transactions started while autocommit is on (autocommit is implicitly disabled)
            // Connection should not be switched while inside a transaction
            Statement stmt = conn.createStatement();
            for (int i = 0; i < 5; i++) {
                stmt.execute("  bEgiN ");
                readerId = queryInstanceId(conn);
                nextReaderId = queryInstanceId(conn);
                assertEquals(readerId, nextReaderId);
                stmt.execute(" CommIT;");
                nextReaderId = queryInstanceId(conn);
                assertNotEquals(readerId, nextReaderId);
            }
        }
    }


    @ParameterizedTest(name = "test_3_2_readerLoadBalancing_autocommitFalse")
    @MethodSource("testParameters")
    public void test_3_2_readerLoadBalancing_autocommitFalse(HikariConfig config) throws SQLException {
        config.addDataSourceProperty(PropertyKey.loadBalanceReadOnlyTraffic.getKeyName(), "true");
        createDataSource(config);
        final String initialWriterId = instanceIDs[0];

        try (final Connection conn = dataSource.getConnection()) {
            conn.setReadOnly(false);
            String writerConnectionId = queryInstanceId(conn);
            assertEquals(initialWriterId, writerConnectionId);
            assertTrue(isDBInstanceWriter(writerConnectionId));

            conn.setAutoCommit(false);
            conn.setReadOnly(true);

            // Connection should not be switched while inside a transaction
            String readerId, nextReaderId;
            Statement stmt = conn.createStatement();
            for (int i = 0; i < 5; i++) {
                readerId = queryInstanceId(conn);
                nextReaderId = queryInstanceId(conn);
                assertEquals(readerId, nextReaderId);
                conn.commit();
                nextReaderId = queryInstanceId(conn);
                assertNotEquals(readerId, nextReaderId);
            }

            for (int i = 0; i < 5; i++) {
                readerId = queryInstanceId(conn);
                nextReaderId = queryInstanceId(conn);
                assertEquals(readerId, nextReaderId);
                stmt.execute("commit");
                nextReaderId = queryInstanceId(conn);
                assertNotEquals(readerId, nextReaderId);
            }

            for (int i = 0; i < 5; i++) {
                readerId = queryInstanceId(conn);
                nextReaderId = queryInstanceId(conn);
                assertEquals(readerId, nextReaderId);
                conn.rollback();
                nextReaderId = queryInstanceId(conn);
                assertNotEquals(readerId, nextReaderId);
            }

            for (int i = 0; i < 5; i++) {
                readerId = queryInstanceId(conn);
                nextReaderId = queryInstanceId(conn);
                assertEquals(readerId, nextReaderId);
                stmt.execute(" roLLback ; ");
                nextReaderId = queryInstanceId(conn);
                assertNotEquals(readerId, nextReaderId);
            }
        }
    }

    @ParameterizedTest(name = "test_3_3_readerLoadBalancing_switchAutoCommitInTransaction")
    @MethodSource("testParameters")
    public void test_3_3_readerLoadBalancing_switchAutoCommitInTransaction(HikariConfig config) throws SQLException {
        config.addDataSourceProperty(PropertyKey.loadBalanceReadOnlyTraffic.getKeyName(), "true");
        createDataSource(config);
        final String initialWriterId = instanceIDs[0];

        try (final Connection conn = dataSource.getConnection()) {
            conn.setReadOnly(false);
            String writerConnectionId = queryInstanceId(conn);
            assertEquals(initialWriterId, writerConnectionId);
            assertTrue(isDBInstanceWriter(writerConnectionId));

            conn.setReadOnly(true);
            String readerId, nextReaderId;
            Statement stmt = conn.createStatement();

            // Start transaction while autocommit is on (autocommit is implicitly disabled)
            // Connection should not be switched while inside a transaction
            stmt.execute("  StarT   TRanSACtion  REad onLy  ; ");
            readerId = queryInstanceId(conn);
            nextReaderId = queryInstanceId(conn);
            assertEquals(readerId, nextReaderId);
            conn.setAutoCommit(false); // Switch autocommit value while inside the transaction
            nextReaderId = queryInstanceId(conn);
            assertEquals(readerId, nextReaderId);
            conn.commit();

            assertFalse(conn.getAutoCommit());
            nextReaderId = queryInstanceId(conn);
            assertNotEquals(readerId, nextReaderId); // Connection should have switched after committing

            readerId = nextReaderId;
            nextReaderId = queryInstanceId(conn);
            assertEquals(readerId, nextReaderId); // Since autocommit is now off, we should be in a transaction; connection should not be switching
            assertThrows(SQLException.class, () -> conn.setReadOnly(false));

            conn.setAutoCommit(true); // Switch autocommit value while inside the transaction
            stmt.execute("commit");

            assertTrue(conn.getAutoCommit());
            readerId = queryInstanceId(conn);

            // Autocommit is now on; connection should switch after each execute
            for (int i = 0; i < 5; i++) {
                nextReaderId = queryInstanceId(conn);
                assertNotEquals(readerId, nextReaderId);
                readerId = nextReaderId;
            }
        }
    }

    private void putDownInstance(String targetInstance) {
        Proxy toPutDown = proxyMap.get(targetInstance);
        disableInstanceConnection(toPutDown);
        log.logDebug("Took down " + targetInstance);
    }

    private void putDownAllInstances(Boolean putDownClusters) {
        log.logDebug("Putting down all instances");
        proxyMap.forEach((instance, proxy) -> {
            if (putDownClusters || (proxy != proxyCluster && proxy != proxyReadOnlyCluster)) {
                disableInstanceConnection(proxy);
            }
        });
    }

    private void disableInstanceConnection(Proxy proxy) {
        try {
            containerHelper.disableConnectivity(proxy);
        } catch (IOException e) {
            fail("Couldn't disable proxy connectivity");
        }
    }

    private void bringUpInstance(String targetInstance) {
        Proxy toBringUp = proxyMap.get(targetInstance);
        containerHelper.enableConnectivity(toBringUp);
        log.logDebug("Brought up " + targetInstance);
    }

    private static HikariConfig getConfig_allPlugins() {
        HikariConfig config = getDefaultConfig();
        addAllPlugins(config);
        return config;
    }

    private static HikariConfig getConfig_readWritePlugin() {
        HikariConfig config = getDefaultConfig();
        addReadWritePlugin(config);
        return config;
    }

    private static HikariConfig getDefaultConfig() {
        final HikariConfig config = new HikariConfig();

        config.setUsername(TEST_USERNAME);
        config.setPassword(TEST_PASSWORD);
        config.setMaximumPoolSize(3);
        config.setReadOnly(true);
        config.setExceptionOverrideClassName(HikariCPSQLException.class.getName());
        config.setInitializationFailTimeout(75000);
        config.setConnectionTimeout(1000);
        config.addDataSourceProperty(PropertyKey.failoverTimeoutMs.getKeyName(), "5000");
        config.addDataSourceProperty(PropertyKey.failoverReaderConnectTimeoutMs.getKeyName(), "1000");
        config.addDataSourceProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), PROXIED_CLUSTER_TEMPLATE);
        config.addDataSourceProperty(PropertyKey.failureDetectionTime.getKeyName(), "3000");
        config.addDataSourceProperty(PropertyKey.failureDetectionInterval.getKeyName(), "1500");
        config.addDataSourceProperty(PropertyKey.socketTimeout.getKeyName(), "3000");

        return config;
    }

    private static void addAllPlugins(HikariConfig config) {
        config.addDataSourceProperty(PropertyKey.connectionPluginFactories.getKeyName(),
                "com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory," +
                "com.mysql.cj.jdbc.ha.plugins.failover.FailoverConnectionPluginFactory," +
                "com.mysql.cj.jdbc.ha.plugins.NodeMonitoringConnectionPluginFactory");
    }

    private static void addReadWritePlugin(HikariConfig config) {
        config.addDataSourceProperty(PropertyKey.connectionPluginFactories.getKeyName(),
                "com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory");
    }

    private void createDataSource(HikariConfig config) {
        String writerEndpoint = clusterTopology.get(0);

        String jdbcUrl = DB_CONN_STR_PREFIX + writerEndpoint + URL_SUFFIX;
        log.logDebug("Writer endpoint: " + jdbcUrl);

        config.setJdbcUrl(jdbcUrl);
        dataSource = new HikariDataSource(config);

        final HikariPoolMXBean hikariPoolMXBean = dataSource.getHikariPoolMXBean();

        log.logDebug("Starting idle connections: " + hikariPoolMXBean.getIdleConnections());
        log.logDebug("Starting active connections: " + hikariPoolMXBean.getActiveConnections());
        log.logDebug("Starting total connections: " + hikariPoolMXBean.getTotalConnections());
    }

    private boolean pluginChainIncludesFailoverPlugin(HikariConfig config) {
        Properties props = config.getDataSourceProperties();
        String plugins = props.getProperty(PropertyKey.connectionPluginFactories.getKeyName());
        return plugins.contains("FailoverConnectionPlugin");
    }
}
