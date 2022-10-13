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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class AuroraMysqlHardFailureIntegrationTest extends AuroraMysqlIntegrationBaseTest {
    protected final HashSet<String> instancesToCrash = new HashSet<>();
    protected ExecutorService crashInstancesExecutorService;

    /** Current reader dies, execute a “write“ statement, an exception should be thrown. */
    @Test
    public void test_readerDiesAndExecuteWriteQuery()
        throws SQLException, InterruptedException {

        // Connect to Instance2 which is a reader.
        assertTrue(clusterSize >= 5, "Minimal cluster configuration: 1 writer + 4 readers");
        final String readerEndpoint = instanceIDs[1];

        final Properties props = initDefaultProps();
        try (final Connection conn = connectToInstance(readerEndpoint + DB_CONN_STR_SUFFIX, MYSQL_PORT, props)) {
            // Crash Instance2.
            startCrashingInstances(readerEndpoint);
            makeSureInstancesDown(readerEndpoint);

            final Statement testStmt1 = conn.createStatement();

            // Assert that an exception with SQLState code S1009 is thrown.
            final SQLException exception =
                assertThrows(
                    SQLException.class, () -> testStmt1.executeUpdate("DROP TABLE IF EXISTS test2_6"));
            assertEquals("S1009", exception.getSQLState());

            // This ensures that the write statement above did not kick off a driver side failover.
            assertFirstQueryThrows(conn, "08S02");
        }
    }

    /** Reader connection failover within the connection pool. */
    @Test
    public void test_pooledReaderConnection_BasicFailover()
        throws SQLException, InterruptedException {

        assertTrue(clusterSize >= 5, "Minimal cluster configuration: 1 writer + 4 readers");
        final String readerId = instanceIDs[1];

        try (final Connection conn = createPooledConnectionWithInstanceId(readerId)) {
            conn.setReadOnly(true);

            startCrashingInstances(readerId);
            makeSureInstancesDown(readerId);

            assertFirstQueryThrows(conn, "08S02");

            // Assert that we are now connected to a new reader instance.
            final String currentConnectionId = queryInstanceId(conn);
            assertTrue(isDBInstanceReader(currentConnectionId));
            assertNotEquals(currentConnectionId, readerId);

            // Assert that the pooled connection is valid.
            assertTrue(conn.isValid(IS_VALID_TIMEOUT));
        }
    }

    @BeforeEach
    void validateCluster() throws InterruptedException, SQLException, IOException {
        crashInstancesExecutorService = Executors.newFixedThreadPool(clusterSize);
        instancesToCrash.clear();
        for (final String id : instanceIDs) {
            crashInstancesExecutorService.submit(() -> {
                while (true) {
                    if (instancesToCrash.contains(id)) {
                        try (final Connection conn = connectToInstance(id + DB_CONN_STR_SUFFIX, MYSQL_PORT, initFailoverDisabledProps());
                            final Statement myStmt = conn.createStatement()) {
                            myStmt.execute("ALTER SYSTEM CRASH INSTANCE");
                        } catch (final SQLException e) {
                            // Do nothing and keep creating connection to crash instance.
                        }
                    }
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            });
        }
        crashInstancesExecutorService.shutdown();

        super.setUpEach();
    }

    @AfterEach
    void reviveInstancesAndCloseTestConnection() throws InterruptedException {
        instancesToCrash.clear();
        crashInstancesExecutorService.shutdownNow();

        makeSureInstancesUp(instanceIDs, false);
    }

    protected void startCrashingInstances(String... instances) {
        instancesToCrash.addAll(Arrays.asList(instances));
    }

    protected void makeSureInstancesDown(String... instances) throws InterruptedException {
        final ExecutorService executorService = Executors.newFixedThreadPool(instances.length);
        final HashSet<String> remainingInstances = new HashSet<>(Arrays.asList(instances));

        for (final String id : instances) {
            executorService.submit(() -> {
                while (true) {
                    try (final Connection conn = connectToInstance(id + DB_CONN_STR_SUFFIX, MYSQL_PORT, initFailoverDisabledProps())) {
                        // Continue waiting until instance is down.
                    } catch (final SQLException e) {
                        remainingInstances.remove(id);
                        break;
                    }
                    TimeUnit.MILLISECONDS.sleep(500);
                }
                return null;
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(120, TimeUnit.SECONDS);

        assertTrue(remainingInstances.isEmpty(), "The following instances are still up: \n"
            + String.join("\n", remainingInstances));
    }

}
