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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

@Disabled
@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class AwsIamAuthenticationIntegrationTest {

    private static final String DB_CONN_STR_PREFIX = "jdbc:mysql:aws://";
    private static final String DB_READONLY_CONN_STR_SUFFIX =
        System.getenv("DB_READONLY_CONN_STR_SUFFIX");
    private static final String TEST_DB_CLUSTER_IDENTIFIER =
        System.getenv("TEST_DB_CLUSTER_IDENTIFIER");
    private static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");
    private static final String TEST_DB_USER = System.getenv("TEST_DB_USER");
    private static final String DB_CONN_STR = DB_CONN_STR_PREFIX + TEST_DB_CLUSTER_IDENTIFIER + DB_READONLY_CONN_STR_SUFFIX;

    /**
     * Attempt to connect using the wrong database username
     */
    @Test
    public void test_1_WrongDatabaseUsername() {
        final Properties props = initProp("WRONG_" + TEST_DB_USER + "_USER", TEST_PASSWORD);

        Assertions.assertThrows(
            SQLException.class,
            () -> {
                final Connection conn = DriverManager.getConnection(DB_CONN_STR, props);
            }
        );
    }

    /**
     * Attempt to connect without specifying a database username
     */
    @Test
    public void test_2_NoDatabaseUsername() {
        final Properties props = initProp("", TEST_PASSWORD);

        Assertions.assertThrows(
            SQLException.class,
            () -> {
                final Connection conn = DriverManager.getConnection(DB_CONN_STR, props);
            }
        );
    }

    /**
     * Attempt to connect using IP address instead of a hostname.
     */
    @Test
    public void test_3_UsingIPAddress() {
        final Properties props = initProp(TEST_DB_USER, TEST_PASSWORD);

        final String hostname = DB_CONN_STR.substring(DB_CONN_STR_PREFIX.length());
        Assertions.assertThrows(
            SQLException.class,
            () -> { final Connection conn = DriverManager.getConnection(DB_CONN_STR_PREFIX + hostToIP(hostname), props);
        });
    }

    /**
     * Attempt to connect using valid database username/password & valid Amazon RDS hostname.
     */
    @Test
    public void test_4_ValidConnectionProperties() throws SQLException {
        final Properties props = initProp(TEST_DB_USER, TEST_PASSWORD);

        final Connection conn = DriverManager.getConnection(DB_CONN_STR, props);
        Assertions.assertNotNull(conn);

        final Statement myQuery = conn.createStatement();
        final ResultSet rs = myQuery.executeQuery("SELECT 1;");
        while (rs.next()) {
            Assertions.assertEquals("1", rs.getString(1));
        }

        conn.close();
        Assertions.assertTrue(conn.isClosed());
    }

    /**
     * Attempt to connect using valid database username, valid Amazon RDS hostname, but no password.
     */
    @Test
    public void test_5_ValidConnectionPropertiesNoPassword() throws SQLException {
        final Properties props = initProp(TEST_DB_USER, "");
        final Connection conn = DriverManager.getConnection(DB_CONN_STR, props);
        Assertions.assertNotNull(conn);

        final Statement myQuery = conn.createStatement();
        final ResultSet rs = myQuery.executeQuery("SELECT 1;");
        while (rs.next()) {
            Assertions.assertEquals("1", rs.getString(1));
        }

        conn.close();
        Assertions.assertTrue(conn.isClosed());
    }

    // Helper Functions
    private Properties initProp(final String user, final String password) {
        final Properties properties = new Properties();
        properties.setProperty(PropertyKey.useAwsIam.getKeyName(), Boolean.TRUE.toString());
        properties.setProperty(PropertyKey.USER.getKeyName(), user);
        properties.setProperty(PropertyKey.PASSWORD.getKeyName(), password);

        return properties;
    }

    private String hostToIP(final String hostname){
        InetAddress inet = null;
        try {
            inet = InetAddress.getByName(hostname);
        } catch (final UnknownHostException e) {
            e.printStackTrace();
        }
        return inet.getHostAddress();
    }
}