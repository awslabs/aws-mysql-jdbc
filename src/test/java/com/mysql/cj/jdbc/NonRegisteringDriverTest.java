/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of this program hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of this connector, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.jdbc.ha.ConnectionProxy;
import com.mysql.cj.jdbc.ha.FailoverConnectionProxy;
import com.mysql.cj.jdbc.ha.LoadBalancedConnection;
import com.mysql.cj.jdbc.ha.LoadBalancedConnectionProxy;
import com.mysql.cj.jdbc.ha.ReplicationConnection;
import com.mysql.cj.jdbc.ha.ReplicationConnectionProxy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class NonRegisteringDriverTest {

    @BeforeEach
    public void setAwsProtocolOnlyToFalse() {
        software.aws.rds.jdbc.mysql.Driver.setAcceptAwsProtocolOnly(false);
    }

    @AfterAll
    public static void cleanup() throws SQLException {
        Driver registeredDriver = DriverManager.getDriver("jdbc:mysql://localhost");
        if (registeredDriver instanceof software.aws.rds.jdbc.mysql.Driver)
        {
            DriverManager.deregisterDriver(registeredDriver);
        }
    }

    @Test
    public void testSetAwsProtocolOnlySwitch() throws Exception {
        software.aws.rds.jdbc.mysql.Driver drv = new software.aws.rds.jdbc.mysql.Driver();
        assertNotNull(drv);

        assertFalse(drv.acceptsURL("jdbc:mysql://localhost:5432/test?acceptAwsProtocolOnly=true"));
        assertTrue(drv.acceptsURL("jdbc:mysql://localhost:5432/test?acceptAwsProtocolOnly=false"));

        software.aws.rds.jdbc.mysql.Driver.setAcceptAwsProtocolOnly(true);
        assertFalse(drv.acceptsURL("jdbc:mysql://localhost:5432/test"));
        assertTrue(drv.acceptsURL("jdbc:mysql:aws://localhost:5432/test"));

        // Check if the connection property is prioritized over the global setting.
        assertTrue(drv.acceptsURL("jdbc:mysql://localhost:5432/test?acceptAwsProtocolOnly=false"));

        software.aws.rds.jdbc.mysql.Driver.setAcceptAwsProtocolOnly(false);
        assertTrue(drv.acceptsURL("jdbc:mysql://localhost:5432/test"));
        assertTrue(drv.acceptsURL("jdbc:mysql:aws://localhost:5432/test"));

        assertFalse(drv.acceptsURL("jdbc:mysql://localhost:5432/test?acceptAwsProtocolOnly=true"));
        assertTrue(drv.acceptsURL("jdbc:mysql://localhost:5432/test?acceptAwsProtocolOnly=false"));
    }

    @Test
    public void testSingleConnectionProtocolReturnsConnectionImpl() throws SQLException {
        String url = "jdbc:mysql://somehost:1234/test";
        JdbcConnection mockConnectionImplConn = Mockito.mock(JdbcConnection.class);
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        NonRegisteringDriver driver = new NonRegisteringDriver();

        try (MockedStatic<ConnectionImpl> mockStaticConnectionImpl = mockStatic(ConnectionImpl.class)) {
            mockStaticConnectionImpl.when(() -> ConnectionImpl.getInstance(eq(connUrl.getMainHost()))).thenReturn(mockConnectionImplConn);
            Connection conn = driver.connect(url, new Properties());
            assertEquals(mockConnectionImplConn, conn);
        }
    }

    @Test
    public void testAwsProtocolReturnsClusterAwareConnectionProxy() throws SQLException {
        String url = "jdbc:mysql:aws://somehost:1234/test";
        JdbcConnection mockAwsProtocolConn = Mockito.mock(JdbcConnection.class);
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        NonRegisteringDriver driver = new NonRegisteringDriver();

        try (MockedStatic<ConnectionProxy> mockStaticClusterAwareConnectionProxy = mockStatic(
            ConnectionProxy.class)) {
            mockStaticClusterAwareConnectionProxy.when(() -> ConnectionProxy.autodetectClusterAndCreateProxyInstance(eq(connUrl))).thenReturn(mockAwsProtocolConn);
            Connection conn = driver.connect(url, new Properties());
            assertEquals(mockAwsProtocolConn, conn);
        }
    }

    @Test
    public void testFailoverProtocolReturnsFailoverConnectionProxy() throws SQLException {
        String url = "jdbc:mysql://host-1:1234,host-2:1234/test";
        JdbcConnection mockFailoverProtocolConn = Mockito.mock(JdbcConnection.class);
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        NonRegisteringDriver driver = new NonRegisteringDriver();

        try (MockedStatic<FailoverConnectionProxy> mockStaticFailoverConnectionProxy = mockStatic(FailoverConnectionProxy.class)) {
            mockStaticFailoverConnectionProxy.when(() -> FailoverConnectionProxy.createProxyInstance(eq(connUrl))).thenReturn(mockFailoverProtocolConn);
            Connection conn = driver.connect(url, new Properties());
            assertEquals(mockFailoverProtocolConn, conn);
        }
    }

    @Test
    public void testLoadBalanceProtocolReturnsLoadBalancedConnectionProxy() throws SQLException {
        String url = "jdbc:mysql:loadbalance://somehost:1234/test";
        JdbcConnection mockLoadBalancedProtocolConn = Mockito.mock(LoadBalancedConnection.class);
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        NonRegisteringDriver driver = new NonRegisteringDriver();

        try (MockedStatic<LoadBalancedConnectionProxy> mockStaticLoadBalancedConnectionProxy = mockStatic(LoadBalancedConnectionProxy.class)) {
            mockStaticLoadBalancedConnectionProxy.when(() -> LoadBalancedConnectionProxy.createProxyInstance(eq(connUrl))).thenReturn(mockLoadBalancedProtocolConn);
            Connection conn = driver.connect(url, new Properties());
            assertEquals(mockLoadBalancedProtocolConn, conn);
        }
    }

    @Test
    public void testReplicationProtocolReturnsReplicationConnectionProxy() throws SQLException {
        String url = "jdbc:mysql:replication://host-1:1234,host-2:1234/test";
        ReplicationConnection mockReplicationProtocolConn = Mockito.mock(ReplicationConnection.class);
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        NonRegisteringDriver driver = new NonRegisteringDriver();

        try (MockedStatic<ReplicationConnectionProxy> mockStaticReplicationConnectionProxy = mockStatic(ReplicationConnectionProxy.class)) {
            mockStaticReplicationConnectionProxy.when(() -> ReplicationConnectionProxy.createProxyInstance(eq(connUrl))).thenReturn(mockReplicationProtocolConn);
            Connection replicationConn = driver.connect(url, new Properties());
            assertEquals(mockReplicationProtocolConn, replicationConn);
        }
    }

    @Test
    public void testReplicationProtocolWithAcceptAwsProtocolOnlyReturnsNull() throws SQLException {
        NonRegisteringDriver driver = new NonRegisteringDriver();
        Connection conn = driver.connect("jdbc:mysql:replication://host-1:1234,host-2:1234/test?acceptAwsProtocolOnly=true", new Properties());
        assertNull(conn);
    }
}