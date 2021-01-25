package com.mysql.cj.jdbc;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.jdbc.ha.*;
import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class NonRegisteringDriverTest {
    // TODO: test method javadocs and license headers for all added files
    /**
     * Tests {@link ClusterAwareConnectionProxy} return original connection if failover is not
     * enabled.
     */
    @Test
    public void testSingleConnectionProtocolReturnsConnectionImpl() throws SQLException {
        String url = "jdbc:mysql://somehost:1234/test";
        JdbcConnection mockConnectionImplConn = Mockito.mock(JdbcConnection.class);
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        NonRegisteringDriver driver = new NonRegisteringDriver();

        try (MockedStatic mockStaticConnectionImpl = mockStatic(ConnectionImpl.class)) {
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

        try (MockedStatic mockStaticClusterAwareConnectionProxy = mockStatic(ClusterAwareConnectionProxy.class)) {
            mockStaticClusterAwareConnectionProxy.when(() -> ClusterAwareConnectionProxy.autodetectClusterAndCreateProxyInstance(eq(connUrl))).thenReturn(mockAwsProtocolConn);
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

        try (MockedStatic mockStaticFailoverConnectionProxy = mockStatic(FailoverConnectionProxy.class)) {
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

        try (MockedStatic mockStaticLoadBalancedConnectionProxy = mockStatic(LoadBalancedConnectionProxy.class)) {
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

        try (MockedStatic mockStaticReplicationConnectionProxy = mockStatic(ReplicationConnectionProxy.class)) {
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