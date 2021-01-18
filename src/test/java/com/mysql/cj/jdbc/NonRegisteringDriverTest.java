package com.mysql.cj.jdbc;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.jdbc.ha.ReplicationConnection;
import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.aws.rds.jdbc.ConnectionByProtocolProvider;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class NonRegisteringDriverTest {
    /**
     * Tests {@link ClusterAwareConnectionProxy} return original connection if failover is not
     * enabled.
     */
    @Test
    public void testAwsProtocolReturnsClusterAwareConnectionProxy() throws SQLException {
        String url = "jdbc:mysql:aws://somehost:1234/test";
        ConnectionByProtocolProvider connProvider = Mockito.mock(ConnectionByProtocolProvider.class);
        Connection mockAwsProtocolConn = Mockito.mock(Connection.class);

        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        when(connProvider.getAwsProtocolConnection(eq(connUrl))).thenReturn(mockAwsProtocolConn);

        NonRegisteringDriver driver = new NonRegisteringDriver(connProvider);
        Connection conn = driver.connect(url, new Properties());
        assertEquals(mockAwsProtocolConn, conn);
    }

    @Test
    public void testReplicationProtocolReturnsReplicationConnection() throws SQLException {
        String url = "jdbc:mysql:replication://host-1:1234,host-2:1234/test";
        ConnectionByProtocolProvider connProvider = Mockito.mock(ConnectionByProtocolProvider.class);
        Connection mockReplicationProtocolConn = Mockito.mock(Connection.class);

        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        when(connProvider.getReplicationProtocolConnection(eq(connUrl))).thenReturn(mockReplicationProtocolConn);

        NonRegisteringDriver driver = new NonRegisteringDriver(connProvider);
        Connection replicationConn = driver.connect(url, new Properties());
        assertEquals(mockReplicationProtocolConn, replicationConn);
    }

    @Test
    public void testReplicationSrvProtocolReturnsReplicationConnection() throws SQLException {
        String url = "jdbc:mysql+srv:replication://host-1,host-2/test";
        ConnectionByProtocolProvider connProvider = Mockito.mock(ConnectionByProtocolProvider.class);
        Connection mockReplicationProtocolConn = Mockito.mock(Connection.class);

        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        when(connProvider.getReplicationProtocolConnection(eq(connUrl))).thenReturn(mockReplicationProtocolConn);

        NonRegisteringDriver driver = new NonRegisteringDriver(connProvider);
        Connection replicationConn = driver.connect(url, new Properties());
        assertEquals(mockReplicationProtocolConn, replicationConn);
    }

    @Test
    public void testReplicationProtocolWithAcceptAwsProtocolOnlyReturnsNull() throws SQLException {
        ConnectionByProtocolProvider connProvider = Mockito.mock(ConnectionByProtocolProvider.class);
        NonRegisteringDriver driver = new NonRegisteringDriver(connProvider);
        Connection conn = driver.connect("jdbc:mysql:replication://host-1:1234,host-2:1234/test?acceptAwsProtocolOnly=true", new Properties());
        assertNull(conn);
        verify(connProvider, never()).getReplicationProtocolConnection(any());
        verify(connProvider, never()).getReplicationProtocolConnection(any());
    }

    @Test
    public void testReplicationSrvProtocolWithAcceptAwsProtocolOnlyReturnsNull() throws SQLException {
        ConnectionByProtocolProvider connProvider = Mockito.mock(ConnectionByProtocolProvider.class);
        NonRegisteringDriver driver = new NonRegisteringDriver(connProvider);
        Connection conn = driver.connect("jdbc:mysql+srv:replication://host-1,host-2/test?acceptAwsProtocolOnly=true", new Properties());
        assertNull(conn);
        verify(connProvider, never()).getReplicationProtocolConnection(any());
        verify(connProvider, never()).getReplicationProtocolConnection(any());
    }
}