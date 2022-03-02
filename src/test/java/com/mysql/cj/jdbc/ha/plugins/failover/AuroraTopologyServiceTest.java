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

package com.mysql.cj.jdbc.ha.plugins.failover;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/** AuroraTopologyServiceTest class. */
public class AuroraTopologyServiceTest {

  private final AuroraTopologyService spyProvider = Mockito.spy(new AuroraTopologyService(Mockito.mock(Log.class)));

  @BeforeEach
  void resetProvider() {
    spyProvider.setClusterId(UUID.randomUUID().toString());
    spyProvider.setClusterInstanceTemplate(new HostInfo(null, "?", HostInfo.NO_PORT, null, null));
    spyProvider.setRefreshRate(AuroraTopologyService.DEFAULT_REFRESH_RATE_IN_MILLISECONDS);
    spyProvider.clearAll();
    AuroraTopologyService.setExpireTime(AuroraTopologyService.DEFAULT_CACHE_EXPIRE_MS);
  }

  @Test
  public void testTopologyQuery() throws SQLException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final Statement mockStatement = Mockito.mock(StatementImpl.class);
    final ResultSet mockResultSet = Mockito.mock(ResultSetImpl.class);
    stubTopologyQuery(mockConn, mockStatement, mockResultSet);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final Properties mainHostProps = new Properties();
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, mainHostProps);
    final HostInfo mainHost = conStr.getMainHost();

    final HostInfo clusterInstanceInfo =
        new HostInfo(
            conStr,
            "?.XYZ.us-east-2.rds.amazonaws.com",
            mainHost.getPort(),
            mainHost.getUser(),
            mainHost.getPassword(),
            mainHost.isPasswordless(),
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

    final List<HostInfo> topology = spyProvider.getTopology(mockConn, false);

    final HostInfo master = topology.get(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX);
    final List<HostInfo> slaves =
        topology.subList(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX + 1, topology.size());

    assertEquals("writer-instance.XYZ.us-east-2.rds.amazonaws.com", master.getHost());
    assertEquals(1234, master.getPort());
    assertEquals("", master.getUser());
    assertEquals("", master.getPassword());
    assertTrue(master.isPasswordless());

    final Map<String, String> props = master.getHostProperties();
    assertEquals("writer-instance", props.get(TopologyServicePropertyKeys.INSTANCE_NAME));
    assertEquals(
        AuroraTopologyService.WRITER_SESSION_ID, props.get(TopologyServicePropertyKeys.SESSION_ID));
    assertEquals("2020-09-15 17:51:53.0", props.get(TopologyServicePropertyKeys.LAST_UPDATED));
    assertEquals("13.5", props.get(TopologyServicePropertyKeys.REPLICA_LAG));

    assertFalse(spyProvider.isMultiWriterCluster());
    assertEquals(3, topology.size());
    assertEquals(2, slaves.size());
  }

  @Test
  public void testTopologyQuery_MultiWriter() throws SQLException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final Statement mockStatement = Mockito.mock(StatementImpl.class);
    final ResultSet mockResultSet = Mockito.mock(ResultSetImpl.class);
    stubTopologyQueryMultiWriter(mockConn, mockStatement, mockResultSet);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final HostInfo mainHost = conStr.getMainHost();

    final HostInfo clusterInstanceInfo =
        new HostInfo(
            conStr,
            "?.XYZ.us-east-2.rds.amazonaws.com",
            mainHost.getPort(),
            mainHost.getUser(),
            mainHost.getPassword(),
            mainHost.isPasswordless(),
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

    final List<HostInfo> topology = spyProvider.getTopology(mockConn, false);
    final List<HostInfo> readers =
        topology.subList(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX + 1, topology.size());

    assertTrue(spyProvider.isMultiWriterCluster());
    assertEquals(3, topology.size());
    assertEquals(2, readers.size());

    final HostInfo master1 = topology.get(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX);
    final HostInfo master2 = topology.get(1);
    final HostInfo master3 = topology.get(2);

    assertEquals("writer-instance-1.XYZ.us-east-2.rds.amazonaws.com", master1.getHost());
    assertEquals(1234, master1.getPort());
    assertEquals("", master1.getUser());
    assertEquals("", master1.getPassword());
    assertTrue(master1.isPasswordless());

    assertEquals("writer-instance-2.XYZ.us-east-2.rds.amazonaws.com", master2.getHost());
    assertEquals(1234, master2.getPort());
    assertEquals("", master2.getUser());
    assertEquals("", master2.getPassword());
    assertTrue(master2.isPasswordless());

    assertEquals("writer-instance-3.XYZ.us-east-2.rds.amazonaws.com", master3.getHost());
    assertEquals(1234, master3.getPort());
    assertEquals("", master3.getUser());
    assertEquals("", master3.getPassword());
    assertTrue(master3.isPasswordless());

    Map<String, String> props1 = master1.getHostProperties();
    assertEquals("writer-instance-1", props1.get(TopologyServicePropertyKeys.INSTANCE_NAME));
    assertEquals(
        AuroraTopologyService.WRITER_SESSION_ID,
        props1.get(TopologyServicePropertyKeys.SESSION_ID));
    assertEquals("2020-09-15 17:51:53.0", props1.get(TopologyServicePropertyKeys.LAST_UPDATED));
    assertEquals("13.5", props1.get(TopologyServicePropertyKeys.REPLICA_LAG));

    Map<String, String> props2 = master2.getHostProperties();
    assertEquals("writer-instance-2", props2.get(TopologyServicePropertyKeys.INSTANCE_NAME));
    assertEquals(
        AuroraTopologyService.WRITER_SESSION_ID,
        props2.get(TopologyServicePropertyKeys.SESSION_ID));
    assertEquals("2020-09-15 17:51:53.0", props2.get(TopologyServicePropertyKeys.LAST_UPDATED));
    assertEquals("13.5", props2.get(TopologyServicePropertyKeys.REPLICA_LAG));

    Map<String, String> props3 = master3.getHostProperties();
    assertEquals("writer-instance-3", props3.get(TopologyServicePropertyKeys.INSTANCE_NAME));
    assertEquals(
        AuroraTopologyService.WRITER_SESSION_ID,
        props3.get(TopologyServicePropertyKeys.SESSION_ID));
    assertEquals("2020-09-15 17:51:53.0", props3.get(TopologyServicePropertyKeys.LAST_UPDATED));
    assertEquals("13.5", props3.get(TopologyServicePropertyKeys.REPLICA_LAG));
  }

  private void stubTopologyQuery(Connection conn, Statement stmt, ResultSet results)
      throws SQLException {
    stubTopologyQueryExecution(conn, stmt, results);
    stubTopologyResponseData(results);
  }

  private void stubTopologyQueryMultiWriter(Connection conn, Statement stmt, ResultSet results)
      throws SQLException {
    stubTopologyQueryExecution(conn, stmt, results);
    stubTopologyResponseDataMultiWriter(results);
  }

  private void stubTopologyQueryExecution(Connection conn, Statement stmt, ResultSet results)
      throws SQLException {
    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(AuroraTopologyService.RETRIEVE_TOPOLOGY_SQL)).thenReturn(results);
  }

  private void stubTopologyResponseData(ResultSet results) throws SQLException {
    when(results.next()).thenReturn(true, true, true, false);
    when(results.getString(AuroraTopologyService.FIELD_SESSION_ID))
        .thenReturn(
            "Replica",
            "Replica",
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID,
            "Replica",
            "Replica");
    when(results.getString(AuroraTopologyService.FIELD_SERVER_ID))
        .thenReturn(
            "replica-instance-1",
            "replica-instance-1",
            "writer-instance",
            "writer-instance",
            "replica-instance-2",
            "replica-instance-2");
    when(results.getTimestamp(AuroraTopologyService.FIELD_LAST_UPDATED))
        .thenReturn(Timestamp.valueOf("2020-09-15 17:51:53.0"));
    when(results.getDouble(AuroraTopologyService.FIELD_REPLICA_LAG)).thenReturn(13.5);
  }

  private void stubTopologyResponseDataMultiWriter(ResultSet results) throws SQLException {
    when(results.next()).thenReturn(true, true, true, false);
    when(results.getString(AuroraTopologyService.FIELD_SESSION_ID))
        .thenReturn(
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID);
    when(results.getString(AuroraTopologyService.FIELD_SERVER_ID))
        .thenReturn(
            "writer-instance-1",
            "writer-instance-1",
            "writer-instance-2",
            "writer-instance-2",
            "writer-instance-3",
            "writer-instance-3");
    when(results.getTimestamp(AuroraTopologyService.FIELD_LAST_UPDATED))
        .thenReturn(Timestamp.valueOf("2020-09-15 17:51:53.0"));
    when(results.getDouble(AuroraTopologyService.FIELD_REPLICA_LAG)).thenReturn(13.5);
  }

  @Test
  public void testCachedEntryRetrieved() throws SQLException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final Statement mockStatement = Mockito.mock(StatementImpl.class);
    final ResultSet mockResultSet = Mockito.mock(ResultSetImpl.class);
    stubTopologyQuery(mockConn, mockStatement, mockResultSet);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final HostInfo mainHost = conStr.getMainHost();
    final HostInfo clusterInstanceInfo =
        new HostInfo(
            conStr,
            "?.XYZ.us-east-2.rds.amazonaws.com",
            mainHost.getPort(),
            mainHost.getUser(),
            mainHost.getPassword(),
            mainHost.isPasswordless(),
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

    spyProvider.getTopology(mockConn, false);
    spyProvider.getTopology(mockConn, false);

    verify(spyProvider, times(1)).queryForTopology(mockConn);
  }

  @Test
  public void testForceUpdateQueryFailureWithSQLException() throws SQLException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final HostInfo mainHost = conStr.getMainHost();
    final HostInfo clusterInstanceInfo =
        new HostInfo(
            conStr,
            "?.XYZ.us-east-2.rds.amazonaws.com",
            mainHost.getPort(),
            mainHost.getUser(),
            mainHost.getPassword(),
            mainHost.isPasswordless(),
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);
    when(mockConn.createStatement()).thenThrow(SQLException.class);

    assertThrows(SQLException.class, () -> spyProvider.getTopology(mockConn, true));
  }

  @Test
  public void testQueryFailureReturnsStaleTopology() throws SQLException, InterruptedException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final Statement mockStatement = Mockito.mock(StatementImpl.class);
    final ResultSet mockResultSet = Mockito.mock(ResultSetImpl.class);
    stubTopologyQuery(mockConn, mockStatement, mockResultSet);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final HostInfo mainHost = conStr.getMainHost();
    final HostInfo clusterInstanceInfo =
        new HostInfo(
            conStr,
            "?.XYZ.us-east-2.rds.amazonaws.com",
            mainHost.getPort(),
            mainHost.getUser(),
            mainHost.getPassword(),
            mainHost.isPasswordless(),
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);
    spyProvider.setRefreshRate(1);

    final List<HostInfo> hosts = spyProvider.getTopology(mockConn, false);
    when(mockConn.createStatement()).thenThrow(SQLSyntaxErrorException.class);
    Thread.sleep(5);
    final List<HostInfo> staleHosts = spyProvider.getTopology(mockConn, false);

    verify(spyProvider, times(2)).queryForTopology(mockConn);
    assertEquals(3, staleHosts.size());
    assertEquals(hosts, staleHosts);
  }

  @Test
  public void testGetHostByName_success() throws SQLException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final Statement mockStatement = Mockito.mock(StatementImpl.class);
    final ResultSet mockResultSet = Mockito.mock(ResultSetImpl.class);
    stubTopologyQuery(mockConn, mockStatement, mockResultSet);

    // populate cache
    List<HostInfo> topology;
    topology = spyProvider.getTopology(mockConn, false);

    final String connectionHostName = "replica-instance-2";
    final int connectionHostIndex = 2;

    when(mockConn.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(AuroraTopologyService.GET_INSTANCE_NAME_SQL))
        .thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(AuroraTopologyService.GET_INSTANCE_NAME_COL))
        .thenReturn(connectionHostName);

    final HostInfo host = spyProvider.getHostByName(mockConn);
    assertEquals(topology.get(connectionHostIndex), host);
  }

  @Test
  public void testGetHostByName_noServerId() throws SQLException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final Statement mockStatement = Mockito.mock(StatementImpl.class);
    final ResultSet mockResultSet = Mockito.mock(ResultSetImpl.class);
    when(mockConn.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(AuroraTopologyService.GET_INSTANCE_NAME_SQL))
        .thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    final HostInfo host = spyProvider.getHostByName(mockConn);
    assertNull(host);
  }

  @Test
  public void testGetHostByName_exception() throws SQLException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final Statement mockStatement = Mockito.mock(StatementImpl.class);
    final ResultSet mockResultSet = Mockito.mock(ResultSetImpl.class);
    when(mockConn.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(AuroraTopologyService.GET_INSTANCE_NAME_SQL))
        .thenReturn(mockResultSet);
    when(mockResultSet.next()).thenThrow(SQLException.class);

    final HostInfo host = spyProvider.getHostByName(mockConn);
    assertNull(host);
  }

  @Test
  public void testProviderRefreshesTopology() throws SQLException, InterruptedException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final Statement mockStatement = Mockito.mock(StatementImpl.class);
    final ResultSet mockResultSet = Mockito.mock(ResultSetImpl.class);
    stubTopologyQuery(mockConn, mockStatement, mockResultSet);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final HostInfo mainHost = conStr.getMainHost();

    final HostInfo clusterInstanceInfo =
        new HostInfo(
            conStr,
            "?.XYZ.us-east-2.rds.amazonaws.com",
            mainHost.getPort(),
            mainHost.getUser(),
            mainHost.getPassword(),
            mainHost.isPasswordless(),
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

    spyProvider.setRefreshRate(1);
    spyProvider.getTopology(mockConn, false);
    Thread.sleep(2);
    spyProvider.getTopology(mockConn, false);

    verify(spyProvider, times(2)).queryForTopology(mockConn);
  }

  @Test
  public void testProviderTopologyExpires() throws SQLException, InterruptedException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final Statement mockStatement = Mockito.mock(StatementImpl.class);
    final ResultSet mockResultSet = Mockito.mock(ResultSetImpl.class);
    stubTopologyQuery(mockConn, mockStatement, mockResultSet);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final HostInfo mainHost = conStr.getMainHost();

    final HostInfo clusterInstanceInfo =
        new HostInfo(
            conStr,
            "?.XYZ.us-east-2.rds.amazonaws.com",
            mainHost.getPort(),
            mainHost.getUser(),
            mainHost.getPassword(),
            mainHost.isPasswordless(),
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

    AuroraTopologyService.setExpireTime(1000); // 1 sec
    spyProvider.setRefreshRate(
        10000); // 10 sec; and cache expiration time is also (indirectly) changed to 10 sec

    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(1)).queryForTopology(mockConn);

    Thread.sleep(3000);

    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(1)).queryForTopology(mockConn);

    Thread.sleep(3000);
    // internal cache has NOT expired yet
    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(1)).queryForTopology(mockConn);

    Thread.sleep(5000);
    // internal cache has expired by now
    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(2)).queryForTopology(mockConn);
  }

  @Test
  public void testProviderTopologyNotExpired() throws SQLException, InterruptedException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final Statement mockStatement = Mockito.mock(StatementImpl.class);
    final ResultSet mockResultSet = Mockito.mock(ResultSetImpl.class);
    stubTopologyQuery(mockConn, mockStatement, mockResultSet);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final HostInfo mainHost = conStr.getMainHost();

    final HostInfo clusterInstanceInfo =
        new HostInfo(
            conStr,
            "?.XYZ.us-east-2.rds.amazonaws.com",
            mainHost.getPort(),
            mainHost.getUser(),
            mainHost.getPassword(),
            mainHost.isPasswordless(),
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

    AuroraTopologyService.setExpireTime(10000); // 10 sec
    spyProvider.setRefreshRate(1000); // 1 sec

    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(1)).queryForTopology(mockConn);

    Thread.sleep(2000);

    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(2)).queryForTopology(mockConn);

    Thread.sleep(2000);

    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(3)).queryForTopology(mockConn);
  }

  @Test
  public void testClearProviderCache() throws SQLException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final Statement mockStatement = Mockito.mock(StatementImpl.class);
    final ResultSet mockResultSet = Mockito.mock(ResultSetImpl.class);
    stubTopologyQuery(mockConn, mockStatement, mockResultSet);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final HostInfo mainHost = conStr.getMainHost();

    final HostInfo clusterInstanceInfo =
        new HostInfo(
            conStr,
            "?.XYZ.us-east-2.rds.amazonaws.com",
            mainHost.getPort(),
            mainHost.getUser(),
            mainHost.getPassword(),
            mainHost.isPasswordless(),
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

    spyProvider.getTopology(mockConn, false);
    spyProvider.addToDownHostList(clusterInstanceInfo);
    assertEquals(1, AuroraTopologyService.topologyCache.size());

    spyProvider.clearAll();
    assertEquals(0, AuroraTopologyService.topologyCache.size());
  }
}
