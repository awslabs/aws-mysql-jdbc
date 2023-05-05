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
import com.mysql.cj.exceptions.WrongArgumentException;
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
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
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
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

    final List<HostInfo> topology = spyProvider.getTopology(mockConn, false);

    final HostInfo master = topology.get(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX);
    final List<HostInfo> replicas =
        topology.subList(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX + 1, topology.size());

    assertEquals("writer-instance.XYZ.us-east-2.rds.amazonaws.com", master.getHost());
    assertEquals(1234, master.getPort());
    assertNull(master.getUser());
    assertNull(master.getPassword());

    final Map<String, String> props = master.getHostProperties();
    assertEquals("writer-instance", props.get(TopologyServicePropertyKeys.INSTANCE_NAME));
    assertEquals(AuroraTopologyService.WRITER_SESSION_ID, props.get(TopologyServicePropertyKeys.SESSION_ID));
    assertEquals("2020-09-15 17:51:53.0", props.get(TopologyServicePropertyKeys.LAST_UPDATED));
    assertEquals("13.5", props.get(TopologyServicePropertyKeys.REPLICA_LAG));

    assertEquals(3, topology.size());
    assertEquals(2, replicas.size());
  }

  @Test
  public void testTopologyQuery_StaleRecord() throws SQLException {
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
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

    // Set up topology cache as though there was an initial successful connection
    spyProvider.getTopology(mockConn, false);

    // If the failover bug where multiple writers would appear in the topology after writer failover, verify the
    // topology service handles it correctly.
    stubTopologyQueryStaleRecord(mockConn, mockStatement, mockResultSet);
    List<HostInfo> topology =  spyProvider.getTopology(mockConn, true);
    final HostInfo master = topology.get(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX);

    final List<HostInfo> replicas =
        topology.subList(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX + 1, topology.size());

    assertEquals("writer-instance-1.XYZ.us-east-2.rds.amazonaws.com", master.getHost());
    assertEquals(1234, master.getPort());
    assertNull(master.getUser());
    assertNull(master.getPassword());

    final Map<String, String> props = master.getHostProperties();
    assertEquals("writer-instance-1", props.get(TopologyServicePropertyKeys.INSTANCE_NAME));
    assertEquals(AuroraTopologyService.WRITER_SESSION_ID, props.get(TopologyServicePropertyKeys.SESSION_ID));
    assertEquals("2020-09-15 17:51:53.0", props.get(TopologyServicePropertyKeys.LAST_UPDATED));
    assertEquals("13.5", props.get(TopologyServicePropertyKeys.REPLICA_LAG));

    assertEquals(2, topology.size());
    assertEquals(1, replicas.size());
  }

  @Test
  public void testTopologyQuery_MultiWriter() throws SQLException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final Statement mockStatement = Mockito.mock(StatementImpl.class);
    final ResultSet mockResultSet = Mockito.mock(ResultSetImpl.class);
    stubTopologyQueryStaleRecord(mockConn, mockStatement, mockResultSet);
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
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

    final List<HostInfo> topology = spyProvider.getTopology(mockConn, false);
    assertNull(topology);
  }

  private void stubTopologyQuery(Connection conn, Statement stmt, ResultSet results)
      throws SQLException {
    stubTopologyQueryExecution(conn, stmt, results);
    stubTopologyResponseData(results);
  }

  private void stubTopologyQueryStaleRecord(Connection conn, Statement stmt, ResultSet results)
      throws SQLException {
    stubTopologyQueryExecution(conn, stmt, results);
    stubTopologyResponseDataStaleRecord(results);
  }

  private void stubTopologyQueryWithInvalidLastUpdatedTimestamp(Connection conn, Statement stmt, ResultSet results)
      throws SQLException {
    stubTopologyQueryExecution(conn, stmt, results);
    stubTopologyResponseDataWithInvalidLastUpdatedTimestamp(results);
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

  private void stubTopologyResponseDataStaleRecord(ResultSet results) throws SQLException {
    when(results.next()).thenReturn(true, true, true, false);
    when(results.getString(AuroraTopologyService.FIELD_SESSION_ID))
        .thenReturn(
            "Replica",
            "Replica",
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID);
    when(results.getString(AuroraTopologyService.FIELD_SERVER_ID))
        .thenReturn(
            "replica-instance-1",
            "replica-instance-1",
            "writer-instance",
            "writer-instance",
            "writer-instance-1",
            "writer-instance-1");
    when(results.getTimestamp(AuroraTopologyService.FIELD_LAST_UPDATED))
        .thenReturn(Timestamp.valueOf("2020-09-15 17:51:53.0"));
    when(results.getDouble(AuroraTopologyService.FIELD_REPLICA_LAG)).thenReturn(13.5);
  }

  private void stubTopologyResponseDataWithInvalidLastUpdatedTimestamp(ResultSet results) throws SQLException {
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
        .thenThrow(WrongArgumentException.class);

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
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);
    when(mockConn.createStatement()).thenThrow(SQLException.class);

    assertThrows(SQLException.class, () -> spyProvider.getTopology(mockConn, true));
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
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

    spyProvider.setRefreshRate(1000); // 1 sec

    spyProvider.getTopology(mockConn, false); // this call should be filling cache
    spyProvider.getTopology(mockConn, false); // this call should use data in cache
    verify(spyProvider, times(1)).queryForTopology(mockConn);
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
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

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
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

    spyProvider.getTopology(mockConn, false);
    spyProvider.addToDownHostList(clusterInstanceInfo);
    assertEquals(1, AuroraTopologyService.downHostCache.size());

    spyProvider.clearAll();
    assertEquals(0, AuroraTopologyService.downHostCache.size());
  }

  @Test
  public void testTopologyQueryGivenInvalidLastUpdatedTimestamp() throws SQLException {
    final JdbcConnection mockConn = Mockito.mock(ConnectionImpl.class);
    final Statement mockStatement = Mockito.mock(StatementImpl.class);
    final ResultSet mockResultSet = Mockito.mock(ResultSetImpl.class);
    stubTopologyQueryWithInvalidLastUpdatedTimestamp(mockConn, mockStatement, mockResultSet);
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
            mainHost.getHostProperties());
    spyProvider.setClusterInstanceTemplate(clusterInstanceInfo);

    final List<HostInfo> topology = spyProvider.getTopology(mockConn, false);

    final HostInfo master = topology.get(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX);
    final List<HostInfo> replicas =
        topology.subList(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX + 1, topology.size());

    final Map<String, String> props = master.getHostProperties();
    // Expected and actual lastUpdated timestamp are rounded since mocking the lastUpdated timestamp is difficult
    final String expectedLastUpdatedTimeStampRounded = Timestamp.from(Instant.now()).toString().substring(0,16);
    final String actualLastUpdatedTimeStampRounded = props.get(TopologyServicePropertyKeys.LAST_UPDATED).substring(0,16);
    assertEquals(expectedLastUpdatedTimeStampRounded, actualLastUpdatedTimeStampRounded);
  }
}
