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

package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPropertySet;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.jdbc.ha.plugins.failover.ITopologyService;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReadWriteSplittingPluginTest {
  @Mock private ICurrentConnectionProvider mockCurrentConnectionProvider;
  @Mock private ITopologyService mockTopologyService;
  @Mock private IConnectionProvider mockConnectionProvider;
  @Mock private IConnectionPlugin mockNextPlugin;
  @Mock private ConnectionImpl mockWriterConn;
  @Mock private ConnectionImpl mockNewWriterConn;
  @Mock private ConnectionImpl mockReaderConn;
  @Mock private Log mockLog;
  @Mock private ConnectionImpl mockClosedWriterConn;
  @Mock private Callable<?> mockCallable;

  private static final int WRITER_INDEX = 0;
  private final RdsHostUtils rdsHostUtils = new RdsHostUtils(mockLog);
  private final ConnectionUrl defaultConnUrl = getConnectionUrl(2);
  private final List<HostInfo> defaultHosts = new ArrayList<>(defaultConnUrl.getHostsList());
  private final HostInfo defaultWriterHost = defaultHosts.get(WRITER_INDEX);
  private final HostInfo defaultReaderHost = defaultHosts.get(WRITER_INDEX + 1);
  private final JdbcPropertySet defaultProps = new JdbcPropertySetImpl();
  private final SQLException failoverException =
          new SQLException("A mock failover exception was thrown", MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_CHANGED);
  private final List<HostInfo> newTopology;
  {
    List<HostInfo> defaultHostCopy = new ArrayList<>(defaultHosts);
    Collections.swap(defaultHostCopy, WRITER_INDEX, WRITER_INDEX + 1);
    this.newTopology = defaultHostCopy;
  }

  private AutoCloseable closeable;

  public ReadWriteSplittingPluginTest() throws SQLException {
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    mockDefaultBehavior();
  }

  void mockDefaultBehavior() throws SQLException {
    when(this.mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(mockWriterConn);
    when(this.mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultWriterHost);
    when(mockTopologyService.getHostByName(eq(mockWriterConn))).thenReturn(defaultWriterHost);
    when(mockTopologyService.getHostByName(eq(mockReaderConn))).thenReturn(defaultReaderHost);

    mockDefaultConnectionBehavior(mockWriterConn, defaultWriterHost);
    mockDefaultConnectionBehavior(mockReaderConn, defaultReaderHost);
    mockClosedConnectionBehavior(mockClosedWriterConn);
    mockConnectToReaderHosts(defaultHosts);
  }

  void mockDefaultConnectionBehavior(ConnectionImpl mockConn, HostInfo mockConnHost) throws SQLException {
    when(mockConn.getPropertySet()).thenReturn(defaultProps);
    when(mockConn.getHostPortPair()).thenReturn(mockConnHost.getHostPortPair());
    when(mockConn.isClosed()).thenReturn(false);
    when(mockConnectionProvider.connect(eq(mockConnHost))).thenReturn(mockConn);
  }

  void mockClosedConnectionBehavior(ConnectionImpl mockConn) {
    when(mockConn.getPropertySet()).thenReturn(defaultProps);
    when(mockConn.isClosed()).thenReturn(true);
  }

  @Test
  public void testHostInfoStored() throws SQLException {
    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    assertEquals(2, plugin.getHosts().size());
  }

  @Test
  public void testSetReadOnly_trueFalse_threeHosts() throws SQLException {
    when(mockCurrentConnectionProvider.getCurrentConnection())
            .thenReturn(mockWriterConn, mockWriterConn, mockWriterConn, mockReaderConn);

    final ConnectionUrl connUrl = getConnectionUrl(3);
    mockConnectToReaderHosts(connUrl.getHostsList());
    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(connUrl);
    final HostInfo writerHost = plugin.getHosts().get(WRITER_INDEX);

    when(mockConnectionProvider.connect(eq(writerHost))).thenReturn(mockWriterConn);
    when(mockReaderConn.getHostPortPair()).thenReturn("instance-3:3306");

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(writerHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());

    plugin.switchConnectionIfRequired(false);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(writerHost)));
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockWriterConn), eq(writerHost));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
  }

  @Test
  public void testSetReadOnly_falseInTransaction() throws SQLException {
    when(mockCurrentConnectionProvider.getCurrentConnection())
            .thenReturn(mockWriterConn, mockWriterConn, mockWriterConn, mockReaderConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(defaultWriterHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());

    plugin.transactionBegun();
    final SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(false));
    assertEquals(MysqlErrorNumbers.SQL_STATE_ACTIVE_SQL_TRANSACTION, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(defaultWriterHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
  }

  @Test
  public void testSetReadOnly_trueTrue() throws SQLException {
    when(mockCurrentConnectionProvider.getCurrentConnection())
            .thenReturn(mockWriterConn, mockWriterConn, mockWriterConn, mockReaderConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(defaultWriterHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(defaultWriterHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
  }

  @Test
  public void testSetReadOnly_false() throws SQLException {
    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    plugin.switchConnectionIfRequired(false);
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testSetReadOnly_true_oneHost() throws SQLException {
    final ConnectionUrl connUrl = getConnectionUrl(1);
    final HostInfo writerHost = connUrl.getMainHost();

    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(writerHost);
    when(mockConnectionProvider.connect(writerHost)).thenReturn(mockWriterConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(connUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertEquals(mockWriterConn, plugin.getReaderConnection());
  }

  @Test
  public void testSetReadOnly_true_oneHost_writerClosed() throws SQLException {
    final ConnectionUrl connUrl = getConnectionUrl(1);
    final HostInfo writerHost = connUrl.getMainHost();

    when(mockConnectionProvider.connect(writerHost)).thenReturn(mockWriterConn);
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(writerHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
            mockClosedWriterConn, mockClosedWriterConn, mockWriterConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(connUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockWriterConn), eq(writerHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(not(eq(mockWriterConn)), eq(writerHost));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertEquals(mockWriterConn, plugin.getReaderConnection());
  }

  @Test
  public void testSetReadOnly_true() throws SQLException {
    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(defaultReaderHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
  }

  @Test
  public void testSetReadOnly_true_readerConnectionFailed() throws SQLException {
    when(mockConnectionProvider.connect(eq(defaultReaderHost))).thenThrow(SQLException.class);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertEquals(mockWriterConn, plugin.getReaderConnection());
  }

  @Test
  public void testSetReadOnly_true_noReaderHostMatch() throws SQLException {
    when(mockCurrentConnectionProvider.getCurrentConnection())
            .thenReturn(mockWriterConn, mockWriterConn, mockWriterConn, mockReaderConn, mockWriterConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);
    final List<HostInfo> hosts = plugin.getHosts();

    plugin.switchConnectionIfRequired(true);
    plugin.switchConnectionIfRequired(false);
    hosts.remove(WRITER_INDEX + 1);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(defaultReaderHost));
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockWriterConn), eq(defaultWriterHost));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(defaultReaderHost));
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockWriterConn), eq(defaultWriterHost));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
  }

  @Test
  public void testSetReadOnly_true_noReaderHostMatch_writerClosed() throws SQLException {
    when(mockCurrentConnectionProvider.getCurrentConnection())
            .thenReturn(mockClosedWriterConn, mockClosedWriterConn, mockClosedWriterConn, mockReaderConn, mockClosedWriterConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);
    final List<HostInfo> hosts = plugin.getHosts();

    plugin.switchConnectionIfRequired(true);
    plugin.switchConnectionIfRequired(false);
    hosts.remove(WRITER_INDEX + 1);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(defaultReaderHost));
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockWriterConn), eq(defaultWriterHost));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());

    final SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(true));
    assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(defaultReaderHost));
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockWriterConn), eq(defaultWriterHost));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
  }

  @Test
  public void testSetReadOnly_false_writerConnectionFails() throws SQLException {
    when(mockConnectionProvider.connect(eq(defaultWriterHost))).thenThrow(SQLException.class);
    when(mockCurrentConnectionProvider.getCurrentConnection())
            .thenReturn(mockClosedWriterConn, mockClosedWriterConn, mockClosedWriterConn, mockReaderConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(defaultReaderHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockClosedWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockClosedWriterConn, plugin.getWriterConnection());

    final SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(false));
    assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(defaultReaderHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockClosedWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockClosedWriterConn, plugin.getWriterConnection());
  }

  @Test
  public void testSetReadOnly_true_readerConnectionFails_writerClosed() throws SQLException {
    when(mockConnectionProvider.connect(eq(defaultReaderHost))).thenThrow(SQLException.class);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(mockClosedWriterConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    final SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(true));
    assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertNull(plugin.getReaderConnection());
    assertEquals(mockClosedWriterConn, plugin.getWriterConnection());
  }

  @Test
  public void testClusterSettings() throws SQLException {
    final String url = "jdbc:mysql:aws://10.10.10.10/test";
    String clusterId = "test-cluster-id";
    final Properties properties = getDefaultProperties();
    properties.setProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), "?.my-custom-domain.com:3306");
    properties.setProperty(PropertyKey.clusterId.getKeyName(), clusterId);
    final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, properties);
    final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
    propertySet.initializeProperties(properties);

    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultReaderHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(mockReaderConn);
    when(mockTopologyService.getTopology(eq(mockReaderConn), eq(false))).thenReturn(defaultHosts);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(propertySet);
    plugin.openInitialConnection(connUrl);

    verify(mockTopologyService, times(1)).getTopology(eq(mockReaderConn), eq(false));
    verify(mockTopologyService, times(1)).setClusterId(clusterId);
    assertNull(plugin.getWriterConnection());
    assertEquals(mockReaderConn, plugin.getReaderConnection());
  }

  @Test
  public void testHostPatternRequired() throws SQLException {
    final String url = "jdbc:mysql:aws://10.10.10.10/test";
    final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, getDefaultProperties());

    when(mockTopologyService.getTopology(eq(mockWriterConn), eq(false))).thenReturn(defaultHosts);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);

    assertThrows(SQLException.class, () -> plugin.openInitialConnection(connUrl));
    verify(mockTopologyService, times(1)).getTopology(eq(mockWriterConn), eq(false));
    assertNull(plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testExecute_failoverToNewWriter() throws Exception {
    final String url = "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com";
    final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, getDefaultProperties());
    final HostInfo mainHost = connUrl.getMainHost();

    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(mainHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(mockWriterConn);
    when(mockTopologyService.getTopology(eq(mockWriterConn), eq(false))).thenReturn(defaultHosts);
    when(mockTopologyService.getHostByName(eq(mockWriterConn))).thenReturn(defaultWriterHost);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(connUrl);

    verify(mockTopologyService, times(1)).getTopology(eq(mockWriterConn), eq(false));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());

    when(mockNextPlugin.execute(any(), any(), any(), any())).thenThrow(failoverException);
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(newTopology.get(WRITER_INDEX));
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(mockNewWriterConn);
    when(mockTopologyService.getTopology(eq(mockNewWriterConn), eq(false))).thenReturn(newTopology);

    assertThrows(SQLException.class, () -> plugin.execute(JdbcConnection.class, "createStatement", mockCallable, new Object[] {}));
    verify(mockTopologyService, times(1)).getTopology(eq(mockNewWriterConn), eq(false));
    assertEquals(newTopology, plugin.getHosts());
    assertEquals(mockNewWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testExecute_failoverToNewWriter_getTopologyFails() throws Exception {
    final String url = "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com";
    final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, getDefaultProperties());
    final HostInfo mainHost = connUrl.getMainHost();

    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(mainHost);
    when(mockTopologyService.getTopology(eq(mockWriterConn), eq(false))).thenReturn(defaultHosts);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(connUrl);

    verify(mockTopologyService, times(1)).getTopology(eq(mockWriterConn), eq(false));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());

    when(mockNextPlugin.execute(any(), any(), any(), any())).thenThrow(failoverException);
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(newTopology.get(WRITER_INDEX));
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(mockNewWriterConn);
    when(mockTopologyService.getTopology(eq(mockNewWriterConn), eq(false))).thenThrow(SQLException.class);

    assertThrows(SQLException.class, () -> plugin.execute(JdbcConnection.class, "createStatement", mockCallable, new Object[] {}));
    verify(mockTopologyService, times(1)).getTopology(eq(mockNewWriterConn), eq(false));
    assertEquals(newTopology, plugin.getHosts());
    assertEquals(mockNewWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());
  }

  @Test
  public void testReaderLoadBalancing_autocommitTrue() throws Exception {
    final String url = "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com?" +
            "loadBalanceReadOnlyTraffic=true";
    final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final List<HostInfo> hosts = getConnectionUrl(5).getHostsList();
    final HostInfo writerHost = hosts.get(WRITER_INDEX);

    when(mockCurrentConnectionProvider.getCurrentConnection())
            .thenReturn(mockWriterConn, mockWriterConn, mockWriterConn, mockReaderConn);
    when(mockTopologyService.getTopology(eq(mockWriterConn), eq(false))).thenReturn(hosts);
    when(mockTopologyService.getTopology(eq(mockReaderConn), eq(false))).thenReturn(hosts);
    when(this.mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(connUrl.getMainHost(), hosts.get(1), hosts.get(1));
    when(mockReaderConn.getAutoCommit()).thenReturn(true);
    mockConnectToReaderHosts(hosts);

    JdbcPropertySetImpl props = new JdbcPropertySetImpl();
    props.initializeProperties(connUrl.getConnectionArgumentsAsProperties());
    final ReadWriteSplittingPlugin plugin = Mockito.spy(initReadWriteSplittingPlugin(props));
    plugin.openInitialConnection(connUrl);

    verify(mockTopologyService, times(1)).getTopology(eq(mockWriterConn), eq(false));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(writerHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());

    plugin.execute(Statement.class, "execute", mockCallable, new Object[] {});
    verify(plugin, times(0)).pickNewReaderConnection();
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));

    plugin.execute(Statement.class, "executeQuery", mockCallable, new Object[] {});
    verify(plugin, times(1)).pickNewReaderConnection();
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));

    plugin.execute(Statement.class, "getAutoCommit", mockCallable, new Object[] {});
    verify(plugin, times(2)).pickNewReaderConnection();
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));

    plugin.execute(Statement.class, "getAutoCommit", mockCallable, new Object[] {});
    verify(plugin, times(2)).pickNewReaderConnection();
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
  }

  @Test
  public void testReaderLoadBalancing_autocommitFalse() throws Exception {
    final String url = "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com?" +
            "loadBalanceReadOnlyTraffic=true";
    final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final List<HostInfo> hosts = getConnectionUrl(5).getHostsList();
    final HostInfo writerHost = hosts.get(WRITER_INDEX);

    when(mockCurrentConnectionProvider.getCurrentConnection())
            .thenReturn(mockWriterConn, mockWriterConn, mockWriterConn, mockReaderConn);
    when(mockTopologyService.getTopology(eq(mockWriterConn), eq(false))).thenReturn(hosts);
    when(mockTopologyService.getTopology(eq(mockReaderConn), eq(false))).thenReturn(hosts);
    when(this.mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(connUrl.getMainHost(), hosts.get(1), hosts.get(1));
    mockConnectToReaderHosts(hosts);

    JdbcPropertySetImpl props = new JdbcPropertySetImpl();
    props.initializeProperties(connUrl.getConnectionArgumentsAsProperties());
    final ReadWriteSplittingPlugin plugin = Mockito.spy(initReadWriteSplittingPlugin(props));
    plugin.openInitialConnection(connUrl);

    verify(mockTopologyService, times(1)).getTopology(eq(mockWriterConn), eq(false));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(writerHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());

    plugin.execute(JdbcConnection.class, "setAutoCommit", mockCallable, new Object[] { false });
    plugin.execute(JdbcConnection.class, "commit", mockCallable, new Object[] {});
    verify(plugin, times(0)).pickNewReaderConnection();
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));

    plugin.execute(JdbcConnection.class, "rollback", mockCallable, new Object[] {});
    verify(plugin, times(1)).pickNewReaderConnection();
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));

    plugin.execute(Statement.class, "execute", mockCallable, new Object[] {});
    verify(plugin, times(2)).pickNewReaderConnection();
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));

    plugin.execute(Statement.class, "execute", mockCallable, new Object[] {});
    verify(plugin, times(2)).pickNewReaderConnection();
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
  }

  @Test
  public void testReaderLoadBalancing_autocommitFalseSqlStatement() throws Exception {
    final String url = "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com?" +
            "loadBalanceReadOnlyTraffic=true";
    final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final List<HostInfo> hosts = getConnectionUrl(5).getHostsList();
    final HostInfo writerHost = hosts.get(WRITER_INDEX);

    when(mockCurrentConnectionProvider.getCurrentConnection())
            .thenReturn(mockWriterConn, mockWriterConn, mockWriterConn, mockReaderConn);
    when(mockTopologyService.getTopology(eq(mockWriterConn), eq(false))).thenReturn(hosts);
    when(mockTopologyService.getTopology(eq(mockReaderConn), eq(false))).thenReturn(hosts);
    when(this.mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(connUrl.getMainHost(), hosts.get(1), hosts.get(1));
    mockConnectToReaderHosts(hosts);

    JdbcPropertySetImpl props = new JdbcPropertySetImpl();
    props.initializeProperties(connUrl.getConnectionArgumentsAsProperties());
    final ReadWriteSplittingPlugin plugin = Mockito.spy(initReadWriteSplittingPlugin(props));
    plugin.openInitialConnection(connUrl);

    verify(mockTopologyService, times(1)).getTopology(eq(mockWriterConn), eq(false));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(writerHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());

    plugin.execute(JdbcConnection.class, "executeUpdate", mockCallable, new Object[] { "SeT AutoCommiT = FalsE" });
    plugin.execute(JdbcConnection.class, "execute", mockCallable, new Object[] { "CoMmIt" });
    verify(plugin, times(0)).pickNewReaderConnection();
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));

    plugin.execute(JdbcConnection.class, "executeUpdate", mockCallable, new Object[] { "RoLlBaCk" });
    verify(plugin, times(1)).pickNewReaderConnection();
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));

    plugin.execute(Statement.class, "execute", mockCallable, new Object[] {});
    verify(plugin, times(2)).pickNewReaderConnection();
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));

    plugin.execute(Statement.class, "execute", mockCallable, new Object[] {});
    verify(plugin, times(2)).pickNewReaderConnection();
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
  }

  private ReadWriteSplittingPlugin initReadWriteSplittingPlugin(JdbcPropertySet props) {
    return new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockTopologyService,
        mockConnectionProvider,
        rdsHostUtils,
        props,
        mockNextPlugin,
        mockLog);
  }

  private Properties getDefaultProperties() {
    Properties props = new Properties();
    props.setProperty(PropertyKey.connectionPluginFactories.getKeyName(),
            "com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory");
    return props;
  }

  private ConnectionUrl getConnectionUrl(int numHosts) throws SQLException {
    String hosts = "";
    for (int hostNum = 1; hostNum <= numHosts; hostNum++) {
      hosts += "instance-" + hostNum;
      if(hostNum != numHosts) {
        hosts += ",";
      }
    }
    String url = "jdbc:mysql:aws://" + hosts + "/test";
    return ConnectionUrl.getConnectionUrlInstance(url, getDefaultProperties());
  }

  private void mockConnectToReaderHosts(List<HostInfo> hosts) throws SQLException {
    for (int i = 1; i < hosts.size(); i++) {
      when(mockConnectionProvider.connect(eq(hosts.get(i)))).thenReturn(mockReaderConn);
    }
  }
}
