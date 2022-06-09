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

package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcPropertySet;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.jdbc.ha.plugins.failover.ITopologyService;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReadWriteSplittingPluginTest {
  private static final int WRITER_INDEX = 0;
  @Mock private ICurrentConnectionProvider mockCurrentConnectionProvider;
  @Mock private ITopologyService mockTopologyService;
  @Mock private IConnectionProvider mockConnectionProvider;
  @Mock private IConnectionPlugin mockNextPlugin;
  @Mock private ConnectionImpl mockWriterConn;
  @Mock private ConnectionImpl mockReaderConn;
  @Mock private Log mockLog;
  @Mock private ConnectionImpl mockClosedWriterConn;
  private final RdsHostUtils rdsHostUtils = new RdsHostUtils(mockLog); 
  private static final String defaultUrl = "jdbc:mysql:aws://writer,reader1/test?" +
      "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
  private static final ConnectionUrl defaultConnUrl =
      ConnectionUrl.getConnectionUrlInstance(defaultUrl, new Properties());
  private static final List<HostInfo> defaultHosts = defaultConnUrl.getHostsList();
  private static final HostInfo defaultWriterHost = defaultHosts.get(WRITER_INDEX);
  private static final HostInfo defaultReaderHost = defaultHosts.get(WRITER_INDEX + 1);
  private final JdbcPropertySet defaultProps = new JdbcPropertySetImpl();
  private AutoCloseable closeable;

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    mockDefaultConnectionBehavior(mockWriterConn, defaultWriterHost);
    mockDefaultConnectionBehavior(mockReaderConn, defaultReaderHost);
    mockClosedConnectionBehavior(mockClosedWriterConn);
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
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(mockWriterConn);

    final ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        defaultProps,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(defaultConnUrl);

    assertEquals(2, plugin.getHosts().size());
  }

  @Test
  public void testSetReadOnly_trueFalse_threeHosts() throws SQLException {
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultWriterHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn, mockReaderConn);

    final String url = "jdbc:mysql:aws://writer,reader1,reader2/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(connUrl);
    final HostInfo writerHost = plugin.getHosts().get(WRITER_INDEX);

    when(mockConnectionProvider.connect(not(eq(writerHost)))).thenReturn(mockReaderConn);
    when(mockConnectionProvider.connect(eq(writerHost))).thenReturn(mockWriterConn);
    when(mockReaderConn.getHostPortPair()).thenReturn("reader2:3306");

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(writerHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());

    plugin.switchConnectionIfRequired(false);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(writerHost)));
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockWriterConn), eq(writerHost));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertFalse(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_falseInTransaction() throws SQLException {
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultWriterHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn, mockReaderConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(defaultWriterHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());

    plugin.transactionBegun();
    final SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(false));
    assertEquals(MysqlErrorNumbers.SQL_STATE_ACTIVE_SQL_TRANSACTION, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(defaultWriterHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_trueTrue() throws SQLException {
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultWriterHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(defaultWriterHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(defaultWriterHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_false() throws SQLException {
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    plugin.switchConnectionIfRequired(false);
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());
    assertFalse(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_zeroHosts() throws SQLException {
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultWriterHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);
    final List<HostInfo> hosts = plugin.getHosts();
    hosts.clear();

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertEquals(mockWriterConn, plugin.getReaderConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_zeroHosts_writerClosed() throws SQLException {
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultWriterHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn);
    when(mockWriterConn.isClosed()).thenReturn(true);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);
    final List<HostInfo> hosts = plugin.getHosts();
    hosts.clear();

    final SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(true));
    assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());
    assertFalse(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_oneHost() throws SQLException {
    final String url = "jdbc:mysql:aws://writer/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final HostInfo writerHost = connUrl.getMainHost();

    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(writerHost);
    when(mockConnectionProvider.connect(writerHost)).thenReturn(mockWriterConn);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(connUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertEquals(mockWriterConn, plugin.getReaderConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_oneHost_writerClosed() throws SQLException {
    final String url = "jdbc:mysql:aws://writer/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final HostInfo writerHost = connUrl.getMainHost();

    when(mockConnectionProvider.connect(writerHost)).thenReturn(mockWriterConn);
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(writerHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockClosedWriterConn,
        mockClosedWriterConn, mockClosedWriterConn, mockClosedWriterConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(connUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockWriterConn), eq(writerHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(not(eq(mockWriterConn)), eq(writerHost));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertEquals(mockWriterConn, plugin.getReaderConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true() throws SQLException {
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultWriterHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(defaultReaderHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_readerConnectionFailed() throws SQLException {
    when(mockConnectionProvider.connect(eq(defaultReaderHost))).thenThrow(SQLException.class);
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultWriterHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertEquals(mockWriterConn, plugin.getReaderConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_noReaderHostMatch() throws SQLException {
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn, mockReaderConn,
        mockWriterConn, mockWriterConn, mockWriterConn);

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
    assertEquals(mockWriterConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_noReaderHostMatch_writerClosed() throws SQLException {
    when(mockWriterConn.isClosed()).thenReturn(true);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn, mockReaderConn,
        mockWriterConn, mockWriterConn, mockWriterConn);

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
    assertFalse(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_trueFalse_zeroHosts() throws SQLException {
    when(mockWriterConn.isClosed()).thenReturn(true);
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultWriterHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);
    final List<HostInfo> hosts = plugin.getHosts();

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(defaultReaderHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());

    hosts.clear();
    final SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(false));
    assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(defaultReaderHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_false_writerConnectionFails() throws SQLException {
    when(mockConnectionProvider.connect(eq(defaultWriterHost))).thenThrow(SQLException.class);
    when(mockWriterConn.isClosed()).thenReturn(true);
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultWriterHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(defaultReaderHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());

    final SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(false));
    assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(defaultReaderHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_readerConnectionFails_writerClosed() throws SQLException {
    when(mockConnectionProvider.connect(eq(defaultReaderHost))).thenThrow(SQLException.class);
    when(mockWriterConn.isClosed()).thenReturn(true);
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultWriterHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);
    plugin.openInitialConnection(defaultConnUrl);

    final SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(true));
    assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertNull(plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertFalse(plugin.getReadOnly());
  }

  @Test
  public void testClusterSettings() throws SQLException {
    final String url = "jdbc:mysql:aws://10.10.10.10/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    String clusterId = "test-cluster-id";
    final Properties properties = new Properties();
    properties.setProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), "?.my-custom-domain.com:3306");
    properties.setProperty(PropertyKey.clusterId.getKeyName(), clusterId);
    final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, properties);
    final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
    propertySet.initializeProperties(properties);

    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultReaderHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(mockReaderConn);
    when(mockTopologyService.getTopology(eq(mockReaderConn), eq(false))).thenReturn(defaultHosts);
    when(mockTopologyService.getHostByName(eq(mockReaderConn))).thenReturn(defaultReaderHost);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(propertySet);
    plugin.openInitialConnection(connUrl);

    verify(mockTopologyService, times(1)).getTopology(eq(mockReaderConn), eq(false));
    verify(mockTopologyService, times(1)).setClusterId(clusterId);
    assertNull(plugin.getWriterConnection());
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertFalse(plugin.getReadOnly());
  }

  @Test
  public void testHostPatternRequired() throws SQLException {
    final String url = "jdbc:mysql:aws://10.10.10.10/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());

    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(defaultWriterHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(mockWriterConn);
    when(mockTopologyService.getTopology(eq(mockWriterConn), eq(false))).thenReturn(defaultHosts);
    when(mockTopologyService.getHostByName(eq(mockWriterConn))).thenReturn(defaultWriterHost);

    final ReadWriteSplittingPlugin plugin = initReadWriteSplittingPlugin(defaultProps);

    assertThrows(SQLException.class, () -> plugin.openInitialConnection(connUrl));
    verify(mockTopologyService, times(1)).getTopology(eq(mockWriterConn), eq(false));
    assertNull(plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());
    assertFalse(plugin.getReadOnly());
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
}
