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
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcPropertySet;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
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
  @Mock ICurrentConnectionProvider mockCurrentConnectionProvider;
  @Mock IConnectionProvider mockConnectionProvider;
  @Mock IConnectionPlugin mockNextPlugin;
  @Mock ConnectionImpl mockWriterConn;
  @Mock ConnectionImpl mockReaderConn;
  @Mock Log mockLog;
  @Mock ConnectionImpl mockClosedWriterConn;
  @Mock ConnectionImpl mockNewWriterConn;
  private AutoCloseable closeable;

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testHostInfoStored() throws SQLException {
    String url = "jdbc:mysql:aws://writer,reader1,reader2/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);

    assertEquals(3, plugin.getHosts().size());
  }

  @Test
  public void testSetReadOnly_trueFalse() throws SQLException {
    String url = "jdbc:mysql:aws://writer,reader1,reader2/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn, mockReaderConn);
    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);
    HostInfo writerHost = plugin.getHosts().get(WRITER_INDEX);

    when(mockConnectionProvider.connect(not(eq(writerHost)))).thenReturn(mockReaderConn);
    when(mockConnectionProvider.connect(eq(writerHost))).thenReturn(mockWriterConn);
    when(mockReaderConn.isClosed()).thenReturn(false);
    when(mockWriterConn.getPropertySet()).thenReturn(props);
    when(mockReaderConn.getHostPortPair()).thenReturn("reader2:3306");
    when(mockReaderConn.getPropertySet()).thenReturn(props);

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
    String url = "jdbc:mysql:aws://writer,reader1,reader2/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn, mockReaderConn);
    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);
    HostInfo writerHost = plugin.getHosts().get(WRITER_INDEX);

    when(mockConnectionProvider.connect(not(eq(writerHost)))).thenReturn(mockReaderConn);
    when(mockConnectionProvider.connect(eq(writerHost))).thenReturn(mockWriterConn);
    when(mockReaderConn.isClosed()).thenReturn(false);
    when(mockWriterConn.getPropertySet()).thenReturn(props);
    when(mockReaderConn.getHostPortPair()).thenReturn("reader2:3306");
    when(mockReaderConn.getPropertySet()).thenReturn(props);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(writerHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());

    plugin.transactionBegun();
    SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(false));
    assertEquals(MysqlErrorNumbers.SQL_STATE_ACTIVE_SQL_TRANSACTION, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(writerHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_trueTrue() throws SQLException {
    String url = "jdbc:mysql:aws://writer,reader1,reader2/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn);
    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);
    HostInfo writerHost = plugin.getHosts().get(WRITER_INDEX);

    when(mockConnectionProvider.connect(not(eq(writerHost)))).thenReturn(mockReaderConn);
    when(mockReaderConn.isClosed()).thenReturn(false);
    when(mockWriterConn.getPropertySet()).thenReturn(props);
    when(mockReaderConn.getHostPortPair()).thenReturn("reader1:3306");

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(writerHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), not(eq(writerHost)));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_false() throws SQLException {
    String url = "jdbc:mysql:aws://writer,reader1,reader2/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn);
    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);

    plugin.switchConnectionIfRequired(false);
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());
    assertFalse(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_zeroHosts() throws SQLException {
    String url = "jdbc:mysql:aws:///test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn);
    when(mockReaderConn.isClosed()).thenReturn(false);

    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);
    List<HostInfo> hosts = plugin.getHosts();
    hosts.clear();

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertEquals(mockWriterConn, plugin.getReaderConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_zeroHosts_writerClosed() throws SQLException {
    String url = "jdbc:mysql:aws:///test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn);
    when(mockWriterConn.isClosed()).thenReturn(true);

    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);
    List<HostInfo> hosts = plugin.getHosts();
    hosts.clear();

    SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(true));
    assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertNull(plugin.getReaderConnection());
    assertFalse(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_oneHost() throws SQLException {
    String url = "jdbc:mysql:aws://writer/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn);
    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertEquals(mockWriterConn, plugin.getReaderConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_oneHost_writerClosed() throws SQLException {
    String url = "jdbc:mysql:aws://writer/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockClosedWriterConn,
        mockClosedWriterConn, mockClosedWriterConn, mockClosedWriterConn);
    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);
    HostInfo writerHost = plugin.getHosts().get(WRITER_INDEX);

    when(mockClosedWriterConn.isClosed()).thenReturn(true);
    when(mockConnectionProvider.connect(writerHost)).thenReturn(mockNewWriterConn);
    when(mockNewWriterConn.getHostPortPair()).thenReturn(writerHost.getHostPortPair());
    when(mockClosedWriterConn.getPropertySet()).thenReturn(props);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockNewWriterConn), eq(writerHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(not(eq(mockNewWriterConn)), eq(writerHost));
    assertEquals(mockNewWriterConn, plugin.getWriterConnection());
    assertEquals(mockNewWriterConn, plugin.getReaderConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_twoHosts() throws SQLException {
    String url = "jdbc:mysql:aws://writer,reader1/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn);
    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);
    HostInfo readerHost = plugin.getHosts().get(WRITER_INDEX + 1);

    when(mockConnectionProvider.connect(eq(readerHost))).thenReturn(mockReaderConn);
    when(mockReaderConn.isClosed()).thenReturn(false);
    when(mockWriterConn.getPropertySet()).thenReturn(props);
    when(mockReaderConn.getHostPortPair()).thenReturn("reader1:3306");

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(readerHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_readerConnectionFailed() throws SQLException {
    String url = "jdbc:mysql:aws://writer,reader1,reader2/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn);
    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);
    HostInfo writerHost = plugin.getHosts().get(WRITER_INDEX);

    when(mockReaderConn.isClosed()).thenReturn(false);
    when(mockConnectionProvider.connect(not(eq(writerHost)))).thenThrow(SQLException.class);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertEquals(mockWriterConn, plugin.getReaderConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_noReaderHostMatch() throws SQLException {
    String url = "jdbc:mysql:aws://writer,reader1/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn, mockReaderConn,
        mockWriterConn, mockWriterConn, mockWriterConn);
    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);
    List<HostInfo> hosts = plugin.getHosts();
    HostInfo writerHost = hosts.get(WRITER_INDEX);
    HostInfo readerHost = hosts.get(WRITER_INDEX + 1);

    when(mockConnectionProvider.connect(eq(readerHost))).thenReturn(mockReaderConn);
    when(mockConnectionProvider.connect(eq(writerHost))).thenReturn(mockWriterConn);
    when(mockReaderConn.isClosed()).thenReturn(false);
    when(mockWriterConn.getPropertySet()).thenReturn(props);
    when(mockReaderConn.getHostPortPair()).thenReturn("reader1:3306");
    when(mockReaderConn.getPropertySet()).thenReturn(props);

    plugin.switchConnectionIfRequired(true);
    plugin.switchConnectionIfRequired(false);
    hosts.remove(WRITER_INDEX + 1);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(readerHost));
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockWriterConn), eq(writerHost));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(readerHost));
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockWriterConn), eq(writerHost));
    assertEquals(mockWriterConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_noReaderHostMatch_writerClosed() throws SQLException {
    String url = "jdbc:mysql:aws://writer,reader1/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn, mockReaderConn,
        mockWriterConn, mockWriterConn, mockWriterConn);
    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);
    List<HostInfo> hosts = plugin.getHosts();
    HostInfo writerHost = hosts.get(WRITER_INDEX);
    HostInfo readerHost = hosts.get(WRITER_INDEX + 1);

    when(mockConnectionProvider.connect(eq(readerHost))).thenReturn(mockReaderConn);
    when(mockConnectionProvider.connect(eq(writerHost))).thenReturn(mockWriterConn);
    when(mockReaderConn.isClosed()).thenReturn(false);
    when(mockWriterConn.isClosed()).thenReturn(true);
    when(mockWriterConn.getPropertySet()).thenReturn(props);
    when(mockReaderConn.getHostPortPair()).thenReturn("reader1:3306");
    when(mockReaderConn.getPropertySet()).thenReturn(props);

    plugin.switchConnectionIfRequired(true);
    plugin.switchConnectionIfRequired(false);
    hosts.remove(WRITER_INDEX + 1);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(readerHost));
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockWriterConn), eq(writerHost));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());

    SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(true));
    assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(readerHost));
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockWriterConn), eq(writerHost));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertFalse(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_trueFalse_zeroHosts() throws SQLException {
    String url = "jdbc:mysql:aws://writer,reader1/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn);
    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);
    List<HostInfo> hosts = plugin.getHosts();
    HostInfo readerHost = hosts.get(WRITER_INDEX + 1);

    when(mockConnectionProvider.connect(eq(readerHost))).thenReturn(mockReaderConn);
    when(mockWriterConn.isClosed()).thenReturn(true);
    when(mockReaderConn.getHostPortPair()).thenReturn("reader1:3306");
    when(mockWriterConn.getPropertySet()).thenReturn(props);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(readerHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());

    hosts.clear();
    SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(false));
    assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(readerHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_false_writerConnectionFails() throws SQLException {
    String url = "jdbc:mysql:aws://writer,reader1/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn, mockWriterConn,
        mockReaderConn, mockReaderConn);
    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);
    List<HostInfo> hosts = plugin.getHosts();
    HostInfo writerHost = hosts.get(WRITER_INDEX);
    HostInfo readerHost = hosts.get(WRITER_INDEX + 1);

    when(mockConnectionProvider.connect(eq(readerHost))).thenReturn(mockReaderConn);
    when(mockConnectionProvider.connect(eq(writerHost))).thenThrow(SQLException.class);
    when(mockWriterConn.isClosed()).thenReturn(true);
    when(mockReaderConn.getHostPortPair()).thenReturn("reader1:3306");
    when(mockWriterConn.getPropertySet()).thenReturn(props);

    plugin.switchConnectionIfRequired(true);
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(readerHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());

    SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(false));
    assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(1)).setCurrentConnection(eq(mockReaderConn), eq(readerHost));
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(eq(mockWriterConn), any(HostInfo.class));
    assertEquals(mockReaderConn, plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertTrue(plugin.getReadOnly());
  }

  @Test
  public void testSetReadOnly_true_readerConnectionFails_writerClosed() throws SQLException {
    String url = "jdbc:mysql:aws://writer,reader1/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySet props = new JdbcPropertySetImpl();

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(
        mockWriterConn,
        mockWriterConn, mockWriterConn);
    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(
        mockCurrentConnectionProvider,
        mockConnectionProvider,
        props,
        mockNextPlugin,
        mockLog);
    plugin.openInitialConnection(connUrl);
    List<HostInfo> hosts = plugin.getHosts();
    HostInfo readerHost = hosts.get(WRITER_INDEX + 1);

    when(mockConnectionProvider.connect(eq(readerHost))).thenThrow(SQLException.class);
    when(mockWriterConn.isClosed()).thenReturn(true);

    SQLException e = assertThrows(SQLException.class, () -> plugin.switchConnectionIfRequired(true));
    assertEquals(MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e.getSQLState());
    verify(mockCurrentConnectionProvider, times(0)).setCurrentConnection(any(ConnectionImpl.class), any(HostInfo.class));
    assertNull(plugin.getReaderConnection());
    assertEquals(mockWriterConn, plugin.getWriterConnection());
    assertFalse(plugin.getReadOnly());
  }
}
