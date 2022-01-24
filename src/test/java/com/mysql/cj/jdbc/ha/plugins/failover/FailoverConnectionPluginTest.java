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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.NativeSession;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPropertySet;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.jdbc.ha.plugins.IConnectionPlugin;
import com.mysql.cj.jdbc.ha.plugins.IConnectionProvider;
import com.mysql.cj.jdbc.ha.plugins.ICurrentConnectionProvider;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

class FailoverConnectionPluginTest {
  private static final String PREFIX = "jdbc:mysql:aws://";
  private static final String URL = "somehost:1234";
  private static final int PORT = 1234;
  private static final String DATABASE = "test";
  private final HostInfo writerHost =
      ClusterAwareTestUtils.createBasicHostInfo("writer", "test");
  private final HostInfo readerHost =
      ClusterAwareTestUtils.createBasicHostInfo("reader", "test");
  private final List<HostInfo> mockTopology =
      new ArrayList<>(Arrays.asList(writerHost, readerHost));
  @Mock private ConnectionImpl mockConnection;
  @Mock private IConnectionProvider mockConnectionProvider;
  @Mock private ICurrentConnectionProvider mockCurrentConnectionProvider;
  @Mock private IConnectionPlugin mockNextPlugin;
  @Mock private HostInfo mockHostInfo;
  @Mock private NativeSession mockSession;
  @Mock private Log mockLogger;
  @Mock private AuroraTopologyService mockTopologyService;
  private AutoCloseable closeable;

  @Test
  public void testConnectToWriterFromReaderOnSetReadOnlyFalse() throws Exception {
    final ConnectionImpl mockReaderConnection = mockConnection;
    final ConnectionImpl mockWriterConnection = mockConnection;

    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);
    final HostInfo writerHost =
        ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerA_Host);
    topology.add(readerB_Host);

    when(mockTopologyService.getTopology(mockReaderConnection, false))
        .thenReturn(topology);
    when(mockTopologyService.getHostByName(mockReaderConnection))
        .thenReturn(readerB_Host);
    when(mockConnectionProvider.connect(mockHostInfo))
        .thenReturn(mockReaderConnection);
    when(mockConnectionProvider.connect(refEq(writerHost)))
        .thenReturn(mockWriterConnection);

    final Properties properties = new Properties();
    properties.setProperty(PropertyKey.failOverReadOnly.getKeyName(), "false");

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin(properties);
    assertTrue(failoverPlugin.isCurrentConnectionReadOnly());
    assertTrue(failoverPlugin.explicitlyReadOnly);

    failoverPlugin.execute(
        JdbcConnection.class,
        "setReadOnly",
        () -> {
          mockConnection.setReadOnly(false);
          return true;
        },
        new Object[] {false});
    assertFalse(failoverPlugin.explicitlyReadOnly);

    assertTrue(failoverPlugin.isCurrentConnectionWriter());
  }

  @Test
  public void testCustomDomainCluster() throws SQLException {
    final String url = "jdbc:mysql:aws://my-custom-domain.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final Properties properties = new Properties();
    properties.setProperty(
        PropertyKey.clusterInstanceHostPattern.getKeyName(),
        "?.my-custom-domain.com:9999");

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin(properties);

    assertFalse(failoverPlugin.isRds());
    assertFalse(failoverPlugin.isRdsProxy());
    assertTrue(failoverPlugin.isClusterTopologyAvailable);
    assertTrue(failoverPlugin.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  /**
   * Tests {@link FailoverConnectionPlugin} uses original connection if failover is not
   * enabled.
   */
  @Test
  public void testFailoverDisabled() throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty(
        PropertyKey.enableClusterAwareFailover.getKeyName(),
        "false");
    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin(properties);

    assertFalse(failoverPlugin.enableFailoverSetting);
    verify(mockConnectionProvider, never()).connect(any());
  }

  @Test
  public void testForWriterReconnectWhenDirectReaderConnectionFails()
      throws SQLException {
    // Although the user specified an instance that happened to be a reader, they have not
    // explicitly specified that they want a reader.
    // It is possible that they don't know the reader/writer status of this instance, so we cannot
    // assume they want a read-only connection.
    // As a result, if this direct connection fails, we should reconnect to the writer.
    final ConnectionImpl mockDirectReaderConn = mockConnection;
    final ConnectionImpl mockWriterConn = mockConnection;

    final String url = "jdbc:mysql:aws://reader-b-host.XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final HostInfo writerHost =
        ClusterAwareTestUtils.createBasicHostInfo("writer-host", null);
    final HostInfo readerAHost =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", null);
    final HostInfo readerBHost =
        ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", null);
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerAHost);
    topology.add(readerBHost);

    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(mockDirectReaderConn);
    when(mockTopologyService.getTopology(mockDirectReaderConn, false))
        .thenReturn(topology);
    when(mockTopologyService.getHostByName(mockDirectReaderConn)).thenReturn(null);

    when(mockConnectionProvider.connect(mockHostInfo)).thenReturn(mockDirectReaderConn);
    when(mockConnectionProvider.connect(refEq(writerHost))).thenReturn(mockWriterConn);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();
    final ConnectionUrl connectionUrl =
            ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    failoverPlugin.openInitialConnection(connectionUrl);

    assertEquals(
        FailoverConnectionPlugin.WRITER_CONNECTION_INDEX,
        failoverPlugin.currentHostIndex);
    assertNull(failoverPlugin.explicitlyReadOnly);
    assertFalse(failoverPlugin.isCurrentConnectionReadOnly());
    assertTrue(failoverPlugin.isFailoverEnabled());
  }

  @Test
  public void testForWriterReconnectWhenInvalidInitialWriterConnection()
      throws SQLException {
    final ConnectionImpl mockCachedWriterConn = mockConnection;
    final ConnectionImpl mockActualWriterConn = mockConnection;

    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final HostInfo cachedWriterHost =
        ClusterAwareTestUtils.createBasicHostInfo("cached-writer-host", "test");
    final HostInfo readerA_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> cachedTopology = new ArrayList<>();
    cachedTopology.add(cachedWriterHost);
    cachedTopology.add(readerA_Host);
    cachedTopology.add(readerB_Host);

    final HostInfo actualWriterHost =
        ClusterAwareTestUtils.createBasicHostInfo("actual-writer-host", "test");
    final HostInfo obsoleteWriterHost =
        ClusterAwareTestUtils.createBasicHostInfo("obsolete-writer-host", "test");
    final List<HostInfo> actualTopology = new ArrayList<>();
    actualTopology.add(actualWriterHost);
    actualTopology.add(readerA_Host);
    actualTopology.add(obsoleteWriterHost);

    when(mockCurrentConnectionProvider.getCurrentConnection())
        .thenReturn(mockCachedWriterConn);
    when(mockTopologyService.getCachedTopology()).thenReturn(cachedTopology);
    when(mockTopologyService.getTopology(mockCachedWriterConn, false))
        .thenReturn(actualTopology);
    when(mockTopologyService.getHostByName(mockCachedWriterConn)).thenReturn(
        obsoleteWriterHost);

    when(mockConnectionProvider.connect(refEq(cachedWriterHost))).thenReturn(
        mockCachedWriterConn);
    when(mockConnectionProvider.connect(refEq(actualWriterHost))).thenReturn(
        mockActualWriterConn);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();
    final ConnectionUrl connectionUrl =
            ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    failoverPlugin.openInitialConnection(connectionUrl);

    assertEquals(
        FailoverConnectionPlugin.WRITER_CONNECTION_INDEX,
        failoverPlugin.currentHostIndex);
    assertEquals(
        actualWriterHost,
        failoverPlugin.hosts.get(failoverPlugin.currentHostIndex));
    assertFalse(failoverPlugin.explicitlyReadOnly);
    assertFalse(failoverPlugin.isCurrentConnectionReadOnly());
  }

  @Test
  public void testIfClusterTopologyAvailable() throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty(
        PropertyKey.clusterInstanceHostPattern.getKeyName(),
        "=?.somehost");
    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin(properties);

    assertFalse(failoverPlugin.isRds());
    assertFalse(failoverPlugin.isRdsProxy());
    assertTrue(failoverPlugin.isClusterTopologyAvailable);
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testIfClusterTopologyAvailableAndDnsPatternRequired() {
    assertThrows(
        SQLException.class,
        this::initFailoverPlugin);
  }

  @Test
  public void testIfClusterTopologyNotAvailable() throws SQLException {
    final List<HostInfo> emptyTopology = new ArrayList<>();
    when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class)))
        .thenReturn(emptyTopology);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();

    assertFalse(failoverPlugin.isRds());
    assertFalse(failoverPlugin.isRdsProxy());
    assertFalse(failoverPlugin.isClusterTopologyAvailable);
    assertFalse(failoverPlugin.isFailoverEnabled());
  }

  @Test
  public void testIpAddressAndTopologyAvailableAndDnsPatternRequired() {
    final String url = "jdbc:mysql:aws://10.10.10.10";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    assertThrows(
        SQLException.class,
        this::initFailoverPlugin);
  }

  @Test
  public void testIpAddressAndTopologyNotAvailable() throws SQLException {
    final String url = "jdbc:mysql:aws://10.10.10.10";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final List<HostInfo> emptyTopology = new ArrayList<>();
    when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class)))
        .thenReturn(emptyTopology);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();

    assertFalse(failoverPlugin.isRds());
    assertFalse(failoverPlugin.isRdsProxy());
    assertFalse(failoverPlugin.isClusterTopologyAvailable);
    assertFalse(failoverPlugin.isFailoverEnabled());
  }

  @Test
  public void testIpAddressCluster() throws SQLException {
    final String url = "jdbc:mysql:aws://10.10.10.10";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final Properties properties = new Properties();
    properties.setProperty(
        PropertyKey.clusterInstanceHostPattern.getKeyName(),
        "?.my-custom-domain.com:9999");
    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin(properties);


    assertFalse(failoverPlugin.isRds());
    assertFalse(failoverPlugin.isRdsProxy());
    assertTrue(failoverPlugin.isClusterTopologyAvailable);
    assertTrue(failoverPlugin.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testIpAddressClusterWithClusterId() throws SQLException {
    final ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    final String url = "jdbc:mysql:aws://10.10.10.10";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final Properties properties = new Properties();
    properties.setProperty(
        PropertyKey.clusterInstanceHostPattern.getKeyName(),
        "?.my-custom-domain.com:9999");
    properties.setProperty(
        PropertyKey.clusterId.getKeyName(),
        "test-cluster-id");
    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin(properties);

    assertFalse(failoverPlugin.isRds());
    assertFalse(failoverPlugin.isRdsProxy());
    assertTrue(failoverPlugin.isClusterTopologyAvailable);
    assertTrue(failoverPlugin.isFailoverEnabled());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
    verify(mockTopologyService, atLeastOnce()).setClusterId(captor.capture());
    final List<String> values = captor.getAllValues();
    assertTrue(values.contains("test-cluster-id"));
  }

  @Test
  public void testLastUsedReaderAvailable() throws SQLException {
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);
    final int newConnectionHostIndex = 1;

    final HostInfo writerHost =
        ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerA_Host);
    topology.add(readerB_Host);

    when(mockConnectionProvider.connect(refEq(readerA_Host))).thenReturn(mockConnection);

    when(mockTopologyService.getCachedTopology()).thenReturn(topology);
    when(mockTopologyService.getLastUsedReaderHost()).thenReturn(readerA_Host);
    when(mockTopologyService.getTopology(
        eq(mockConnection),
        any(Boolean.class))).thenReturn(topology);
    when(mockTopologyService.getHostByName(mockConnection)).thenReturn(readerA_Host);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();

    assertEquals(newConnectionHostIndex, failoverPlugin.currentHostIndex);
    assertTrue(failoverPlugin.explicitlyReadOnly);
    assertTrue(failoverPlugin.isCurrentConnectionReadOnly());
  }

  @Test
  public void testRdsCluster() throws SQLException {
    final ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    when(mockConnection.getSession()).thenReturn(mockSession);
    when(mockSession.getLog()).thenReturn(mockLogger);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();

    assertFalse(failoverPlugin.isRdsProxy());
    assertTrue(failoverPlugin.isRds());
    assertTrue(failoverPlugin.isClusterTopologyAvailable);
    assertTrue(failoverPlugin.isFailoverEnabled());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
    verify(mockTopologyService, atLeastOnce()).setClusterId(captor.capture());
    final List<String> values = captor.getAllValues();
    assertTrue(values.contains(
        "my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234"));
  }

  @Test
  public void testRdsCustomCluster() throws SQLException {
    final String url =
        "jdbc:mysql:aws://my-custom-cluster-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();

    assertTrue(failoverPlugin.isRds());
    assertFalse(failoverPlugin.isRdsProxy());
    assertTrue(failoverPlugin.isClusterTopologyAvailable);
    assertTrue(failoverPlugin.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testRdsInstance() throws SQLException {
    final String url =
        "jdbc:mysql:aws://my-instance-name.XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();

    assertTrue(failoverPlugin.isRds());
    assertFalse(failoverPlugin.isRdsProxy());
    assertTrue(failoverPlugin.isClusterTopologyAvailable);
    assertTrue(failoverPlugin.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testRdsProxy() throws SQLException {
    final ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    final String url = "jdbc:mysql:aws://test-proxy.proxy-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();

    assertTrue(failoverPlugin.isRds());
    assertTrue(failoverPlugin.isRdsProxy());
    assertTrue(failoverPlugin.isClusterTopologyAvailable);
    assertFalse(failoverPlugin.isFailoverEnabled());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
    verify(mockTopologyService, atLeastOnce()).setClusterId(captor.capture());
    final List<String> values = captor.getAllValues();
    assertTrue(values.contains("test-proxy.proxy-XYZ.us-east-2.rds.amazonaws.com:1234"));
  }

  @Test
  public void testRdsReaderCluster() throws SQLException {
    final ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);
    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();

    assertTrue(failoverPlugin.isRds());
    assertFalse(failoverPlugin.isRdsProxy());
    assertTrue(failoverPlugin.isClusterTopologyAvailable);
    assertTrue(failoverPlugin.isFailoverEnabled());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
    verify(mockTopologyService, atLeastOnce()).setClusterId(captor.capture());
    final List<String> values = captor.getAllValues();
    assertTrue(values.contains(
        "my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234"));
  }

  @Test
  public void testReadOnlyFalseWhenWriterCluster() throws SQLException {
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final HostInfo writerHost =
        ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerA_Host);
    topology.add(readerB_Host);

    when(mockTopologyService.getCachedTopology()).thenReturn(topology);
    when(mockTopologyService.getTopology(
        eq(mockConnection),
        any(Boolean.class))).thenReturn(topology);
    when(mockTopologyService.getHostByName(mockConnection)).thenReturn(writerHost);
    when(mockConnectionProvider.connect(refEq(writerHost))).thenReturn(mockConnection);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();

    assertTrue(failoverPlugin.isCurrentConnectionWriter());
    assertFalse(failoverPlugin.explicitlyReadOnly);
    assertFalse(failoverPlugin.isCurrentConnectionReadOnly());
  }

  @Test
  public void testReadOnlyTrueWhenReaderCluster() throws SQLException {
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);
    final int connectionHostIndex = 1;

    final HostInfo writerHost =
        ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
    final HostInfo readerAHost =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerAHost);

    when(mockConnectionProvider.connect(refEq(readerAHost))).thenReturn(mockConnection);
    when(mockTopologyService.getTopology(
        eq(mockConnection),
        any(Boolean.class))).thenReturn(topology);
    when(mockTopologyService.getCachedTopology()).thenReturn(topology);
    when(mockTopologyService.getHostByName(mockConnection)).thenReturn(readerAHost);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();

    assertEquals(connectionHostIndex, failoverPlugin.currentHostIndex);
    assertTrue(failoverPlugin.explicitlyReadOnly);
    assertTrue(failoverPlugin.isCurrentConnectionReadOnly());
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    when(mockTopologyService.getTopology(
        eq(mockConnection),
        any(Boolean.class))).thenReturn(mockTopology);
    when(mockTopologyService.getHostByName(mockConnection)).thenReturn(writerHost);
    when(mockCurrentConnectionProvider.getCurrentConnection()).thenReturn(mockConnection);
    when(mockCurrentConnectionProvider.getCurrentHostInfo()).thenReturn(mockHostInfo);
    when(mockHostInfo.getHost()).thenReturn(URL);
    when(mockHostInfo.getDatabaseUrl()).thenReturn(PREFIX + URL);
    when(mockHostInfo.getPort()).thenReturn(PORT);
    when(mockHostInfo.getDatabase()).thenReturn(DATABASE);
    when(mockConnection.getSession()).thenReturn(mockSession);
  }

  private FailoverConnectionPlugin initFailoverPlugin() throws SQLException {
    return initFailoverPlugin(new Properties());
  }

  private FailoverConnectionPlugin initFailoverPlugin(Properties properties)
      throws SQLException {
    final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
    propertySet.initializeProperties(properties);
    when(mockConnection.getPropertySet()).thenReturn(propertySet);
    return new FailoverConnectionPlugin(
        mockCurrentConnectionProvider,
        propertySet,
        mockNextPlugin,
        mockLogger,
        mockConnectionProvider,
        () -> mockTopologyService);
  }
}