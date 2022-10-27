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

import com.mysql.cj.NativeSession;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.url.AwsSingleConnectionUrl;
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
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
  @Mock private ClusterAwareMetricsContainer mockClusterMetricContainer;
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

  @Test
  void testInitialConnectionPropertiesWithNoValues() throws SQLException {
    final List<HostInfo> emptyTopology = new ArrayList<>();
    when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class)))
        .thenReturn(emptyTopology);

    final Properties properties = new Properties();

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin(properties);

    assertEquals(0, failoverPlugin.initialConnectionProps.size());
    assertFalse(failoverPlugin.isRds());
    assertFalse(failoverPlugin.isRdsProxy());
    assertFalse(failoverPlugin.isClusterTopologyAvailable);
    assertFalse(failoverPlugin.isFailoverEnabled());
  }

  @Test
  void testInitialConnectionPropertiesWithExplicitlySetValues() throws SQLException {
    final List<HostInfo> emptyTopology = new ArrayList<>();
    when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class)))
        .thenReturn(emptyTopology);

    final Properties properties = new Properties();
    properties.setProperty("maxAllowedPacket", "10");

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin(properties);
    final Map<String, String> initialConnectionProperties = failoverPlugin.initialConnectionProps;

    assertEquals(1, initialConnectionProperties.size());
    assertEquals("10", initialConnectionProperties.get("maxAllowedPacket"));
    assertFalse(failoverPlugin.isRds());
    assertFalse(failoverPlugin.isRdsProxy());
    assertFalse(failoverPlugin.isClusterTopologyAvailable);
    assertFalse(failoverPlugin.isFailoverEnabled());
  }

  /**
   * Verify that reconnection uses the correct topology
   */
  @Test
  public void testReconnectionWhenTopologyHasBeenUpdated()
      throws SQLException {
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/test";
    mockHostInfo = new HostInfo(new AwsSingleConnectionUrl(ConnectionUrlParser.parseConnectionString(url), new Properties()),
        "my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com", 3306, null, null);

    final HostInfo instance0 =
        ClusterAwareTestUtils.createBasicHostInfo("instance-0", "test");
    final HostInfo instance1 =
        ClusterAwareTestUtils.createBasicHostInfo("instance-1", "test");
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(instance0);
    topology.add(instance1);
    AtomicReference<List<HostInfo>> topologyHolder = new AtomicReference<>();
    mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    doAnswer((Answer<List<HostInfo>>) invocation -> topologyHolder.get())
        .when(mockTopologyService).getCachedTopology();
    doAnswer((Answer<List<HostInfo>>) invocation -> {
      topologyHolder.set(new ArrayList<>(topology));
      return topologyHolder.get();
    }).when(mockTopologyService).getTopology(any(), eq(false));
    doAnswer((Answer<List<HostInfo>>) invocation -> {
      topologyHolder.set(new ArrayList<>(topology));
      return topologyHolder.get();
    }).when(mockTopologyService).getTopology(any(), eq(true));

    final AtomicReference<ConnectionImpl> currentConnectionHolder = new AtomicReference<>();
    final AtomicReference<HostInfo> currentHostInfoHolder = new AtomicReference<>();
    mockCurrentConnectionProvider = mock(ICurrentConnectionProvider.class);
    when(mockCurrentConnectionProvider.getCurrentConnection())
        .thenAnswer((Answer<ConnectionImpl>) invocation -> currentConnectionHolder.get());
    when(mockCurrentConnectionProvider.getCurrentHostInfo())
        .thenAnswer((Answer<HostInfo>) invocation -> currentHostInfoHolder.get());
    Mockito.doAnswer(invocation -> {
      currentConnectionHolder.set(invocation.getArgument(0));
      currentHostInfoHolder.set(invocation.getArgument(1));
      return null;
    }).when(mockCurrentConnectionProvider).setCurrentConnection(any(), any());

    final ConnectionImpl instance0Connection = mock(ConnectionImpl.class);
    {
      when(instance0Connection.getHostPortPair()).thenReturn(instance0.getHostPortPair());
      final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
      propertySet.initializeProperties(new Properties());
      when(instance0Connection.getPropertySet()).thenReturn(propertySet);
      when(instance0Connection.getSession()).thenReturn(mockSession);
      when(instance0Connection.toString()).thenReturn("Mock connection to instance-0");
    }
    final ConnectionImpl instance1Connection = mock(ConnectionImpl.class);
    {
      when(instance1Connection.getHostPortPair()).thenReturn(instance1.getHostPortPair());
      final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
      propertySet.initializeProperties(new Properties());
      when(instance1Connection.getPropertySet()).thenReturn(propertySet);
      when(instance1Connection.getSession()).thenReturn(mockSession);
      when(instance1Connection.toString()).thenReturn("Mock connection to instance-1");
    }

    doAnswer((Answer<ConnectionImpl>) invocation -> {
      if (instance0.equalHostPortPair(invocation.getArgument(0))) {
        return instance0Connection;
      }
      if (instance1.equalHostPortPair(invocation.getArgument(0))) {
        return instance1Connection;
      }
      if (mockHostInfo.equalHostPortPair(invocation.getArgument(0))) {
        return mockConnection;
      }
      return null;
    }).when(mockConnectionProvider).connect(any());
    when(mockTopologyService.getHostByName(instance0Connection)).thenReturn(
        instance0);
    when(mockTopologyService.getHostByName(instance1Connection)).thenReturn(
        instance1);

    currentHostInfoHolder.set(mockHostInfo);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();
    final ConnectionUrl connectionUrl =
        ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    failoverPlugin.openInitialConnection(connectionUrl);

    assertEquals(instance0Connection, mockCurrentConnectionProvider.getCurrentConnection());
    assertEquals(instance0, mockCurrentConnectionProvider.getCurrentHostInfo());

    // Cached topology is updated, for instance by another ConnectionProxy instance (e.g. during a failover)
    topology.clear();
    topology.add(instance1);

    failoverPlugin.updateTopologyAndConnectIfNeeded(false);

    assertEquals(
        FailoverConnectionPlugin.WRITER_CONNECTION_INDEX,
        failoverPlugin.currentHostIndex);
    assertEquals(
        instance1,
        failoverPlugin.hosts.get(failoverPlugin.currentHostIndex));
    assertEquals(instance1Connection, mockCurrentConnectionProvider.getCurrentConnection());
    assertEquals(instance1, mockCurrentConnectionProvider.getCurrentHostInfo());
  }

  /**
   * Verify that reconnection uses the correct topology, and reconnects to writer when hosts are switched
   */
  @Test
  public void testReconnectionToWriterWhenHostsAreSwitched()
      throws SQLException {
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/test";
    mockHostInfo = new HostInfo(new AwsSingleConnectionUrl(ConnectionUrlParser.parseConnectionString(url), new Properties()),
        "my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com", 3306, null, null);

    final HostInfo instance0 =
        ClusterAwareTestUtils.createBasicHostInfo("instance-0", "test");
    final HostInfo instance1 =
        ClusterAwareTestUtils.createBasicHostInfo("instance-1", "test");
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(instance0);
    topology.add(instance1);
    AtomicReference<List<HostInfo>> topologyHolder = new AtomicReference<>();
    mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    doAnswer((Answer<List<HostInfo>>) invocation -> topologyHolder.get())
        .when(mockTopologyService).getCachedTopology();
    doAnswer((Answer<List<HostInfo>>) invocation -> {
      topologyHolder.set(new ArrayList<>(topology));
      return topologyHolder.get();
    }).when(mockTopologyService).getTopology(any(), eq(false));
    doAnswer((Answer<List<HostInfo>>) invocation -> {
      topologyHolder.set(new ArrayList<>(topology));
      return topologyHolder.get();
    }).when(mockTopologyService).getTopology(any(), eq(true));

    final AtomicReference<ConnectionImpl> currentConnectionHolder = new AtomicReference<>();
    final AtomicReference<HostInfo> currentHostInfoHolder = new AtomicReference<>();
    mockCurrentConnectionProvider = mock(ICurrentConnectionProvider.class);
    when(mockCurrentConnectionProvider.getCurrentConnection())
        .thenAnswer((Answer<ConnectionImpl>) invocation -> currentConnectionHolder.get());
    when(mockCurrentConnectionProvider.getCurrentHostInfo())
        .thenAnswer((Answer<HostInfo>) invocation -> currentHostInfoHolder.get());
    Mockito.doAnswer(invocation -> {
      currentConnectionHolder.set(invocation.getArgument(0));
      currentHostInfoHolder.set(invocation.getArgument(1));
      return null;
    }).when(mockCurrentConnectionProvider).setCurrentConnection(any(), any());

    final ConnectionImpl instance0Connection = mock(ConnectionImpl.class);
    {
      when(instance0Connection.getHostPortPair()).thenReturn(instance0.getHostPortPair());
      final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
      propertySet.initializeProperties(new Properties());
      when(instance0Connection.getPropertySet()).thenReturn(propertySet);
      when(instance0Connection.getSession()).thenReturn(mockSession);
      when(instance0Connection.toString()).thenReturn("Mock connection to instance-0");
    }
    final ConnectionImpl instance1Connection = mock(ConnectionImpl.class);
    {
      when(instance1Connection.getHostPortPair()).thenReturn(instance1.getHostPortPair());
      final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
      propertySet.initializeProperties(new Properties());
      when(instance1Connection.getPropertySet()).thenReturn(propertySet);
      when(instance1Connection.getSession()).thenReturn(mockSession);
      when(instance1Connection.toString()).thenReturn("Mock connection to instance-1");
    }

    doAnswer((Answer<ConnectionImpl>) invocation -> {
      if (instance0.equalHostPortPair(invocation.getArgument(0))) {
        return instance0Connection;
      }
      if (instance1.equalHostPortPair(invocation.getArgument(0))) {
        return instance1Connection;
      }
      if (mockHostInfo.equalHostPortPair(invocation.getArgument(0))) {
        return mockConnection;
      }
      return null;
    }).when(mockConnectionProvider).connect(any());
    when(mockTopologyService.getHostByName(instance0Connection)).thenReturn(
        instance0);
    when(mockTopologyService.getHostByName(instance1Connection)).thenReturn(
        instance1);

    currentHostInfoHolder.set(mockHostInfo);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();
    final ConnectionUrl connectionUrl =
        ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    failoverPlugin.openInitialConnection(connectionUrl);

    assertEquals(instance0Connection, mockCurrentConnectionProvider.getCurrentConnection());
    assertEquals(instance0, mockCurrentConnectionProvider.getCurrentHostInfo());

    // Cached topology is updated, for instance by another ConnectionProxy instance (e.g. during a failover)
    topology.clear();
    topology.add(instance1);
    topology.add(instance0);

    failoverPlugin.updateTopologyAndConnectIfNeeded(false);

    assertEquals(
        FailoverConnectionPlugin.WRITER_CONNECTION_INDEX,
        failoverPlugin.currentHostIndex);
    assertEquals(
        instance1,
        failoverPlugin.hosts.get(failoverPlugin.currentHostIndex));
    assertEquals(instance1Connection, mockCurrentConnectionProvider.getCurrentConnection());
    assertEquals(instance1, mockCurrentConnectionProvider.getCurrentHostInfo());
  }

  /**
   * With AWS Aurora v2, the hosts returned from the topology service are:
   * initially:                             instance-0 (writer), instance-1
   * when instance-1 takes over:            instance-1 (writer)
   * after instance-0 restarted as replica: instance-1 (writer), instance-0
   * <p>
   * The topology is updated by one instance doing the failover,
   * and the new topology is used in updateTopologyAndConnectIfNeeded
   * When the new topology contains only one endpoint, the failover should not be disabled
   */
  @Test
  void testFailoverStillEnabledWhenReconnectionWithOneHostInTopology()
      throws SQLException {
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:3306/test";
    mockHostInfo = new HostInfo(new AwsSingleConnectionUrl(ConnectionUrlParser.parseConnectionString(url), new Properties()),
        "my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com", 3306, null, null);

    final HostInfo instance0 =
        ClusterAwareTestUtils.createBasicHostInfo("instance-0", "test");
    final HostInfo instance1 =
        ClusterAwareTestUtils.createBasicHostInfo("instance-1", "test");
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(instance0);
    topology.add(instance1);
    AtomicReference<List<HostInfo>> topologyHolder = new AtomicReference<>();
    mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    doAnswer((Answer<List<HostInfo>>) invocation -> topologyHolder.get())
        .when(mockTopologyService).getCachedTopology();
    doAnswer((Answer<List<HostInfo>>) invocation -> {
      topologyHolder.set(new ArrayList<>(topology));
      return topologyHolder.get();
    }).when(mockTopologyService).getTopology(any(), eq(false));
    doAnswer((Answer<List<HostInfo>>) invocation -> {
      topologyHolder.set(new ArrayList<>(topology));
      return topologyHolder.get();
    }).when(mockTopologyService).getTopology(any(), eq(true));

    final AtomicReference<ConnectionImpl> currentConnectionHolder = new AtomicReference<>();
    final AtomicReference<HostInfo> currentHostInfoHolder = new AtomicReference<>(mockHostInfo);
    mockCurrentConnectionProvider = mock(ICurrentConnectionProvider.class);
    when(mockCurrentConnectionProvider.getCurrentConnection())
        .thenAnswer((Answer<ConnectionImpl>) invocation -> currentConnectionHolder.get());
    when(mockCurrentConnectionProvider.getCurrentHostInfo())
        .thenAnswer((Answer<HostInfo>) invocation -> currentHostInfoHolder.get());
    Mockito.doAnswer(invocation -> {
      currentConnectionHolder.set(invocation.getArgument(0));
      currentHostInfoHolder.set(invocation.getArgument(1));
      return null;
    }).when(mockCurrentConnectionProvider).setCurrentConnection(any(), any());

    final ConnectionImpl instance0Connection = mock(ConnectionImpl.class);
    {
      when(instance0Connection.getHostPortPair()).thenReturn(instance0.getHostPortPair());
      final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
      propertySet.initializeProperties(new Properties());
      when(instance0Connection.getPropertySet()).thenReturn(propertySet);
      when(instance0Connection.getSession()).thenReturn(mockSession);
      when(instance0Connection.toString()).thenReturn("Mock connection to instance-0");
    }
    final ConnectionImpl instance1Connection = mock(ConnectionImpl.class);
    {
      when(instance1Connection.getHostPortPair()).thenReturn(instance1.getHostPortPair());
      final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
      propertySet.initializeProperties(new Properties());
      when(instance1Connection.getPropertySet()).thenReturn(propertySet);
      when(instance1Connection.getSession()).thenReturn(mockSession);
      when(instance1Connection.toString()).thenReturn("Mock connection to instance-1");
    }

    doAnswer((Answer<ConnectionImpl>) invocation -> {
      if (instance0.equalHostPortPair(invocation.getArgument(0))) {
        return instance0Connection;
      }
      if (instance1.equalHostPortPair(invocation.getArgument(0))) {
        return instance1Connection;
      }
      if (mockHostInfo.equalHostPortPair(invocation.getArgument(0))) {
        return mockConnection;
      }
      return null;
    }).when(mockConnectionProvider).connect(any());
    when(mockTopologyService.getHostByName(instance0Connection)).thenReturn(
        instance0);
    when(mockTopologyService.getHostByName(instance1Connection)).thenReturn(
        instance1);

    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin();
    final ConnectionUrl connectionUrl =
        ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    failoverPlugin.openInitialConnection(connectionUrl);

    assertEquals(instance0Connection, mockCurrentConnectionProvider.getCurrentConnection());
    assertEquals(instance0, mockCurrentConnectionProvider.getCurrentHostInfo());

    // Cached topology is updated, for instance by another ConnectionProxy instance (e.g. during a failover)
    topology.clear();
    topology.add(instance1);

    failoverPlugin.updateTopologyAndConnectIfNeeded(false);

    assertEquals(
        FailoverConnectionPlugin.WRITER_CONNECTION_INDEX,
        failoverPlugin.currentHostIndex);
    assertEquals(
        instance1,
        failoverPlugin.hosts.get(failoverPlugin.currentHostIndex));
    assertTrue(failoverPlugin.isFailoverEnabled(), "failover should be enabled");
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
        () -> mockTopologyService,
        () -> mockClusterMetricContainer);
  }
}
