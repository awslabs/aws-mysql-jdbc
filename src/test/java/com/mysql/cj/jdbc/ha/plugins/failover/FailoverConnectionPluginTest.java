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
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
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
import com.mysql.cj.jdbc.ha.util.RdsUtils;
import com.mysql.cj.log.Log;
import java.util.Objects;
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
import java.util.Map;
import java.util.Properties;

class FailoverConnectionPluginTest {
  private static final String PREFIX = "jdbc:mysql:aws://";
  private static final String URL = "somehost:1234";
  private static final int PORT = 1234;
  private static final String DATABASE = "test";
  private final List<HostInfo> mockTopology =
      new ArrayList<>(Arrays.asList(writerHost, readerHost));
  static HostInfo writerHost;
  static HostInfo readerHost;
  static {
    try {
      writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer");
      readerHost = ClusterAwareTestUtils.createBasicHostInfo("reader");
    } catch (SQLException e) {
      fail();
    }
  }

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
        ClusterAwareTestUtils.createBasicHostInfo("writer-host");
    final HostInfo readerA_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host");
    final HostInfo readerB_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-b-host");
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
        ClusterAwareTestUtils.createBasicHostInfo("writer-host");
    final HostInfo readerAHost =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host");
    final HostInfo readerBHost =
        ClusterAwareTestUtils.createBasicHostInfo("reader-b-host");
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
        ClusterAwareTestUtils.createBasicHostInfo("cached-writer-host");
    final HostInfo readerA_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host");
    final HostInfo readerB_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-b-host");
    final List<HostInfo> cachedTopology = new ArrayList<>();
    cachedTopology.add(cachedWriterHost);
    cachedTopology.add(readerA_Host);
    cachedTopology.add(readerB_Host);

    final HostInfo actualWriterHost =
        ClusterAwareTestUtils.createBasicHostInfo("actual-writer-host");
    final HostInfo obsoleteWriterHost =
        ClusterAwareTestUtils.createBasicHostInfo("obsolete-writer-host");
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
    assertTrue(ClusterAwareTestUtils.hostsAreTheSame(
        actualWriterHost,
        failoverPlugin.hosts.get(failoverPlugin.currentHostIndex)));
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
        ClusterAwareTestUtils.createBasicHostInfo("writer-host");
    final HostInfo readerA_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host");
    final HostInfo readerB_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-b-host");
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
        ClusterAwareTestUtils.createBasicHostInfo("writer-host");
    final HostInfo readerA_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host");
    final HostInfo readerB_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-b-host");
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
        ClusterAwareTestUtils.createBasicHostInfo("writer-host");
    final HostInfo readerAHost =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host");
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

    assertEquals(4, failoverPlugin.initialConnectionProps.size());
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

    assertEquals(5, initialConnectionProperties.size());
    assertEquals("10", initialConnectionProperties.get("maxAllowedPacket"));
    assertFalse(failoverPlugin.isRds());
    assertFalse(failoverPlugin.isRdsProxy());
    assertFalse(failoverPlugin.isClusterTopologyAvailable);
    assertFalse(failoverPlugin.isFailoverEnabled());
  }

  @Test
  public void testPluginsShareTopologyCache() throws Exception {
    final String clusterId = "clusterId";
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final HostInfo writerHost =
        ClusterAwareTestUtils.createBasicHostInfo("writer-host");
    final HostInfo readerA_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host");
    final HostInfo readerB_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-b-host");
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerA_Host);
    topology.add(readerB_Host);
    final AuroraTopologyService auroraTopologyService1 = new AuroraTopologyService(null);
    final AuroraTopologyService spyAuroratopologyService1 = spy(auroraTopologyService1);
    spyAuroratopologyService1.clusterId = clusterId;

    final AuroraTopologyService auroraTopologyService2 = new AuroraTopologyService(null);
    final AuroraTopologyService spyAuroraTopologyService2 = spy(auroraTopologyService2);
    spyAuroraTopologyService2.clusterId = clusterId;

    doReturn(new AuroraTopologyService.ClusterTopologyInfo(topology))
        .when(spyAuroratopologyService1).queryForTopology(eq(mockConnection));
    doReturn(writerHost).when(spyAuroratopologyService1).getHostByName(eq(mockConnection));
    doReturn(new AuroraTopologyService.ClusterTopologyInfo(topology))
        .when(spyAuroraTopologyService2).queryForTopology(eq(mockConnection));
    doReturn(writerHost).when(spyAuroraTopologyService2).getHostByName(eq(mockConnection));

    final Properties properties = new Properties();
    final FailoverConnectionPlugin failoverPlugin1 = initFailoverPlugin(properties, spyAuroratopologyService1);
    final FailoverConnectionPlugin failoverPlugin2 = initFailoverPlugin(properties, spyAuroraTopologyService2);

    assert(Objects.equals(failoverPlugin1.hosts.get(0).getDatabase(), ""));
    assertEquals(failoverPlugin1.hosts.size(), failoverPlugin2.hosts.size());
    for (int i = 0; i < failoverPlugin1.hosts.size(); i++) {
      assertTrue(ClusterAwareTestUtils.hostsAreTheSame(failoverPlugin1.hosts.get(i), failoverPlugin2.hosts.get(i)));
    }
    verify(spyAuroratopologyService1, times(1)).queryForTopology(eq(mockConnection));
    verify(spyAuroraTopologyService2, never()).queryForTopology(eq(mockConnection));
  }

  @Test
  public void testHostsAndHostIndexOutOfSync() throws Exception {
    final String clusterId = "clusterId";
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final HostInfo writerHost =
        ClusterAwareTestUtils.createBasicHostInfo("writer-host");
    final HostInfo readerA_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host");
    final HostInfo readerB_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-b-host");

    final List<HostInfo> topology1 = new ArrayList<>();
    topology1.add(writerHost);
    topology1.add(readerA_Host);
    topology1.add(readerB_Host);

    final List<HostInfo> topology2 = new ArrayList<>();
    topology2.add(readerA_Host);
    topology2.add(readerB_Host);

    final AuroraTopologyService auroraTopologyService = new AuroraTopologyService(null);
    final AuroraTopologyService spyAuroraTopologyService = spy(auroraTopologyService);
    spyAuroraTopologyService.clusterId = clusterId;

    when(mockConnection.getPropertySet()).thenReturn(null);

    doReturn(topology1).doReturn(topology1).doReturn(topology1).doReturn(topology2)
        .when(spyAuroraTopologyService).getTopology(eq(mockConnection), any(Boolean.class));

    doReturn(writerHost).when(spyAuroraTopologyService).getHostByName(eq(mockConnection));

    final Properties properties = new Properties();
    final FailoverConnectionPlugin failoverPlugin1 = initFailoverPlugin(properties, spyAuroraTopologyService);
    failoverPlugin1.explicitlyReadOnly = false;
    final FailoverConnectionPlugin failoverPlugin2 = initFailoverPlugin(properties, spyAuroraTopologyService);
    failoverPlugin2.explicitlyReadOnly = false;

    failoverPlugin1.execute(
        JdbcConnection.class,
        "execute",
        () -> true,
        new Object[] {false});

    failoverPlugin2.execute(
        JdbcConnection.class,
        "execute",
        () -> true,
        new Object[] {false});

    final ClusterAwareTestUtils.HostInfoMatcher writerMatcher = new ClusterAwareTestUtils.HostInfoMatcher(writerHost);
    final ClusterAwareTestUtils.HostInfoMatcher readerAMatcher = new ClusterAwareTestUtils.HostInfoMatcher(readerA_Host);
    final ClusterAwareTestUtils.HostInfoMatcher readerBMatcher = new ClusterAwareTestUtils.HostInfoMatcher(readerB_Host);
    verify(mockConnectionProvider, times(0)).connect(argThat(writerMatcher));
    verify(mockConnectionProvider, times(1)).connect(argThat(readerAMatcher));
    verify(mockConnectionProvider, times(0)).connect(argThat(readerBMatcher));
    verify(spyAuroraTopologyService, times(4)).getTopology(any(JdbcConnection.class), any(Boolean.class));
  }

  @Test
  public void testFailoverEnabledScaleDown() throws Exception {
    final String clusterId = "clusterId";
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final HostInfo writerHost =
        ClusterAwareTestUtils.createBasicHostInfo("writer-host");
    final HostInfo readerA_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host");

    final List<HostInfo> topology1 = new ArrayList<>();
    topology1.add(writerHost);
    topology1.add(readerA_Host);

    final List<HostInfo> topology2 = new ArrayList<>();
    topology2.add(readerA_Host);

    final AuroraTopologyService auroraTopologyService = new AuroraTopologyService(null);
    final AuroraTopologyService spyAuroraTopologyService = spy(auroraTopologyService);
    spyAuroraTopologyService.clusterId = clusterId;

    when(mockConnection.getPropertySet()).thenReturn(null);

    doReturn(topology1).doReturn(topology2)
        .when(spyAuroraTopologyService).getTopology(eq(mockConnection), any(Boolean.class));

    doReturn(writerHost).when(spyAuroraTopologyService).getHostByName(eq(mockConnection));

    final Properties properties = new Properties();
    final FailoverConnectionPlugin failoverPlugin1 = initFailoverPlugin(properties, spyAuroraTopologyService);
    final FailoverConnectionPlugin spyFailoverPlugin = spy(failoverPlugin1);
    spyFailoverPlugin.currentHostIndex = 0;
    spyFailoverPlugin.isInitialConnectionToReader = false;
    spyFailoverPlugin.explicitlyReadOnly = false;

    spyFailoverPlugin.execute(
        JdbcConnection.class,
        "execute",
        () -> true,
        new Object[] {false});

    verify(spyAuroraTopologyService, times(2)).getTopology(any(JdbcConnection.class), any(Boolean.class));
    verify(spyFailoverPlugin, times(1)).updateTopologyIfNeeded(any(Boolean.class));
    verify(spyFailoverPlugin, times(1)).pickNewConnection(eq(topology2));
  }

  @Test
  public void testFailoverEnabledScaleUp() throws Exception {
    final String clusterId = "clusterId";
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final HostInfo writerHost =
        ClusterAwareTestUtils.createBasicHostInfo("writer-host");
    final HostInfo readerA_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host");

    final List<HostInfo> topology1 = new ArrayList<>();
    topology1.add(writerHost);
    topology1.add(readerA_Host);

    final List<HostInfo> topology2 = new ArrayList<>();
    topology2.add(readerA_Host);

    final AuroraTopologyService auroraTopologyService = new AuroraTopologyService(null);
    final AuroraTopologyService spyAuroraTopologyService = spy(auroraTopologyService);
    spyAuroraTopologyService.clusterId = clusterId;

    when(mockConnection.getPropertySet()).thenReturn(null);

    doReturn(topology2).doReturn(topology1)
        .when(spyAuroraTopologyService).getTopology(eq(mockConnection), any(Boolean.class));

    doReturn(writerHost).when(spyAuroraTopologyService).getHostByName(eq(mockConnection));

    final Properties properties = new Properties();
    final FailoverConnectionPlugin failoverPlugin1 = initFailoverPlugin(properties, spyAuroraTopologyService);
    final FailoverConnectionPlugin spyFailoverPlugin = spy(failoverPlugin1);
    spyFailoverPlugin.currentHostIndex = 0;
    spyFailoverPlugin.isInitialConnectionToReader = false;
    spyFailoverPlugin.explicitlyReadOnly = false;

    spyFailoverPlugin.execute(
        JdbcConnection.class,
        "execute",
        () -> true,
        new Object[] {false});

    verify(spyAuroraTopologyService, times(2)).getTopology(any(JdbcConnection.class), any(Boolean.class));
    verify(spyFailoverPlugin, times(1)).updateTopologyIfNeeded(any(Boolean.class));
    verify(spyFailoverPlugin, times(1)).pickNewConnection(eq(topology1));
  }

  @Test
  public void testFailoverHostRoleChanged() throws Exception {
    final String clusterId = "clusterId";
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final HostInfo writerHost =
        ClusterAwareTestUtils.createBasicHostInfo("writer-host");
    final HostInfo readerA_Host =
        ClusterAwareTestUtils.createBasicHostInfo("reader-a-host");

    final List<HostInfo> topology1 = new ArrayList<>();
    topology1.add(writerHost);
    topology1.add(readerA_Host);

    final List<HostInfo> topology2 = new ArrayList<>();
    topology2.add(readerA_Host);
    topology2.add(writerHost);

    final AuroraTopologyService auroraTopologyService = new AuroraTopologyService(null);
    final AuroraTopologyService spyAuroraTopologyService = spy(auroraTopologyService);
    spyAuroraTopologyService.clusterId = clusterId;

    when(mockConnection.getPropertySet()).thenReturn(null);

    doReturn(topology2).doReturn(topology1)
        .when(spyAuroraTopologyService).getTopology(eq(mockConnection), any(Boolean.class));

    doReturn(writerHost).when(spyAuroraTopologyService).getHostByName(eq(mockConnection));

    final Properties properties = new Properties();
    final FailoverConnectionPlugin failoverPlugin1 = initFailoverPlugin(properties, spyAuroraTopologyService);
    final FailoverConnectionPlugin spyFailoverPlugin = spy(failoverPlugin1);
    spyFailoverPlugin.currentHostIndex = 0;
    spyFailoverPlugin.isInitialConnectionToReader = false;
    spyFailoverPlugin.explicitlyReadOnly = false;

    spyFailoverPlugin.execute(
        JdbcConnection.class,
        "execute",
        () -> true,
        new Object[] {false});

    verify(spyAuroraTopologyService, times(2)).getTopology(any(JdbcConnection.class), any(Boolean.class));
    verify(spyFailoverPlugin, times(1)).updateTopologyIfNeeded(any(Boolean.class));
    verify(spyFailoverPlugin, times(1)).pickNewConnection(eq(topology1));
  }

  @Test
  public void testRetainPropertiesFromPreviousPlugins() throws Exception {
    final String clusterId = "clusterId";
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
    final String host = url.split(PREFIX)[1];
    when(mockHostInfo.getDatabaseUrl()).thenReturn(url);
    when(mockHostInfo.getHost()).thenReturn(host);

    final HostInfo writerHost =
        ClusterAwareTestUtils.createBasicHostInfo("writer-host");
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);

    final AuroraTopologyService auroraTopologyService = new AuroraTopologyService(null);
    final AuroraTopologyService spyAuroratopologyService = spy(auroraTopologyService);
    spyAuroratopologyService.clusterId = clusterId;

    doReturn(new AuroraTopologyService.ClusterTopologyInfo(topology))
        .when(spyAuroratopologyService).queryForTopology(eq(mockConnection));
    doReturn(writerHost).when(spyAuroratopologyService).getHostByName(eq(mockConnection));

    final Properties properties = new Properties();
    final FailoverConnectionPlugin failoverPlugin = initFailoverPlugin(properties, spyAuroratopologyService);

    final String testProperty = "testProperty";
    final String testPropertyValue = "testPropertyValue";
    final Properties previousPluginProperties = new Properties();
    previousPluginProperties.put(testProperty, testPropertyValue);

    final ConnectionUrl connectionUrl = ConnectionUrl.getConnectionUrlInstance(url, previousPluginProperties);
    failoverPlugin.openInitialConnection(connectionUrl);

    assert(failoverPlugin.initialConnectionProps.get(testProperty).equals(testPropertyValue));
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
    return initFailoverPlugin(new Properties(), mockTopologyService);
  }

  private FailoverConnectionPlugin initFailoverPlugin(Properties properties) throws SQLException {
    return initFailoverPlugin(properties, mockTopologyService);
  }
  
  private FailoverConnectionPlugin initFailoverPlugin(Properties properties, AuroraTopologyService topologyService)
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
        () -> topologyService,
        () -> mockClusterMetricContainer);
  }
}
