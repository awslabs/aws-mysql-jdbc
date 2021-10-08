/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of this program hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of this connector, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc.ha.ca;

import com.mysql.cj.Messages;
import com.mysql.cj.NativeSession;
import com.mysql.cj.conf.BooleanPropertyDefinition;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.IntegerPropertyDefinition;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPropertySet;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * ClusterAwareConnectionProxyTest class.
 * */
public class ClusterAwareConnectionProxyTest {

  /**
   * Tests {@link ClusterAwareConnectionProxy} return original connection if failover is not
   * enabled.
   */
  @Test
  public void testFailoverDisabled() throws SQLException {
    final String url =
        "jdbc:mysql:aws://somehost:1234/test?"
            + PropertyKey.enableClusterAwareFailover.getKeyName()
            + "=false";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(conStr.getMainHost())).thenReturn(mockConn);
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr, mockConnectionProvider, mockTopologyService, null, null);

    assertFalse(proxy.enableFailoverSetting);
    assertSame(mockConn, proxy.getConnection());
  }

  @Test
  public void testIfClusterTopologyAvailable() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    stubPropertySet(mockConn);
    when(mockConn.getSession()).thenReturn(Mockito.mock(NativeSession.class));
    final String url =
        "jdbc:mysql:aws://somehost:1234/test?"
            + PropertyKey.clusterInstanceHostPattern.getKeyName()
            + "=?.somehost";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConn);

    HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer", "test");
    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);
    when(mockTopologyService.getHostByName(mockConn)).thenReturn(writerHost);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr, mockConnectionProvider, mockTopologyService, null, null);

    assertFalse(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  private void stubPropertySet(JdbcConnection mockConn) {
    final JdbcPropertySet mockPropertySet = Mockito.mock(JdbcPropertySet.class);
    when(mockConn.getPropertySet()).thenReturn(mockPropertySet);

    final RuntimeProperty<Boolean> localSessionState =
        new BooleanPropertyDefinition(PropertyKey.useLocalSessionState, false, true, "", "", "", 5)
            .createRuntimeProperty();
    when(mockPropertySet.getBooleanProperty(PropertyKey.useLocalSessionState))
        .thenReturn(localSessionState);

    final RuntimeProperty<Integer> connectTimeout =
        new IntegerPropertyDefinition(PropertyKey.connectTimeout, 0, true, Messages.getString("ConnectionProperties.connectTimeout"),
            "0.1.0", PropertyDefinitions.CATEGORY_NETWORK, 9, 0, Integer.MAX_VALUE).createRuntimeProperty();
    when(mockPropertySet.getIntegerProperty(PropertyKey.connectTimeout)).thenReturn(connectTimeout);

    final RuntimeProperty<Integer> socketTimeout =
        new IntegerPropertyDefinition(PropertyKey.socketTimeout, 0, true, Messages.getString("ConnectionProperties.socketTimeout"),
            "0.1.0", PropertyDefinitions.CATEGORY_NETWORK, 10, 0, Integer.MAX_VALUE).createRuntimeProperty();
    when(mockPropertySet.getIntegerProperty(PropertyKey.socketTimeout)).thenReturn(socketTimeout);
  }

  @Test
  public void testIfClusterTopologyNotAvailable() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final String url = "jdbc:mysql:aws://somehost:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);
    final List<HostInfo> emptyTopology = new ArrayList<>();
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(emptyTopology);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConn);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr, mockConnectionProvider, mockTopologyService, null, null);

    assertFalse(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertFalse(proxy.isClusterTopologyAvailable());
    assertFalse(proxy.isFailoverEnabled());
  }

  @Test
  public void testIfClusterTopologyAvailableAndDnsPatternRequired() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    stubPropertySet(mockConn);
    when(mockConn.getSession()).thenReturn(Mockito.mock(NativeSession.class));

    final String url = "jdbc:mysql:aws://somehost:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConn);

    HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer", "test");
    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);
    when(mockTopologyService.getHostByName(mockConn)).thenReturn(writerHost);

    assertThrows(
        SQLException.class,
        () ->
            new ClusterAwareConnectionProxy(
                conStr, mockConnectionProvider, mockTopologyService, null, null));
  }

  @Test
  public void testRdsCluster() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final NativeSession mockSession = Mockito.mock(NativeSession.class);
    final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
    final Log mockLog = Mockito.mock(Log.class);
    when(mockConn.getSession()).thenReturn(mockSession);
    when(mockSession.getLog()).thenReturn(mockLog);
    when(mockConn.getPropertySet()).thenReturn(propertySet);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConn);

    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer", "test");
    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);
    when(mockTopologyService.getHostByName(mockConn)).thenReturn(writerHost);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler);

    assertTrue(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    verify(mockTopologyService, atLeastOnce())
        .setClusterId("my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234");
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testRdsReaderCluster() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final NativeSession mockSession = Mockito.mock(NativeSession.class);
    final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
    final Log mockLog = Mockito.mock(Log.class);
    when(mockConn.getSession()).thenReturn(mockSession);
    when(mockSession.getLog()).thenReturn(mockLog);
    when(mockConn.getPropertySet()).thenReturn(propertySet);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConn);

    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer", "test");
    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);
    when(mockTopologyService.getHostByName(mockConn)).thenReturn(writerHost);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler);

    assertTrue(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    verify(mockTopologyService, atLeastOnce())
        .setClusterId("my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234");
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testRdsCustomCluster() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final NativeSession mockSession = Mockito.mock(NativeSession.class);
    final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
    final Log mockLog = Mockito.mock(Log.class);
    when(mockConn.getSession()).thenReturn(mockSession);
    when(mockSession.getLog()).thenReturn(mockLog);
    when(mockConn.getPropertySet()).thenReturn(propertySet);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConn);

    final String url =
        "jdbc:mysql:aws://my-custom-cluster-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer", "test");
    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);
    when(mockTopologyService.getHostByName(mockConn)).thenReturn(writerHost);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler);

    assertTrue(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testRdsInstance() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final NativeSession mockSession = Mockito.mock(NativeSession.class);
    final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
    final Log mockLog = Mockito.mock(Log.class);
    when(mockConn.getSession()).thenReturn(mockSession);
    when(mockSession.getLog()).thenReturn(mockLog);
    when(mockConn.getPropertySet()).thenReturn(propertySet);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConn);

    final String url = "jdbc:mysql:aws://my-instance-name.XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer", "test");
    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);
    when(mockTopologyService.getHostByName(mockConn)).thenReturn(writerHost);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler);

    assertTrue(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testRdsProxy() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final NativeSession mockSession = Mockito.mock(NativeSession.class);
    final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
    final Log mockLog = Mockito.mock(Log.class);
    when(mockConn.getSession()).thenReturn(mockSession);
    when(mockSession.getLog()).thenReturn(mockLog);
    when(mockConn.getPropertySet()).thenReturn(propertySet);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConn);

    final String url = "jdbc:mysql:aws://test-proxy.proxy-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(Mockito.mock(HostInfo.class));
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler);

    assertTrue(proxy.isRds());
    assertTrue(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertFalse(proxy.isFailoverEnabled());
    verify(mockTopologyService, atLeastOnce())
        .setClusterId("test-proxy.proxy-XYZ.us-east-2.rds.amazonaws.com:1234");
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testCustomDomainCluster() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final NativeSession mockSession = Mockito.mock(NativeSession.class);
    final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
    final Log mockLog = Mockito.mock(Log.class);
    when(mockConn.getSession()).thenReturn(mockSession);
    when(mockSession.getLog()).thenReturn(mockLog);
    when(mockConn.getPropertySet()).thenReturn(propertySet);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConn);

    final String url =
        "jdbc:mysql:aws://my-custom-domain.com:1234/test?"
            + PropertyKey.clusterInstanceHostPattern.getKeyName()
            + "=?.my-custom-domain.com:9999";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer", "test");
    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);
    when(mockTopologyService.getHostByName(mockConn)).thenReturn(writerHost);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler);

    assertFalse(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testIpAddressCluster() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final NativeSession mockSession = Mockito.mock(NativeSession.class);
    final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
    final Log mockLog = Mockito.mock(Log.class);
    when(mockConn.getSession()).thenReturn(mockSession);
    when(mockSession.getLog()).thenReturn(mockLog);
    when(mockConn.getPropertySet()).thenReturn(propertySet);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConn);

    final String url =
        "jdbc:mysql:aws://10.10.10.10:1234/test?"
            + PropertyKey.clusterInstanceHostPattern.getKeyName()
            + "=?.my-custom-domain.com:9999";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer", "test");
    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);
    when(mockTopologyService.getHostByName(mockConn)).thenReturn(writerHost);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler);

    assertFalse(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testIpAddressClusterWithClusterId() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final NativeSession mockSession = Mockito.mock(NativeSession.class);
    final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
    final Log mockLog = Mockito.mock(Log.class);
    when(mockConn.getSession()).thenReturn(mockSession);
    when(mockSession.getLog()).thenReturn(mockLog);
    when(mockConn.getPropertySet()).thenReturn(propertySet);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConn);

    final String url =
        "jdbc:mysql:aws://10.10.10.10:1234/test?"
            + PropertyKey.clusterInstanceHostPattern.getKeyName()
            + "=?.my-custom-domain.com:9999&"
            + PropertyKey.clusterId.getKeyName()
            + "=test-cluster-id";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer", "test");
    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);
    when(mockTopologyService.getHostByName(mockConn)).thenReturn(writerHost);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler);

    assertFalse(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    verify(mockTopologyService, atLeastOnce()).setClusterId("test-cluster-id");
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testIpAddressAndTopologyAvailableAndDnsPatternRequired() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final NativeSession mockSession = Mockito.mock(NativeSession.class);
    final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
    final Log mockLog = Mockito.mock(Log.class);
    when(mockConn.getSession()).thenReturn(mockSession);
    when(mockSession.getLog()).thenReturn(mockLog);
    when(mockConn.getPropertySet()).thenReturn(propertySet);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConn);

    final String url = "jdbc:mysql:aws://10.10.10.10:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer", "test");
    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);
    when(mockTopologyService.getHostByName(mockConn)).thenReturn(writerHost);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    assertThrows(
        SQLException.class,
        () ->
            new ClusterAwareConnectionProxy(
                conStr,
                mockConnectionProvider,
                mockTopologyService,
                writerFailoverHandler,
                readerFailoverHandler));
  }

  @Test
  public void testIpAddressAndTopologyNotAvailable() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final NativeSession mockSession = Mockito.mock(NativeSession.class);
    final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
    final Log mockLog = Mockito.mock(Log.class);
    when(mockConn.getSession()).thenReturn(mockSession);
    when(mockSession.getLog()).thenReturn(mockLog);
    when(mockConn.getPropertySet()).thenReturn(propertySet);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConn);

    final String url = "jdbc:mysql:aws://10.10.10.10:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final List<HostInfo> emptyTopology = new ArrayList<>();
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(emptyTopology);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler);

    assertFalse(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertFalse(proxy.isClusterTopologyAvailable());
    assertFalse(proxy.isFailoverEnabled());
  }

  @Test
  public void testReadOnlyFalseWhenWriterCluster() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    stubPropertySet(mockConn);
    when(mockConn.getSession()).thenReturn(Mockito.mock(NativeSession.class));
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test", "", "");
    final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerA_Host);
    topology.add(readerB_Host);

    when(mockTopologyService.getCachedTopology()).thenReturn(topology);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class))).thenReturn(topology);
    when(mockTopologyService.getHostByName(mockConn)).thenReturn(writerHost);
    when(mockConnectionProvider.connect(refEq(writerHost))).thenReturn(mockConn);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            Mockito.mock(WriterFailoverHandler.class),
            Mockito.mock(ReaderFailoverHandler.class));

    assertTrue(proxy.isCurrentConnectionWriter());
    assertFalse(proxy.explicitlyReadOnly);
    assertFalse(proxy.isCurrentConnectionReadOnly());
  }

  @Test
  public void testReadOnlyTrueWhenReaderCluster() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    stubPropertySet(mockConn);
    when(mockConn.getSession()).thenReturn(Mockito.mock(NativeSession.class));

    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);
    final int connectionHostIndex = 1;

    final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
    final HostInfo readerAHost = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test", "", "");
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerAHost);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(refEq(readerAHost))).thenReturn(mockConn);

    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class))).thenReturn(topology);
    when(mockTopologyService.getCachedTopology()).thenReturn(topology);
    when(mockTopologyService.getHostByName(mockConn)).thenReturn(readerAHost);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            Mockito.mock(WriterFailoverHandler.class),
            Mockito.mock(ReaderFailoverHandler.class));

    assertEquals(connectionHostIndex, proxy.currentHostIndex);
    assertTrue(proxy.explicitlyReadOnly);
    assertTrue(proxy.isCurrentConnectionReadOnly());
  }

  @Test
  public void testLastUsedReaderAvailable() throws SQLException {
    final ConnectionImpl mockConn = Mockito.mock(ConnectionImpl.class);
    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    stubPropertySet(mockConn);
    when(mockConn.getSession()).thenReturn(Mockito.mock(NativeSession.class));

    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);
    final int newConnectionHostIndex = 1;

    final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test", "", "");
    final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerA_Host);
    topology.add(readerB_Host);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(refEq(readerA_Host))).thenReturn(mockConn);

    when(mockTopologyService.getCachedTopology()).thenReturn(topology);
    when(mockTopologyService.getLastUsedReaderHost()).thenReturn(readerA_Host);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class))).thenReturn(topology);
    when(mockTopologyService.getHostByName(mockConn)).thenReturn(readerA_Host);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            Mockito.mock(WriterFailoverHandler.class),
            Mockito.mock(ReaderFailoverHandler.class));

    assertEquals(newConnectionHostIndex, proxy.currentHostIndex);
    assertTrue(proxy.explicitlyReadOnly);
    assertTrue(proxy.isCurrentConnectionReadOnly());
  }

  @Test
  public void testForWriterReconnectWhenInvalidInitialWriterConnection() throws SQLException {
    final ConnectionImpl mockCachedWriterConn = Mockito.mock(ConnectionImpl.class);
    stubPropertySet(mockCachedWriterConn);
    when(mockCachedWriterConn.getSession()).thenReturn(Mockito.mock(NativeSession.class));

    final ConnectionImpl mockActualWriterConn = Mockito.mock(ConnectionImpl.class);
    stubPropertySet(mockActualWriterConn);
    when(mockActualWriterConn.getSession()).thenReturn(Mockito.mock(NativeSession.class));

    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final HostInfo cachedWriterHost = ClusterAwareTestUtils.createBasicHostInfo("cached-writer-host", "test", "", "");
    final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> cachedTopology = new ArrayList<>();
    cachedTopology.add(cachedWriterHost);
    cachedTopology.add(readerA_Host);
    cachedTopology.add(readerB_Host);

    final HostInfo actualWriterHost = ClusterAwareTestUtils.createBasicHostInfo("actual-writer-host", "test", "", "");
    final HostInfo obsoleteWriterHost = ClusterAwareTestUtils.createBasicHostInfo("obsolete-writer-host", "test");
    final List<HostInfo> actualTopology = new ArrayList<>();
    actualTopology.add(actualWriterHost);
    actualTopology.add(readerA_Host);
    actualTopology.add(obsoleteWriterHost);

    when(mockTopologyService.getCachedTopology()).thenReturn(cachedTopology);
    when(mockTopologyService.getTopology(eq(mockCachedWriterConn), any(Boolean.class)))
        .thenReturn(actualTopology);
    when(mockTopologyService.getHostByName(mockCachedWriterConn)).thenReturn(obsoleteWriterHost);

    ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(refEq(cachedWriterHost))).thenReturn(mockCachedWriterConn);
    when(mockConnectionProvider.connect(refEq(actualWriterHost))).thenReturn(mockActualWriterConn);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            Mockito.mock(WriterFailoverHandler.class),
            Mockito.mock(ReaderFailoverHandler.class));

    assertEquals(ClusterAwareConnectionProxy.WRITER_CONNECTION_INDEX, proxy.currentHostIndex);
    assertEquals(actualWriterHost, proxy.hosts.get(proxy.currentHostIndex));
    assertFalse(proxy.explicitlyReadOnly);
    assertFalse(proxy.isCurrentConnectionReadOnly());
  }

  @Test
  public void testForWriterReconnectWhenDirectReaderConnectionFails() throws SQLException {
    // Although the user specified an instance that happened to be a reader, they have not
    // explicitly specified that they want a reader.
    // It is possible that they don't know the reader/writer status of this instance, so we cannot
    // assume they want a read-only connection.
    // As a result, if this direct connection fails, we should reconnect to the writer.
    final ConnectionImpl mockDirectReaderConn = Mockito.mock(ConnectionImpl.class);
    stubPropertySet(mockDirectReaderConn);
    when(mockDirectReaderConn.getSession()).thenReturn(Mockito.mock(NativeSession.class));

    final ConnectionImpl mockWriterConn = Mockito.mock(ConnectionImpl.class);
    stubPropertySet(mockWriterConn);
    when(mockWriterConn.getSession()).thenReturn(Mockito.mock(NativeSession.class));

    final String url = "jdbc:mysql:aws://reader-b-host.XYZ.us-east-2.rds.amazonaws.com";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", null, "", "");
    final HostInfo readerAHost = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", null);
    final HostInfo readerBHost = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", null);
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerAHost);
    topology.add(readerBHost);

    when(mockTopologyService.getTopology(eq(mockDirectReaderConn), any(Boolean.class)))
        .thenReturn(topology);
    when(mockTopologyService.getHostByName(mockDirectReaderConn))
        .thenReturn(null);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(conStr.getMainHost())).thenReturn(mockDirectReaderConn);
    when(mockConnectionProvider.connect(refEq(writerHost))).thenReturn(mockWriterConn);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            Mockito.mock(WriterFailoverHandler.class),
            Mockito.mock(ReaderFailoverHandler.class));

    assertEquals(ClusterAwareConnectionProxy.WRITER_CONNECTION_INDEX, proxy.currentHostIndex);
    assertNull(proxy.explicitlyReadOnly);
    assertFalse(proxy.isCurrentConnectionReadOnly());
  }

  @Test
  public void testConnectToWriterFromReaderOnSetReadOnlyFalse() throws SQLException {
    final ConnectionImpl mockReaderConnection = Mockito.mock(ConnectionImpl.class);
    stubPropertySet(mockReaderConnection);
    when(mockReaderConnection.getSession()).thenReturn(Mockito.mock(NativeSession.class));

    final ConnectionImpl mockWriterConnection = Mockito.mock(ConnectionImpl.class);

    final String url =
        "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test", "", "");
    final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerA_Host);
    topology.add(readerB_Host);

    when(mockTopologyService.getTopology(eq(mockReaderConnection), any(Boolean.class)))
        .thenReturn(topology);
    when(mockTopologyService.getHostByName(mockReaderConnection)).thenReturn(readerB_Host);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(conStr.getMainHost())).thenReturn(mockReaderConnection);
    when(mockConnectionProvider.connect(refEq(writerHost))).thenReturn(mockWriterConnection);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            conStr,
            mockConnectionProvider,
            mockTopologyService,
            Mockito.mock(WriterFailoverHandler.class),
            Mockito.mock(ReaderFailoverHandler.class));
    assertTrue(proxy.isCurrentConnectionReadOnly());
    assertTrue(proxy.explicitlyReadOnly);

    final JdbcConnection connectionProxy =
        (JdbcConnection)
            java.lang.reflect.Proxy.newProxyInstance(
                JdbcConnection.class.getClassLoader(),
                new Class<?>[] {JdbcConnection.class},
                proxy);
    connectionProxy.setReadOnly(false);

    assertFalse(proxy.explicitlyReadOnly);
    assertTrue(proxy.isCurrentConnectionWriter());
  }
}