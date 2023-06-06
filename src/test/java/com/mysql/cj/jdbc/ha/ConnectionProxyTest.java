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

package com.mysql.cj.jdbc.ha;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.ha.plugins.ConnectionPluginManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * ClusterAwareConnectionProxyTest class.
 */
public class ConnectionProxyTest {
  private static final String DEFAULT_CONNECTION_STR =
      "jdbc:mysql:aws://somehost:1234/test";
  @Mock private ConnectionImpl mockConnection;
  @Mock private ConnectionImpl mockConnection2;
  @Mock private Statement mockStatement;
  @Mock private HostInfo mockHostInfo;
  @Mock private ConnectionPluginManager mockPluginManager;
  private AutoCloseable closeable;

  @Test
  public void testInvokeReturnJdbcInterfaceProxy()
      throws Throwable {
    when(mockPluginManager.execute(any(), eq("createStatement"), any(), any()))
        .thenReturn(mockStatement);
    doThrow(new SQLException()).when(mockConnection).close();

    final ConnectionUrl conStr =
        ConnectionUrl.getConnectionUrlInstance(DEFAULT_CONNECTION_STR, new Properties());
    final ConnectionProxy proxy = getConnectionProxy(conStr);
    assertSame(mockConnection, proxy.getCurrentConnection());

    final Object result =
        proxy.invoke(null, Connection.class.getMethod("createStatement"), null);
    assertTrue(result instanceof Proxy);
  }

  @Test
  public void testInvokeThrowsException() throws Throwable {
    doThrow(new SQLException())
        .when(mockPluginManager)
        .execute(any(), eq("createStatement"), any(), any());

    final ConnectionUrl conStr =
        ConnectionUrl.getConnectionUrlInstance(DEFAULT_CONNECTION_STR, new Properties());
    final ConnectionProxy proxy = getConnectionProxy(conStr);
    assertSame(mockConnection, proxy.getCurrentConnection());

    assertThrows(
        SQLException.class,
        () -> proxy.invoke(null, Connection.class.getMethod("createStatement"), null));
  }

  @Test
  public void testPluginDisabled() throws SQLException {
    final String url =
        "jdbc:mysql:aws://somehost:1234/test?"
            + PropertyKey.useConnectionPlugins.getKeyName()
            + "=false";
    final ConnectionUrl conStr =
        ConnectionUrl.getConnectionUrlInstance(url, new Properties());

    final ConnectionProxy proxy = getConnectionProxy(conStr);

    assertSame(mockConnection, proxy.getCurrentConnection());
  }

  @Test
  public void testSetCurrentConnectionDoesNotThrowSQLException() throws SQLException {
    when(mockConnection.isClosed()).thenReturn(false);
    doThrow(new SQLException()).when(mockConnection).close();

    final ConnectionUrl conStr =
        ConnectionUrl.getConnectionUrlInstance(DEFAULT_CONNECTION_STR, new Properties());
    final ConnectionProxy proxy = getConnectionProxy(conStr);

    assertSame(mockConnection, proxy.getCurrentConnection());

    assertDoesNotThrow(() -> proxy.setCurrentConnection(mockConnection2, mockHostInfo));

    assertSame(mockConnection2, proxy.getCurrentConnection());
    assertSame(mockHostInfo, proxy.getCurrentHostInfo());
  }

  @Test
  public void testSetCurrentConnectionWithClosedConnection() throws SQLException {
    when(mockConnection.isClosed()).thenReturn(false);

    final ConnectionUrl conStr =
        ConnectionUrl.getConnectionUrlInstance(DEFAULT_CONNECTION_STR, new Properties());
    final ConnectionProxy proxy = getConnectionProxy(conStr);

    assertSame(mockConnection, proxy.getCurrentConnection());

    proxy.setCurrentConnection(mockConnection2, mockHostInfo);

    verify(mockConnection).close();
    assertSame(mockConnection2, proxy.getCurrentConnection());
    assertSame(mockHostInfo, proxy.getCurrentHostInfo());
  }

  @Test
  public void testAwsProtocolWithLegacyPropertyValues() throws SQLException {
    final Properties props = new Properties();
    props.setProperty("zeroDateTimeBehavior", "convertToNull");

    final ConnectionUrl conStr =
        ConnectionUrl.getConnectionUrlInstance(DEFAULT_CONNECTION_STR, props);
    final ConnectionProxy proxy = getConnectionProxy(conStr);

    assertSame(mockConnection, proxy.getCurrentConnection());
    assert(proxy.getCurrentHostInfo().getHostProperties().get("zeroDateTimeBehavior").equals("CONVERT_TO_NULL"));
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  private ConnectionProxy getConnectionProxy(ConnectionUrl connectionUrl)
      throws SQLException {
    return new ConnectionProxy(
        connectionUrl,
        mockConnection,
        (log) -> mockPluginManager);
  }
}
