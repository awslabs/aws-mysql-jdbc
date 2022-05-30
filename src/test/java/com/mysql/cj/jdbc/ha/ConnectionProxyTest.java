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

package com.mysql.cj.jdbc.ha;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
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
  @Mock private ResultSet mockResultSet;
  @Mock private HostInfo mockHostInfo;
  @Mock private ConnectionPluginManager mockPluginManager;
  private AutoCloseable closeable;

  @Test
  public void testInvokeReturnJdbcInterfaceProxy()
      throws Throwable {
    Method createStatementMethod = Connection.class.getMethod("createStatement");
    Object[] executeArgs = new Object[] { "SELECT 1" };
    when(mockPluginManager.executeOnConnection(eq(createStatementMethod), eq(null)))
        .thenReturn(mockStatement);
    when(mockPluginManager.executeOnConnectionBoundObject(any(), eq("executeQuery"), any(), eq(executeArgs)))
        .thenReturn(mockResultSet);
    doThrow(new SQLException()).when(mockConnection).close();

    final ConnectionUrl conStr =
        ConnectionUrl.getConnectionUrlInstance(DEFAULT_CONNECTION_STR, new Properties());
    final ConnectionProxy proxy = getConnectionProxy(conStr);
    assertSame(mockConnection, proxy.getCurrentConnection());

    final Object statementObject =
        proxy.invoke(null, createStatementMethod, null);
    assertTrue(statementObject instanceof Proxy);
    assertTrue(statementObject instanceof Statement);
    verify(mockPluginManager, times(1)).executeOnConnection(eq(createStatementMethod), eq(null));
    verify(mockPluginManager, never()).executeOnConnectionBoundObject(any(), any(), any(), any());

    final ResultSet resultSet = ((Statement) statementObject).executeQuery("SELECT 1");
    assertTrue(resultSet instanceof Proxy);
    verify(mockPluginManager, times(1)).executeOnConnection(eq(createStatementMethod), eq(null));
    verify(mockPluginManager, times(1)).executeOnConnectionBoundObject(any(),
        eq("executeQuery"), any(), eq(executeArgs));
  }

  @Test
  public void testInvokeThrowsException() throws Throwable {
    Method createStatementMethod = Connection.class.getMethod("createStatement");
    doThrow(new SQLException())
        .when(mockPluginManager)
        .executeOnConnection(eq(createStatementMethod), any());

    final ConnectionUrl conStr =
        ConnectionUrl.getConnectionUrlInstance(DEFAULT_CONNECTION_STR, new Properties());
    final ConnectionProxy proxy = getConnectionProxy(conStr);
    assertSame(mockConnection, proxy.getCurrentConnection());

    assertThrows(
        SQLException.class,
        () -> proxy.invoke(null, createStatementMethod, null));
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
