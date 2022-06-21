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

package com.mysql.cj.jdbc.ha.plugins;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.ConnectionProxy;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Stream;

class NodeMonitoringConnectionPluginTest {
  static final String NODE = "node";
  static final Class<JdbcConnection> MONITOR_METHOD_INVOKE_ON = JdbcConnection.class;
  static final String MONITOR_METHOD_NAME = "executeQuery";
  static final String NO_MONITOR_METHOD_NAME = "foo";
  static final int FAILURE_DETECTION_TIME = 10;
  static final int FAILURE_DETECTION_INTERVAL = 100;
  static final int FAILURE_DETECTION_COUNT = 5;
  private static final Object[] EMPTY_ARGS = {};
  @Mock ConnectionProxy proxy;
  @Mock JdbcConnection connection;
  @Mock Statement statement;
  @Mock ResultSet resultSet;
  @Mock PropertySet propertySet;
  @Mock HostInfo hostInfo;
  @Mock IConnectionPlugin mockPlugin;
  @Mock Log logger;
  @Mock Supplier<IMonitorService> supplier;
  @Mock MonitorConnectionContext context;
  @Mock IMonitorService monitorService;
  @Mock Callable<?> sqlFunction;
  @Mock RuntimeProperty<Boolean> failureDetectionEnabledProperty;
  @Mock RuntimeProperty<Integer> failureDetectionTimeProperty;
  @Mock RuntimeProperty<Integer> failureDetectionIntervalProperty;
  @Mock RuntimeProperty<Integer> failureDetectionCountProperty;
  private NodeMonitoringConnectionPlugin plugin;
  private AutoCloseable closeable;

  /**
   * Generate different sets of method arguments where one argument is null to ensure
   * {@link NodeMonitoringConnectionPlugin#NodeMonitoringConnectionPlugin(ICurrentConnectionProvider, PropertySet, IConnectionPlugin, Log)}
   * can handle null arguments correctly.
   *
   * @return different sets of arguments.
   */
  private static Stream<Arguments> generateNullArguments() {
    final ConnectionProxy proxy = mock(ConnectionProxy.class);
    final ICurrentConnectionProvider currentConnectionProvider = mock(ICurrentConnectionProvider.class);
    final PropertySet set = new DefaultPropertySet();
    final Log log = new NullLogger("NodeMonitoringConnectionPluginTest");
    final IConnectionPlugin connectionPlugin = new DefaultConnectionPlugin(currentConnectionProvider, log);

    return Stream.of(
        Arguments.of(null, set, connectionPlugin, log),
        Arguments.of(proxy, null, connectionPlugin, log),
        Arguments.of(proxy, set, null, log),
        Arguments.of(proxy, set, connectionPlugin, null)
    );
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @BeforeEach
  void init() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);

    initDefaultMockReturns();
  }

  void initDefaultMockReturns() throws Exception {
    when(hostInfo.getHost())
        .thenReturn(NODE);
    when(supplier.get())
        .thenReturn(monitorService);
    when(monitorService.startMonitoring(
        any(JdbcConnection.class),
        anySet(),
        any(HostInfo.class),
        any(PropertySet.class),
        anyInt(),
        anyInt(),
        anyInt()))
        .thenReturn(context);

    when(mockPlugin.execute(
        any(Class.class),
        anyString(),
        Mockito.any(Callable.class),
        eq(EMPTY_ARGS))).thenReturn("done");

    when(proxy.getCurrentConnection()).thenReturn(connection);
    when(proxy.getCurrentHostInfo()).thenReturn(hostInfo);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(anyString())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);
    when(hostInfo.getHost()).thenReturn("host");
    when(hostInfo.getHost()).thenReturn("port");

    when(propertySet.getBooleanProperty(Mockito.eq(PropertyKey.failureDetectionEnabled)))
        .thenReturn(failureDetectionEnabledProperty);
    when(propertySet.getIntegerProperty(Mockito.eq(PropertyKey.failureDetectionTime)))
        .thenReturn(failureDetectionTimeProperty);
    when(propertySet.getIntegerProperty(Mockito.eq(PropertyKey.failureDetectionInterval)))
        .thenReturn(failureDetectionIntervalProperty);
    when(propertySet.getIntegerProperty(Mockito.eq(PropertyKey.failureDetectionCount)))
        .thenReturn(failureDetectionCountProperty);

    when(failureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);
    when(failureDetectionTimeProperty.getValue())
        .thenReturn(FAILURE_DETECTION_TIME);
    when(failureDetectionIntervalProperty.getValue())
        .thenReturn(FAILURE_DETECTION_INTERVAL);
    when(failureDetectionCountProperty.getValue())
        .thenReturn(FAILURE_DETECTION_COUNT);
  }

  @ParameterizedTest
  @MethodSource("generateNullArguments")
  void test_1_initWithNullArguments(
      final ConnectionProxy proxy,
      final PropertySet set,
      final IConnectionPlugin connectionPlugin,
      final Log log) {
    assertThrows(
        IllegalArgumentException.class,
        () -> new NodeMonitoringConnectionPlugin(proxy, set, connectionPlugin, log));
  }

  @Test
  void test_2_executeWithFailoverDisabled() throws Exception {
    when(failureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.FALSE);

    initializePlugin();
    plugin.execute(
        MONITOR_METHOD_INVOKE_ON,
        MONITOR_METHOD_NAME,
        sqlFunction,
        EMPTY_ARGS);

    verify(supplier, never()).get();
    verify(mockPlugin).execute(
        any(Class.class),
        eq(MONITOR_METHOD_NAME),
        eq(sqlFunction),
        eq(EMPTY_ARGS));
  }

  @Test
  void test_3_executeWithNoNeedToMonitor() throws Exception {
    when(failureDetectionEnabledProperty.getValue())
        .thenReturn(Boolean.TRUE);

    initializePlugin();
    plugin.execute(MONITOR_METHOD_INVOKE_ON, NO_MONITOR_METHOD_NAME, sqlFunction,
        EMPTY_ARGS);

    verify(supplier, atMostOnce()).get();
    verify(mockPlugin).execute(
        any(Class.class),
        eq(NO_MONITOR_METHOD_NAME),
        eq(sqlFunction),
        eq(EMPTY_ARGS));
  }

  private void initializePlugin() {
    plugin = new NodeMonitoringConnectionPlugin(proxy,
        propertySet,
        mockPlugin,
        logger,
        supplier);
  }
}
