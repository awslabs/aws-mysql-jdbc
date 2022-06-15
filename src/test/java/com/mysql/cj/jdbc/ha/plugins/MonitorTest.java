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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.conf.BooleanProperty;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.IntegerProperty;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

class MonitorTest {

  @Mock
  IConnectionProvider connectionProvider;
  @Mock
  ConnectionImpl connection;
  @Mock
  HostInfo hostInfo;
  @Mock
  PropertySet propertySet;
  @Mock
  Log log;
  @Mock
  MonitorConnectionContext contextWithShortInterval;
  @Mock
  MonitorConnectionContext contextWithLongInterval;
  @Mock
  BooleanProperty booleanProperty;
  @Mock
  IntegerProperty integerProperty;
  @Mock
  IExecutorServiceInitializer executorServiceInitializer;
  @Mock
  ExecutorService executorService;
  @Mock
  Future<?> futureResult;
  @Mock
  DefaultMonitorService monitorService;

  private static final int SHORT_INTERVAL_MILLIS = 30;
  private static final int SHORT_INTERVAL_SECONDS = SHORT_INTERVAL_MILLIS / 1000;
  private static final int LONG_INTERVAL_MILLIS = 300;

  private AutoCloseable closeable;
  private Monitor monitor;

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    when(contextWithShortInterval.getFailureDetectionIntervalMillis())
        .thenReturn(SHORT_INTERVAL_MILLIS);
    when(contextWithLongInterval.getFailureDetectionIntervalMillis())
        .thenReturn(LONG_INTERVAL_MILLIS);
    when(propertySet.getBooleanProperty(any(PropertyKey.class)))
        .thenReturn(booleanProperty);
    when(booleanProperty.getStringValue())
        .thenReturn(Boolean.TRUE.toString());
    when(propertySet.getIntegerProperty(any(PropertyKey.class)))
        .thenReturn(integerProperty);
    when(integerProperty.getValue())
        .thenReturn(SHORT_INTERVAL_MILLIS);
    when(connectionProvider.connect(any(HostInfo.class)))
        .thenReturn(connection);
    when(executorServiceInitializer.createExecutorService())
        .thenReturn(executorService);
    MonitorThreadContainer.getInstance(executorServiceInitializer);

    monitor = spy(new Monitor(
        connectionProvider,
        hostInfo,
        propertySet,
        propertySet.getIntegerProperty(PropertyKey.monitorDisposalTime).getValue(),
        monitorService,
        log));
  }

  @AfterEach
  void cleanUp() throws Exception {
    monitorService.releaseResources();
    MonitorThreadContainer.releaseInstance();
    closeable.close();
  }

  @Test
  void test_1_startMonitoringWithDifferentContexts() {
    monitor.startMonitoring(contextWithShortInterval);
    monitor.startMonitoring(contextWithLongInterval);

    assertEquals(
        SHORT_INTERVAL_MILLIS,
        monitor.getConnectionCheckIntervalMillis());
    verify(contextWithShortInterval)
        .setStartMonitorTime(anyLong());
    verify(contextWithLongInterval)
        .setStartMonitorTime(anyLong());
  }

  @Test
  void test_2_stopMonitoringWithContextRemaining() {
    monitor.startMonitoring(contextWithShortInterval);
    monitor.startMonitoring(contextWithLongInterval);

    monitor.stopMonitoring(contextWithShortInterval);
    assertEquals(
        LONG_INTERVAL_MILLIS,
        monitor.getConnectionCheckIntervalMillis());
  }

  @Test
  void test_3_stopMonitoringWithNoMatchingContexts() {
    assertDoesNotThrow(() -> monitor.stopMonitoring(contextWithLongInterval));
    assertEquals(
        0,
        monitor.getConnectionCheckIntervalMillis());

    monitor.startMonitoring(contextWithShortInterval);
    assertDoesNotThrow(() -> monitor.stopMonitoring(contextWithLongInterval));
    assertEquals(
        SHORT_INTERVAL_MILLIS,
        monitor.getConnectionCheckIntervalMillis());
  }

  @Test
  void test_4_stopMonitoringTwiceWithSameContext() {
    monitor.startMonitoring(contextWithLongInterval);
    assertDoesNotThrow(() -> {
      monitor.stopMonitoring(contextWithLongInterval);
      monitor.stopMonitoring(contextWithLongInterval);
    });
    assertEquals(
        0,
        monitor.getConnectionCheckIntervalMillis());
  }

  @Test
  void test_5_isConnectionHealthyWithNoExistingConnection() throws SQLException {
    final Monitor.ConnectionStatus status = monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);

    verify(connectionProvider).connect(any(HostInfo.class));
    assertTrue(status.isValid);
    assertTrue(status.elapsedTime >= 0);
  }

  @Test
  void test_6_isConnectionHealthyWithExistingConnection() throws SQLException {
    when(connection.isValid(eq(SHORT_INTERVAL_SECONDS)))
        .thenReturn(Boolean.TRUE, Boolean.FALSE);
    when(connection.isClosed())
        .thenReturn(Boolean.FALSE);

    // Start up a monitoring connection.
    monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);

    final Monitor.ConnectionStatus status1 = monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
    assertTrue(status1.isValid);

    final Monitor.ConnectionStatus status2 = monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
    assertFalse(status2.isValid);

    verify(connection, times(2)).isValid(anyInt());
  }

  @Test
  void test_7_isConnectionHealthyWithSQLException() throws SQLException {
    when(connection.isValid(anyInt()))
        .thenThrow(new SQLException());
    when(connection.isClosed())
        .thenReturn(Boolean.FALSE);

    // Start up a monitoring connection.
    monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);

    assertDoesNotThrow(() -> {
      final Monitor.ConnectionStatus status = monitor.checkConnectionStatus(SHORT_INTERVAL_MILLIS);
      assertFalse(status.isValid);
      assertTrue(status.elapsedTime >= 0);
    });
  }

  @RepeatedTest(1000)
  void test_8_runWithoutContext() {
    final MonitorThreadContainer container = MonitorThreadContainer.getInstance(executorServiceInitializer);
    final Map<String, IMonitor> monitorMap = container.getMonitorMap();
    final Map<IMonitor, Future<?>> taskMap = container.getTasksMap();

    doAnswer(invocation -> {
      container.releaseResource(invocation.getArgument(0));
      return null;
    }).when(monitorService).notifyUnused(any(IMonitor.class));

    doReturn((long) SHORT_INTERVAL_MILLIS)
        .when(monitor).getCurrentTimeMillis();

    // Put monitor into container map
    final String nodeKey = "monitorA";
    monitorMap.put(nodeKey, monitor);
    taskMap.put(monitor, futureResult);

    // Run monitor without contexts
    // Should end by itself
    monitor.run();

    // After running with empty context, monitor should be out of the map
    assertNull(monitorMap.get(nodeKey));
    assertNull(taskMap.get(monitor));

    // Clean-up
    MonitorThreadContainer.releaseInstance();
  }

  @RepeatedTest(1000)
  void test_9_runWithContext() {
    final MonitorThreadContainer container = MonitorThreadContainer.getInstance(executorServiceInitializer);
    final Map<String, IMonitor> monitorMap = container.getMonitorMap();
    final Map<IMonitor, Future<?>> taskMap = container.getTasksMap();

    doAnswer(invocation -> {
      container.releaseResource(invocation.getArgument(0));
      return null;
    }).when(monitorService).notifyUnused(any(IMonitor.class));

    // Put monitor into container map
    final String nodeKey = "monitorA";
    monitorMap.put(nodeKey, monitor);
    taskMap.put(monitor, futureResult);

    // Put context
    monitor.startMonitoring(contextWithShortInterval);
    // Set and start thread to remove context from monitor
    final Thread thread = new Thread(() -> {
      try {
        Thread.sleep(SHORT_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        fail("Thread to stop monitoring context was interrupted.", e);
      } finally {
        monitor.stopMonitoring(contextWithShortInterval);
      }
    });
    thread.start();

    // Run monitor
    // Should end by itself once thread above stops monitoring 'contextWithShortInterval'
    monitor.run();

    // After running monitor should be out of the map
    assertNull(monitorMap.get(nodeKey));
    assertNull(taskMap.get(monitor));

    // Clean-up
    MonitorThreadContainer.releaseInstance();
  }
}
