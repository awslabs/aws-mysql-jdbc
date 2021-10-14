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

package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.BooleanProperty;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.ha.ca.ConnectionProvider;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;

class MonitorTest {

  @Mock
  ConnectionProvider connectionProvider;
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

  private static final int SHORT_INTERVAL_MILLIS = 30;
  private static final int SHORT_INTERVAL_SECONDS = SHORT_INTERVAL_MILLIS / 1000;
  private static final int LONG_INTERVAL_MILLIS = 300;

  private AutoCloseable closeable;
  private Monitor monitor;

  @BeforeEach
  void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    Mockito
        .when(contextWithShortInterval.getFailureDetectionIntervalMillis())
        .thenReturn(SHORT_INTERVAL_MILLIS);
    Mockito
        .when(contextWithLongInterval.getFailureDetectionIntervalMillis())
        .thenReturn(LONG_INTERVAL_MILLIS);
    Mockito
        .when(propertySet.getBooleanProperty(Mockito.any(PropertyKey.class)))
        .thenReturn(booleanProperty);
    Mockito
        .when(booleanProperty.getStringValue())
        .thenReturn(Boolean.TRUE.toString());
    Mockito
        .when(connectionProvider.connect(Mockito.any(HostInfo.class)))
        .thenReturn(connection);

    monitor = new Monitor(connectionProvider, hostInfo, propertySet, log);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void test_1_startMonitoringWithDifferentContexts() {
    monitor.startMonitoring(contextWithShortInterval);
    monitor.startMonitoring(contextWithLongInterval);

    Assertions.assertEquals(
        SHORT_INTERVAL_MILLIS,
        monitor.getShortestFailureDetectionIntervalMillis());
    Mockito
        .verify(contextWithShortInterval)
        .setStartMonitorTime(Mockito.anyLong());
    Mockito
        .verify(contextWithLongInterval)
        .setStartMonitorTime(Mockito.anyLong());
  }

  @Test
  void test_2_stopMonitoringWithContextRemaining() {
    monitor.startMonitoring(contextWithShortInterval);
    monitor.startMonitoring(contextWithLongInterval);

    monitor.stopMonitoring(contextWithShortInterval);
    Assertions.assertEquals(
        LONG_INTERVAL_MILLIS,
        monitor.getShortestFailureDetectionIntervalMillis());
  }

  @Test
  void test_3_stopMonitoringWithNoMatchingContexts() {
    Assertions.assertDoesNotThrow(() -> monitor.stopMonitoring(contextWithLongInterval));
    Assertions.assertEquals(
        0,
        monitor.getShortestFailureDetectionIntervalMillis());

    monitor.startMonitoring(contextWithShortInterval);
    Assertions.assertDoesNotThrow(() -> monitor.stopMonitoring(contextWithLongInterval));
    Assertions.assertEquals(
        SHORT_INTERVAL_MILLIS,
        monitor.getShortestFailureDetectionIntervalMillis());
  }

  @Test
  void test_4_stopMonitoringTwiceWithSameContext() {
    monitor.startMonitoring(contextWithLongInterval);
    Assertions.assertDoesNotThrow(() -> monitor.stopMonitoring(contextWithLongInterval));
    Assertions.assertDoesNotThrow(() -> monitor.stopMonitoring(contextWithLongInterval));
    Assertions.assertEquals(
        0,
        monitor.getShortestFailureDetectionIntervalMillis());
  }

  @Test
  void test_5_isConnectionHealthyWithNoExistingConnection() throws SQLException {
    final Monitor.ConnectionStatus status = monitor.isConnectionHealthy(SHORT_INTERVAL_MILLIS);

    Mockito.verify(connectionProvider).connect(Mockito.any(HostInfo.class));
    Assertions.assertTrue(status.isValid);
    Assertions.assertEquals(0, status.elapsedTime);
  }

  @Test
  void test_6_isConnectionHealthyWithExistingConnection() throws SQLException {
    Mockito
        .when(connection.isValid(Mockito.eq(SHORT_INTERVAL_SECONDS)))
        .thenReturn(Boolean.TRUE, Boolean.FALSE);
    Mockito
        .when(connection.isClosed())
        .thenReturn(Boolean.FALSE);

    // Start up a monitoring connection.
    monitor.isConnectionHealthy(SHORT_INTERVAL_MILLIS);

    final Monitor.ConnectionStatus status1 = monitor.isConnectionHealthy(SHORT_INTERVAL_MILLIS);
    Assertions.assertTrue(status1.isValid);

    final Monitor.ConnectionStatus status2 = monitor.isConnectionHealthy(SHORT_INTERVAL_MILLIS);
    Assertions.assertFalse(status2.isValid);

    Mockito.verify(connection, Mockito.times(2)).isValid(Mockito.anyInt());
  }

  @Test
  void test_7_isConnectionHealthyWithSQLException() throws SQLException {
    Mockito
        .when(connection.isValid(Mockito.anyInt()))
        .thenThrow(new SQLException());
    Mockito
        .when(connection.isClosed())
        .thenReturn(Boolean.FALSE);

    // Start up a monitoring connection.
    monitor.isConnectionHealthy(SHORT_INTERVAL_MILLIS);

    Assertions.assertDoesNotThrow(() -> {
      final Monitor.ConnectionStatus status = monitor.isConnectionHealthy(SHORT_INTERVAL_MILLIS);
      Assertions.assertFalse(status.isValid);
      Assertions.assertEquals(0, status.elapsedTime);
    });

    Mockito.verify(log).logTrace(Mockito.anyString(), Mockito.any(SQLException.class));
  }
}
