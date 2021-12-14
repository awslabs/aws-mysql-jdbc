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

import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultMonitorServiceTest {
  private static final Set<String> NODE_KEYS =
      new HashSet<>(Collections.singletonList("any.node.domain"));
  private static final int FAILURE_DETECTION_TIME_MILLIS = 10;
  private static final int FAILURE_DETECTION_INTERVAL_MILLIS = 100;
  private static final int FAILURE_DETECTION_COUNT = 3;

  @Mock
  private IMonitorInitializer monitorInitializer;
  @Mock
  private IExecutorServiceInitializer executorServiceInitializer;
  @Mock
  private Log logger;
  @Mock
  private IMonitor monitorA;
  @Mock
  private IMonitor monitorB;
  @Mock
  private ExecutorService executorService;
  @Mock
  private Future<?> task;
  @Mock
  private HostInfo info;
  @Mock
  private JdbcConnection connection;

  private PropertySet propertySet;
  private AutoCloseable closeable;
  private DefaultMonitorService monitorService;
  private ArgumentCaptor<MonitorConnectionContext> contextCaptor;

  @BeforeEach
  void init() {
    propertySet = new DefaultPropertySet();
    closeable = MockitoAnnotations.openMocks(this);
    contextCaptor = ArgumentCaptor.forClass(MonitorConnectionContext.class);

    when(monitorInitializer.createMonitor(
        any(HostInfo.class),
        any(PropertySet.class),
        any(IMonitorService.class)))
        .thenReturn(monitorA, monitorB);

    when(executorServiceInitializer.createExecutorService())
        .thenReturn(executorService);

    doReturn(task)
        .when(executorService)
        .submit(any(IMonitor.class));

    monitorService = new DefaultMonitorService(
        monitorInitializer,
        executorServiceInitializer,
        logger);
  }

  @AfterEach
  void cleanUp() throws Exception {
    monitorService.releaseResources();
    closeable.close();
  }

  @Test
  void test_1_startMonitoringWithNoExecutor() {
    doNothing().when(monitorA).startMonitoring(contextCaptor.capture());

    monitorService.startMonitoring(
        connection,
        NODE_KEYS,
        info,
        propertySet,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT);

    assertNotNull(contextCaptor.getValue());
    verify(executorService).submit(eq(monitorA));
  }

  @Test
  void test_2_startMonitoringCalledMultipleTimes() {
    doNothing().when(monitorA).startMonitoring(contextCaptor.capture());

    final int runs = 5;

    for (int i = 0; i < runs; i++) {
      monitorService.startMonitoring(
          connection,
          NODE_KEYS,
          info,
          propertySet,
          FAILURE_DETECTION_TIME_MILLIS,
          FAILURE_DETECTION_INTERVAL_MILLIS,
          FAILURE_DETECTION_COUNT);
    }

    assertNotNull(contextCaptor.getValue());

    // executorService should only be called once.
    verify(executorService).submit(eq(monitorA));
  }

  @Test
  void test_3_stopMonitoringWithInterruptedThread() {
    doNothing().when(monitorA).stopMonitoring(contextCaptor.capture());

    final MonitorConnectionContext context = monitorService.startMonitoring(
        connection,
        NODE_KEYS,
        info,
        propertySet,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT);

    monitorService.stopMonitoring(context);

    assertEquals(context, contextCaptor.getValue());
    verify(monitorA).stopMonitoring(any());
  }

  @Test
  void test_4_stopMonitoringCalledTwice() {
    doNothing().when(monitorA).stopMonitoring(contextCaptor.capture());

    final MonitorConnectionContext context = monitorService.startMonitoring(
        connection,
        NODE_KEYS,
        info,
        propertySet,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT);

    monitorService.stopMonitoring(context);

    assertEquals(context, contextCaptor.getValue());

    monitorService.stopMonitoring(context);
    verify(monitorA, times(2)).stopMonitoring(any());
  }

  @Test
  void test_5_getMonitorCalledWithMultipleNodesInKeys() {
    final Set<String> nodeKeys = new HashSet<>();
    nodeKeys.add("nodeOne.domain");
    nodeKeys.add("nodeTwo.domain");

    final Set<String> nodeKeysTwo = new HashSet<>();
    nodeKeysTwo.add("nodeTwo.domain");

    final IMonitor monitorOne = monitorService.getMonitor(nodeKeys, info, propertySet);
    assertNotNull(monitorOne);

    // Should get the same monitor as before as contain the same key "nodeTwo.domain"
    final IMonitor monitorOneSame =
        monitorService.getMonitor(nodeKeysTwo, info, propertySet);
    assertNotNull(monitorOneSame);
    assertEquals(monitorOne, monitorOneSame);

    // Make sure createMonitor was called once
    verify(monitorInitializer).createMonitor(
        eq(info),
        eq(propertySet),
        eq(monitorService));
  }

  @Test
  void test_6_getMonitorCalledWithDifferentNodeKeys() {
    final Set<String> nodeKeys = new HashSet<>();
    nodeKeys.add("nodeNEW.domain");

    final IMonitor monitorOne = monitorService.getMonitor(nodeKeys, info, propertySet);
    assertNotNull(monitorOne);

    // Ensuring monitor is the same one and not creating a new one
    final IMonitor monitorOneDupe =
        monitorService.getMonitor(nodeKeys, info, propertySet);
    assertEquals(monitorOne, monitorOneDupe);

    // Ensuring monitors are not the same as they have different keys
    // "any.node.domain" compared to "nodeNEW.domain"
    final IMonitor monitorTwo = monitorService.getMonitor(NODE_KEYS, info, propertySet);
    assertNotNull(monitorTwo);
    assertNotEquals(monitorOne, monitorTwo);
  }

  @Test
  void test_7_getMonitorCalledWithSameKeysInDifferentNodeKeys() {
    final Set<String> nodeKeys = new HashSet<>();
    nodeKeys.add("nodeA");

    final Set<String> nodeKeysTwo = new HashSet<>();
    nodeKeysTwo.add("nodeA");
    nodeKeysTwo.add("nodeB");

    final Set<String> nodeKeysThree = new HashSet<>();
    nodeKeysThree.add("nodeB");

    final IMonitor monitorOne = monitorService.getMonitor(nodeKeys, info, propertySet);
    assertNotNull(monitorOne);

    // Add a new key using the same monitor
    // Adding "nodeB" as a new key using the same monitor as "nodeA"
    final IMonitor monitorOneDupe =
        monitorService.getMonitor(nodeKeysTwo, info, propertySet);
    assertEquals(monitorOne, monitorOneDupe);

    // Using new keyset but same node, "nodeB" should return same monitor
    final IMonitor monitorOneDupeAgain =
        monitorService.getMonitor(nodeKeysThree, info, propertySet);
    assertEquals(monitorOne, monitorOneDupeAgain);

    // Make sure createMonitor was called once
    verify(monitorInitializer).createMonitor(
        eq(info),
        eq(propertySet),
        eq(monitorService));
  }

  @Test
  void test_8_startMonitoringNoNodeKeys() {
    final Set<String> nodeKeysEmpty = new HashSet<>();

    assertThrows(IllegalArgumentException.class, () -> monitorService.startMonitoring(
        connection,
        nodeKeysEmpty,
        info,
        propertySet,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT));
  }

  @Test
  void test_9_releaseResourceTwice() {
    // Ensure no NullPointerException.
    monitorService.releaseResources();
    monitorService.releaseResources();
  }
}
