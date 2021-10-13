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

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

class DefaultMonitorServiceTest {
  private static final String NODE = "node.domain";
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
  private IMonitor monitor;
  @Mock
  private ExecutorService executorService;
  @Mock
  private Future<?> task;
  @Mock
  private HostInfo info;
  @Mock
  private PropertySet set;

  private AutoCloseable closeable;
  private DefaultMonitorService monitorService;
  private ArgumentCaptor<MonitorConnectionContext> contextCaptor;

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
    contextCaptor = ArgumentCaptor.forClass(MonitorConnectionContext.class);

    Mockito
        .when(monitorInitializer.createMonitor(
            Mockito.any(HostInfo.class),
            Mockito.any(PropertySet.class)))
        .thenReturn(monitor);

    Mockito
        .when(executorServiceInitializer.createExecutorService())
        .thenReturn(executorService);

    Mockito
        .doReturn(task)
        .when(executorService)
        .submit(Mockito.any(IMonitor.class));

    monitorService = new DefaultMonitorService(
        monitorInitializer,
        executorServiceInitializer,
        logger);
  }

  @AfterEach
  void cleanUp() throws Exception {
    DefaultMonitorService.MONITOR_MAP.clear();
    DefaultMonitorService.TASKS_MAP.clear();
    DefaultMonitorService.threadPool = null;
    closeable.close();
  }

  @Test
  void test_1_startMonitoringWithNoExecutor() {
    Mockito.doNothing().when(monitor).startMonitoring(contextCaptor.capture());

    monitorService.startMonitoring(
        NODE,
        info,
        set,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT);

    Assertions.assertNotNull(contextCaptor.getValue());
    Mockito
        .verify(executorService)
        .submit(Mockito.eq(monitor));
  }

  @Test
  void test_2_startMonitoringCalledMultipleTimes() {
    Mockito.doNothing().when(monitor).startMonitoring(contextCaptor.capture());

    final int runs = 5;

    for (int i = 0; i < runs; i++) {
      monitorService.startMonitoring(
          NODE,
          info,
          set,
          FAILURE_DETECTION_TIME_MILLIS,
          FAILURE_DETECTION_INTERVAL_MILLIS,
          FAILURE_DETECTION_COUNT);
    }

    Assertions.assertNotNull(contextCaptor.getValue());

    // executorService should only be called once.
    Mockito
        .verify(executorService)
        .submit(Mockito.eq(monitor));
  }

  @Test
  void test_3_stopMonitoringWithInterruptedThread() {
    Mockito.doNothing().when(monitor).stopMonitoring(contextCaptor.capture());

    final MonitorConnectionContext context = monitorService.startMonitoring(
        NODE,
        info,
        set,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT);

    monitorService.stopMonitoring(context);

    Assertions.assertEquals(context, contextCaptor.getValue());
    Mockito.verify(monitor).stopMonitoring(Mockito.any());
  }

  @Test
  void test_4_stopMonitoringCalledTwice() {
    Mockito.doNothing().when(monitor).stopMonitoring(contextCaptor.capture());

    final MonitorConnectionContext context = monitorService.startMonitoring(
        NODE,
        info,
        set,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT);

    monitorService.stopMonitoring(context);

    Assertions.assertEquals(context, contextCaptor.getValue());

    Mockito.verify(monitor).stopMonitoring(Mockito.any());
    monitorService.stopMonitoring(context);
  }
}
