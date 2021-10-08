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

import com.mysql.cj.log.Log;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

class DefaultMonitorServiceTest {
  private final static String NODE = "node.domain";
  private final static int FAILURE_DETECTION_TIME_MILLIS = 10;
  private final static int FAILURE_DETECTION_INTERVAL_MILLIS = 100;
  private final static int FAILURE_DETECTION_COUNT = 3;

  @Mock private MonitorInitializer monitorInitializer;
  @Mock private ThreadInitializer threadInitializer;
  @Mock private Log logger;
  @Mock private IMonitor monitor;
  @Mock private Thread thread;

  private AutoCloseable closeable;
  private DefaultMonitorService monitorService;
  private ArgumentCaptor<MonitorConnectionContext> contextCaptor;
  private ArgumentCaptor<DefaultMonitorService> serviceCaptor;
  private ArgumentCaptor<IMonitor> monitorCaptor;

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);

    serviceCaptor = ArgumentCaptor.forClass(DefaultMonitorService.class);
    monitorCaptor = ArgumentCaptor.forClass(IMonitor.class);
    contextCaptor = ArgumentCaptor.forClass(MonitorConnectionContext.class);

    Mockito
        .when(monitorInitializer.createMonitor(serviceCaptor.capture()))
        .thenReturn(monitor);
    Mockito
        .when(threadInitializer.createThread(monitorCaptor.capture()))
        .thenReturn(thread);

    monitorService = new DefaultMonitorService(
        NODE,
        monitorInitializer,
        threadInitializer,
        logger);
  }

  @AfterEach
  void cleanUp() throws Exception {
    DefaultMonitorService.MONITOR_MAPPING.clear();
    DefaultMonitorService.THREAD_MAPPING.clear();
    closeable.close();
  }

  @Test
  void test_1_successfulInitialization() {
    Assertions.assertEquals(monitorService, serviceCaptor.getValue());
    Assertions.assertEquals(monitor, monitorCaptor.getValue());
  }

  @Test
  void test_2_startMonitoringWithNoThread() {
    Mockito.doNothing().when(monitor).startMonitoring(contextCaptor.capture());
    Mockito.when(thread.getState()).thenReturn(Thread.State.NEW);

    monitorService.startMonitoring(
        NODE,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT);

    Assertions.assertNotNull(contextCaptor.getValue());
    Mockito.verify(thread).start();
  }

  @Test
  void test_3_startMonitoringWithThread() {
    Mockito.doNothing().when(monitor).startMonitoring(contextCaptor.capture());
    Mockito.when(thread.getState()).thenReturn(Thread.State.RUNNABLE);

    monitorService.startMonitoring(
        NODE,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT);

    Assertions.assertNotNull(contextCaptor.getValue());
    Mockito.verify(thread, Mockito.times(0)).start();
  }

  @Test
  void test_4_stopMonitoringWithRunningThread() {
    Mockito.when(thread.isInterrupted()).thenReturn(false);
    Mockito.doNothing().when(monitor).stopMonitoring(contextCaptor.capture());

    final MonitorConnectionContext context = monitorService.startMonitoring(
        NODE,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT);

    monitorService.stopMonitoring(NODE, context);

    Assertions.assertEquals(context, contextCaptor.getValue());
    Mockito.verify(monitor).stopMonitoring(Mockito.any());
    Mockito.verify(thread).isInterrupted();
    Mockito.verify(thread).interrupt();
  }

  @Test
  void test_5_stopMonitoringWithInterruptedThread() {
    Mockito.when(thread.isInterrupted()).thenReturn(true);
    Mockito.doNothing().when(monitor).stopMonitoring(contextCaptor.capture());

    final MonitorConnectionContext context = monitorService.startMonitoring(
        NODE,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT);

    monitorService.stopMonitoring(NODE, context);

    Assertions.assertEquals(context, contextCaptor.getValue());
    Mockito.verify(monitor).stopMonitoring(Mockito.any());
    Mockito.verify(thread).isInterrupted();
    Mockito.verify(thread, Mockito.never()).interrupt();
  }

  @Test
  void test_6_stopMonitoringCalledTwice() {
    Mockito.when(thread.isInterrupted()).thenReturn(false);
    Mockito.doNothing().when(monitor).stopMonitoring(contextCaptor.capture());

    final MonitorConnectionContext context = monitorService.startMonitoring(
        NODE,
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT);

    monitorService.stopMonitoring(NODE, context);

    Assertions.assertEquals(context, contextCaptor.getValue());

    Mockito.verify(monitor).stopMonitoring(Mockito.any());
    monitorService.stopMonitoring(NODE, context);

    Mockito.verify(thread).interrupt();
    Mockito.verify(thread).isInterrupted();
  }
}