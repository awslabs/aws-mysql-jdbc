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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DefaultMonitorService implements IMonitorService {
  protected static final Map<String, IMonitor> MONITOR_MAP = new ConcurrentHashMap<>();
  protected static final Map<String, ScheduledExecutorService> EXECUTOR_SERVICE_MAP = new ConcurrentHashMap<>();
  protected static final Map<IMonitor, ScheduledFuture<?>> TASKS_MAP = new ConcurrentHashMap<>();
  private final Log log;

  public DefaultMonitorService(HostInfo hostInfo, PropertySet propertySet, Log log) {
    this(
        hostInfo.getHost(),
        () -> new Monitor(hostInfo, propertySet, log),
        Executors::newSingleThreadScheduledExecutor,
        log
    );
  }

  DefaultMonitorService(
      String node,
      IMonitorInitializer monitorInitializer,
      IExecutorServiceInitializer threadInitializer,
      Log log) {

    MONITOR_MAP.putIfAbsent(
        node,
        monitorInitializer.createMonitor());

   EXECUTOR_SERVICE_MAP.putIfAbsent(
        node,
        threadInitializer.createExecutorService());
    this.log = log;
  }

  @Override
  public MonitorConnectionContext startMonitoring(
      String node,
      int failureDetectionTimeMillis,
      int failureDetectionIntervalMillis,
      int failureDetectionCount) {

    final MonitorConnectionContext context = new MonitorConnectionContext(
        node,
        log,
        failureDetectionTimeMillis,
        failureDetectionIntervalMillis,
        failureDetectionCount);

    final IMonitor monitor = MONITOR_MAP.get(node);
    final ScheduledExecutorService executor = EXECUTOR_SERVICE_MAP.get(node);

    monitor.startMonitoring(context);

    final ScheduledFuture<?> executorService = TASKS_MAP.get(monitor);
    if (executorService == null) {
      TASKS_MAP.put(
          monitor,
          executor.schedule(monitor, failureDetectionIntervalMillis, TimeUnit.MILLISECONDS));
    }

    return context;
  }

  @Override
  public void stopMonitoring(MonitorConnectionContext context) {
    final IMonitor monitor = MONITOR_MAP.get(context.getNode());
    monitor.stopMonitoring(context);
  }
}
