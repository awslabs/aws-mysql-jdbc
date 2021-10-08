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

public class DefaultMonitorService implements IMonitorService {
  protected static final Map<String, IMonitor> MONITOR_MAPPING = new ConcurrentHashMap<>();
  protected static final Map<String, Thread> THREAD_MAPPING = new ConcurrentHashMap<>();
  private final Log log;

  public DefaultMonitorService(HostInfo hostInfo, PropertySet propertySet, Log log) {
    this(
        hostInfo.getHost(),
        ((service) -> new Monitor(hostInfo, propertySet, log)),
        Thread::new,
        log
    );
  }

  DefaultMonitorService(
      String node,
      IMonitorInitializer monitorInitializer,
      IThreadInitializer threadInitializer,
      Log log) {
    final IMonitor monitor = MONITOR_MAPPING.computeIfAbsent(
        node,
        k -> monitorInitializer.createMonitor(this));

   THREAD_MAPPING.putIfAbsent(
        node,
        threadInitializer.createThread(monitor));
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

    MONITOR_MAPPING.get(node).startMonitoring(context);
    final Thread thread = THREAD_MAPPING.get(node);
    if (Thread.State.NEW.equals(thread.getState())) {
      thread.start();
    }

    return context;
  }

  @Override
  public void stopMonitoring(String node, MonitorConnectionContext context) {
    final IMonitor monitor = MONITOR_MAPPING.get(node);
    monitor.stopMonitoring(context);

    final Thread thread = THREAD_MAPPING.get(node);
    if (thread == null || thread.isInterrupted()) {
      return;
    }

    thread.interrupt();
    THREAD_MAPPING.remove(node);
  }
}
