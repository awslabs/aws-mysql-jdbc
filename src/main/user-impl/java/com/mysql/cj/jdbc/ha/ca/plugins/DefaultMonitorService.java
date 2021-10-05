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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultMonitorService implements IMonitorService {
  private static final Map<String, Monitor> MONITOR_MAPPING = new ConcurrentHashMap<>();
  private static final Map<String, Thread> THREAD_MAPPING = new ConcurrentHashMap<>();
  private static final Map<String, List<MonitorConfig>> CONFIG_MAPPING = new ConcurrentHashMap<>();

  @Override
  public Monitor createMonitorIfAbsent(
      String node,
      MonitorInitializer initializer) {

    return MONITOR_MAPPING.putIfAbsent(
        node,
        initializer.createMonitor(this));
  }

  @Override
  public Monitor getMonitor(String node) {
    return MONITOR_MAPPING.get(node);
  }

  @Override
  public boolean addMonitorConfig(String node, MonitorConfig config) {
    final List<MonitorConfig> configs =
        CONFIG_MAPPING.computeIfAbsent(node, arr -> new ArrayList<>());
    return configs.add(config);
  }

  @Override
  public Thread startNewThreadIfAbsent(String node, ThreadInitializer threadInitializer) {
    final Monitor monitor = MONITOR_MAPPING.get(node);
    if (monitor == null) {
      // Not sure if this null check is necessary
      // Throw an exception or not?
      System.out.println("CREATE A MONITOR FIRST");
    }

    Thread thread = THREAD_MAPPING.putIfAbsent(node, threadInitializer.startThread(monitor));
    thread.start();
    return thread;
  }

  @Override
  public List<MonitorConfig> getMonitorConfigs(String node) {
    return CONFIG_MAPPING.computeIfAbsent(node, arr -> new ArrayList<>());
  }

  @Override
  public boolean isMonitoringNode(String node) {
    return MONITOR_MAPPING.containsKey(node) & THREAD_MAPPING.containsKey(node);
  }

  @Override
  public void releaseMonitor(String node) {
    Monitor monitor = MONITOR_MAPPING.get(node);
    if (monitor == null) {
      return;
    }

    monitor.stopMonitoring();
    MONITOR_MAPPING.remove(node);
    removeMonitorConfigs(node);
  }

  @Override
  public void releaseThread(String node) {
    Thread thread = THREAD_MAPPING.get(node);
    if (thread == null || thread.isInterrupted()) {
      return;
    }

    thread.interrupt();
    THREAD_MAPPING.remove(node);
    removeMonitorConfigs(node);
  }

  private void removeMonitorConfigs(String node) {
    List<MonitorConfig> configs = CONFIG_MAPPING.get(node);
    if (configs == null || configs.isEmpty()) {
      return;
    }

    CONFIG_MAPPING.remove(node);
  }
}
