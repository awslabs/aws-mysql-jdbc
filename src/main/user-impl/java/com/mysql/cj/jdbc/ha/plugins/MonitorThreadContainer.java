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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * This singleton class keeps track of all the monitoring threads and handles the creation
 * and clean up of each monitoring thread.
 */
public class MonitorThreadContainer {
  private static MonitorThreadContainer singleton = null;
  private static final AtomicInteger CLASS_USAGE_COUNT = new AtomicInteger();
  private final Map<String, IMonitor> monitorMap = new ConcurrentHashMap<>();
  private final Map<IMonitor, Future<?>> tasksMap = new ConcurrentHashMap<>();
  private final Queue<IMonitor> availableMonitors = new ConcurrentLinkedDeque<>();
  private final ExecutorService threadPool;
  private static final Object LOCK_OBJECT = new Object();

  /**
   * Create an instance of the {@link MonitorThreadContainer}.
   *
   * @return a singleton instance of the {@link MonitorThreadContainer}.
   */
  public static MonitorThreadContainer getInstance() {
    return getInstance(Executors::newCachedThreadPool);
  }

  static MonitorThreadContainer getInstance(IExecutorServiceInitializer executorServiceInitializer) {
    if (singleton == null) {
      synchronized (LOCK_OBJECT) {
        singleton = new MonitorThreadContainer(executorServiceInitializer);
      }
      CLASS_USAGE_COUNT.set(0);
    }
    CLASS_USAGE_COUNT.getAndIncrement();
    return singleton;
  }

  /**
   * Release resources held in the {@link MonitorThreadContainer} and clear references
   * to the container.
   */
  public static void releaseInstance() {
    if (singleton == null) {
      return;
    }

    if (CLASS_USAGE_COUNT.decrementAndGet() <= 0) {
      synchronized (LOCK_OBJECT) {
        singleton.releaseResources();
        singleton = null;
      }
    }
  }

  private MonitorThreadContainer(IExecutorServiceInitializer executorServiceInitializer) {
    this.threadPool = executorServiceInitializer.createExecutorService();
  }

  public Map<String, IMonitor> getMonitorMap() {
    return monitorMap;
  }

  public Map<IMonitor, Future<?>> getTasksMap() {
    return tasksMap;
  }

  public ExecutorService getThreadPool() {
    return threadPool;
  }

  String getNode(Set<String> nodeKeys) {
    return getNode(nodeKeys, null);
  }

  String getNode(Set<String> nodeKeys, String defaultValue) {
    return nodeKeys
        .stream().filter(monitorMap::containsKey)
        .findAny()
        .orElse(defaultValue);
  }

  IMonitor getMonitor(String node) {
    return monitorMap.get(node);
  }

  IMonitor getOrCreateMonitor(Set<String> nodeKeys, Supplier<IMonitor> monitorSupplier) {
    final String node = getNode(nodeKeys, nodeKeys.iterator().next());
    final IMonitor monitor = monitorMap.computeIfAbsent(node, k -> {
      if (!availableMonitors.isEmpty()) {
        final IMonitor availableMonitor = availableMonitors.remove();
        if (!availableMonitor.isStopped()) {
          return availableMonitor;
        }
        tasksMap.computeIfPresent(availableMonitor, (key, v) -> {
          v.cancel(true);
          return null;
        });
      }

      return monitorSupplier.get();
    });

    populateMonitorMap(nodeKeys, monitor);
    return monitor;
  }

  private void populateMonitorMap(Set<String> nodeKeys, IMonitor monitor) {
    for (String nodeKey : nodeKeys) {
      monitorMap.putIfAbsent(nodeKey, monitor);
    }
  }

  void addTask(IMonitor monitor) {
    tasksMap.computeIfAbsent(monitor, k -> threadPool.submit(monitor));
  }

  /**
   * Clear all references used by the given monitor.
   * Put the monitor in to a queue waiting to be reused.
   *
   * @param monitor The monitor to reset.
   */
  public void resetResource(IMonitor monitor) {
    if (monitor == null) {
      return;
    }

    final List<IMonitor> monitorList = Collections.singletonList(monitor);
    monitorMap.values().removeAll(monitorList);
    availableMonitors.add(monitor);
  }

  /**
   * Remove references to the given {@link Monitor} object and stop the background monitoring
   * thread.
   *
   * @param monitor The {@link Monitor} representing a monitoring thread.
   */
  public void releaseResource(IMonitor monitor) {
    if (monitor == null) {
      return;
    }

    final List<IMonitor> monitorList = Collections.singletonList(monitor);
    monitorMap.values().removeAll(monitorList);
    tasksMap.computeIfPresent(monitor, (k, v) -> {
      v.cancel(true);
      return null;
    });
  }

  private void releaseResources() {
    monitorMap.clear();
    tasksMap.values().stream()
        .filter(val -> !val.isDone() && !val.isCancelled())
        .forEach(val -> val.cancel(true));

    if (threadPool != null) {
      threadPool.shutdownNow();
    }
  }
}
