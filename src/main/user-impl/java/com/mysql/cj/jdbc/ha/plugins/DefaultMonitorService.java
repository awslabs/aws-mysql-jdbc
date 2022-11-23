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

import com.mysql.cj.Messages;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executors;

/**
 * This class handles the creation and clean up of monitoring threads to servers with one
 * or more active connections.
 */
public class DefaultMonitorService implements IMonitorService {
  MonitorThreadContainer threadContainer;

  private final Log logger;
  final IMonitorInitializer monitorInitializer;

  public DefaultMonitorService(Log logger) {
    this(
        (hostInfo, propertySet, monitorService) -> new Monitor(
            new BasicConnectionProvider(),
            hostInfo,
            propertySet,
            propertySet.getIntegerProperty(PropertyKey.monitorDisposalTime).getValue(),
            monitorService,
            logger),
        () -> Executors.newCachedThreadPool(r -> {
          final Thread monitoringThread = new Thread(r);
          monitoringThread.setDaemon(true);
          return monitoringThread;
        }),
        logger
    );
  }

  DefaultMonitorService(
      IMonitorInitializer monitorInitializer,
      IExecutorServiceInitializer executorServiceInitializer,
      Log logger) {

    this.monitorInitializer = monitorInitializer;
    this.logger = logger;
    this.threadContainer = MonitorThreadContainer.getInstance(executorServiceInitializer);
  }

  @Override
  public MonitorConnectionContext startMonitoring(
      JdbcConnection connectionToAbort,
      Set<String> nodeKeys,
      HostInfo hostInfo,
      PropertySet propertySet,
      int failureDetectionTimeMillis,
      int failureDetectionIntervalMillis,
      int failureDetectionCount) {

    if (nodeKeys.isEmpty()) {
      final String warning = Messages.getString("DefaultMonitorService.EmptyNodeKeys");
      logger.logWarn(warning);
      throw new IllegalArgumentException(warning);
    }

    final IMonitor monitor = getMonitor(nodeKeys, hostInfo, propertySet);
    this.logger.logTrace("Monitor value: " + monitor == null ? "null" : monitor.toString());

    final MonitorConnectionContext context = new MonitorConnectionContext(
        connectionToAbort,
        nodeKeys,
        logger,
        failureDetectionTimeMillis,
        failureDetectionIntervalMillis,
        failureDetectionCount);

    monitor.startMonitoring(context);

    return context;
  }

  @Override
  public void stopMonitoring(MonitorConnectionContext context) {
    if (context == null) {
      logger.logWarn(NullArgumentMessage.getMessage("context"));
      return;
    }

    context.invalidate();

    // Any 1 node is enough to find the monitor containing the context
    // All nodes will map to the same monitor
    IMonitor monitor;
    for (Iterator<String> it = context.getNodeKeys().iterator(); it.hasNext();) {
      String nodeKey = it.next();
      monitor = this.threadContainer.getMonitor(nodeKey);
      if (monitor != null) {
        monitor.stopMonitoring(context);
        return;
      }
    }
    logger.logTrace(Messages.getString("DefaultMonitorService.NoMonitorForContext"));
  }

  @Override
  public void stopMonitoringForAllConnections(Set<String> nodeKeys) {
    IMonitor monitor;
    for (String nodeKey : nodeKeys) {
      monitor = this.threadContainer.getMonitor(nodeKey);
      if (monitor != null) {
        monitor.clearContexts();
        this.threadContainer.resetResource(monitor);
        return;
      }
    }
  }

  @Override
  public void releaseResources() {
    this.threadContainer = null;
    MonitorThreadContainer.releaseInstance();
  }

  @Override
  public void notifyUnused(IMonitor monitor) {
    if (monitor == null) {
      logger.logWarn(NullArgumentMessage.getMessage("monitor"));
      return;
    }

    // Remove monitor from the maps
    this.threadContainer.releaseResource(monitor);
  }

  /**
   * Get or create a {@link Monitor} for a server.
   *
   * @param nodeKeys All references to the server requiring monitoring.
   * @param hostInfo Information such as hostname of the server.
   * @param propertySet The user configuration for the current connection.
   * @return A {@link Monitor} object associated with a specific server.
   */
  protected IMonitor getMonitor(Set<String> nodeKeys, HostInfo hostInfo, PropertySet propertySet) {
    return this.threadContainer.getOrCreateMonitor(nodeKeys, () -> monitorInitializer.createMonitor(hostInfo, propertySet, this));
  }
}
