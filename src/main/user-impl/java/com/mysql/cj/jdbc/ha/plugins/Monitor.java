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

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class uses a background thread to monitor a particular server with one or more
 * active {@link Connection}.
 */
public class Monitor implements IMonitor {
  static class ConnectionStatus {
    boolean isValid;
    long elapsedTime;

    ConnectionStatus(boolean isValid, long elapsedTime) {
      this.isValid = isValid;
      this.elapsedTime = elapsedTime;
    }
  }

  private static final int THREAD_SLEEP_WHEN_INACTIVE_MILLIS = 100;
  private static final String MONITORING_PROPERTY_PREFIX = "monitoring-";

  private final Queue<MonitorConnectionContext> contexts = new ConcurrentLinkedQueue<>();
  private final IConnectionProvider connectionProvider;
  private final Log logger;
  private final PropertySet propertySet;
  private final HostInfo hostInfo;
  private Connection monitoringConn = null;
  private long connectionCheckIntervalMillis = Integer.MAX_VALUE;
  private final AtomicLong lastContextUsedTimestamp = new AtomicLong();
  private final long monitorDisposalTime;
  private final IMonitorService monitorService;
  private final AtomicBoolean stopped = new AtomicBoolean(true);

  /**
   * Store the monitoring configuration for a connection.
   *
   * @param connectionProvider A provider for creating new connections.
   * @param hostInfo The {@link HostInfo} of the server this {@link Monitor} instance is
   *                 monitoring.
   * @param propertySet The {@link PropertySet} containing additional monitoring configuration.
   * @param monitorDisposalTime Time before stopping the monitoring thread where there are
   *                            no active connection to the server this {@link Monitor}
   *                            instance is monitoring.
   * @param monitorService A reference to the {@link DefaultMonitorService} implementation
   *                       that initialized this class.
   * @param logger A {@link Log} implementation.
   */
  public Monitor(
      IConnectionProvider connectionProvider,
      HostInfo hostInfo,
      PropertySet propertySet,
      long monitorDisposalTime,
      IMonitorService monitorService,
      Log logger) {
    this.connectionProvider = connectionProvider;
    this.hostInfo = hostInfo;
    this.propertySet = propertySet;
    this.logger = logger;
    this.monitorDisposalTime = monitorDisposalTime;
    this.monitorService = monitorService;

    this.lastContextUsedTimestamp.set(this.getCurrentTimeNano());
  }

  long getCurrentTimeNano() {
    return System.nanoTime();
  }

  @Override
  public void startMonitoring(MonitorConnectionContext context) {
    this.connectionCheckIntervalMillis = Math.min(
        this.connectionCheckIntervalMillis,
        context.getFailureDetectionIntervalMillis());
    final long currentTime = this.getCurrentTimeNano();
    context.setStartMonitorTime(currentTime);
    this.lastContextUsedTimestamp.set(currentTime);
    this.contexts.add(context);
  }

  @Override
  public void stopMonitoring(MonitorConnectionContext context) {
    if (context == null) {
      logger.logWarn(NullArgumentMessage.getMessage("context"));
      return;
    }

    context.invalidate();
    this.contexts.remove(context);

    this.connectionCheckIntervalMillis = findShortestIntervalMillis();
  }

  public synchronized void clearContexts() {
    this.contexts.clear();
    this.connectionCheckIntervalMillis = findShortestIntervalMillis();
  }

  @Override
  public void run() {
    try {
      this.stopped.set(false);
      while (true) {
        if (!this.contexts.isEmpty()) {
          final long statusCheckStartTime = this.getCurrentTimeNano();
          this.lastContextUsedTimestamp.set(statusCheckStartTime);

          final ConnectionStatus status =
              checkConnectionStatus(this.getConnectionCheckIntervalMillis());

          for (MonitorConnectionContext monitorContext : this.contexts) {
            monitorContext.updateConnectionStatus(
                statusCheckStartTime,
                statusCheckStartTime + status.elapsedTime,
                status.isValid);
          }

          TimeUnit.MILLISECONDS.sleep(Math.max(
              0,
              this.getConnectionCheckIntervalMillis() - status.elapsedTime));
        } else {
          if ((this.getCurrentTimeNano() - this.lastContextUsedTimestamp.get())
              >= TimeUnit.MILLISECONDS.toNanos(this.monitorDisposalTime)) {
            monitorService.notifyUnused(this);
            break;
          }
          TimeUnit.MILLISECONDS.sleep(THREAD_SLEEP_WHEN_INACTIVE_MILLIS);
        }
      }
    } catch (InterruptedException intEx) {
      // do nothing; exit thread
    } finally {
      if (this.monitoringConn != null) {
        try {
          this.monitoringConn.close();
        } catch (SQLException ex) {
          // ignore
        }
      }
      this.stopped.set(true);
    }
  }

  /**
   * Check the status of the monitored server by sending a ping.
   *
   * @param shortestFailureDetectionIntervalMillis The shortest failure detection interval
   *                                               used by all the connections to this server.
   *                                               This value is used as the maximum time
   *                                               to wait for a response from the server.
   * @return whether the server is still alive and the elapsed time spent checking.
   */
  ConnectionStatus checkConnectionStatus(final long shortestFailureDetectionIntervalMillis) {
    long start = this.getCurrentTimeNano();
    try {
      if (this.monitoringConn == null || this.monitoringConn.isClosed()) {
        // open a new connection
        Map<String, String> monitoringConnProperties = new HashMap<>();
        // Default values for connect and socket timeout
        final String defaultTimeout = Long.toString(shortestFailureDetectionIntervalMillis);
        monitoringConnProperties.put(PropertyKey.connectTimeout.getKeyName(), defaultTimeout);
        monitoringConnProperties.put(PropertyKey.socketTimeout.getKeyName(), defaultTimeout);
        Properties originalProperties = this.propertySet.exposeAsProperties();
        if (originalProperties != null) {
          originalProperties.stringPropertyNames().stream()
              .filter(p -> p.startsWith(MONITORING_PROPERTY_PREFIX))
              .forEach(p -> monitoringConnProperties.put(
                  p.substring(MONITORING_PROPERTY_PREFIX.length()),
                  originalProperties.getProperty(p)));
        }

        start = this.getCurrentTimeNano();
        this.monitoringConn = this.connectionProvider.connect(copy(
            this.hostInfo,
            monitoringConnProperties));
        return new ConnectionStatus(true, this.getCurrentTimeNano() - start);
      }

      start = this.getCurrentTimeNano();
      boolean isValid =
          this.monitoringConn.isValid((int) TimeUnit.MILLISECONDS.toSeconds(shortestFailureDetectionIntervalMillis));
      return new ConnectionStatus(isValid, this.getCurrentTimeNano() - start);
    } catch (SQLException sqlEx) {
      //this.logger.logTrace(String.format("[Monitor] Error checking connection status: %s", sqlEx.getMessage()));
      return new ConnectionStatus(false, this.getCurrentTimeNano() - start);
    }
  }

  long getConnectionCheckIntervalMillis() {
    if (this.connectionCheckIntervalMillis == Integer.MAX_VALUE) {
      // connectionCheckIntervalMillis is at Integer.MAX_VALUE because there are no contexts available.
      return 0;
    }
    return this.connectionCheckIntervalMillis;
  }

  @Override
  public boolean isStopped() {
    return this.stopped.get();
  }

  private HostInfo copy(HostInfo src, Map<String, String> props) {
    return new HostInfo(
        null,
        src.getHost(),
        src.getPort(),
        src.getUser(),
        src.getPassword(),
        props);
  }

  private long findShortestIntervalMillis() {
    long currentMin = Long.MAX_VALUE;
    for (MonitorConnectionContext context : this.contexts) {
      currentMin = Math.min(currentMin, context.getFailureDetectionIntervalMillis());
    }
    return currentMin;
  }
}
