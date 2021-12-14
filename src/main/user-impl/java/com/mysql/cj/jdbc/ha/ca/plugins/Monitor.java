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
import com.mysql.cj.jdbc.ha.ca.ConnectionProvider;
import com.mysql.cj.log.Log;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Comparator;
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
  private final ConnectionProvider connectionProvider;
  private final Log logger;
  private final PropertySet propertySet;
  private final HostInfo hostInfo;
  private Connection monitoringConn = null;
  private int connectionCheckIntervalMillis = Integer.MAX_VALUE;
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
      ConnectionProvider connectionProvider,
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
  }

  @Override
  public synchronized void startMonitoring(MonitorConnectionContext context) {
    this.connectionCheckIntervalMillis = Math.min(
        this.connectionCheckIntervalMillis,
        context.getFailureDetectionIntervalMillis());
    final long currentTime = this.getCurrentTimeMillis();
    context.setStartMonitorTime(currentTime);
    this.lastContextUsedTimestamp.set(currentTime);
    this.contexts.add(context);
  }

  @Override
  public synchronized void stopMonitoring(MonitorConnectionContext context) {
    if (context == null) {
      logger.logWarn(NullArgumentMessage.getMessage("context"));
      return;
    }
    synchronized (context) {
      this.contexts.remove(context);
      context.invalidate();
    }
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
          final long currentTime = this.getCurrentTimeMillis();
          this.lastContextUsedTimestamp.set(currentTime);

          final ConnectionStatus status =
              checkConnectionStatus(this.getConnectionCheckIntervalMillis());

          for (MonitorConnectionContext monitorContext : this.contexts) {
            monitorContext.updateConnectionStatus(
                currentTime,
                status.isValid);
          }

          TimeUnit.MILLISECONDS.sleep(Math.max(
              0,
              this.getConnectionCheckIntervalMillis() - status.elapsedTime));
        } else {
          if ((this.getCurrentTimeMillis() - this.lastContextUsedTimestamp.get())
              >= this.monitorDisposalTime) {
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
  ConnectionStatus checkConnectionStatus(final int shortestFailureDetectionIntervalMillis) {
    long start = this.getCurrentTimeMillis();
    try {
      if (this.monitoringConn == null || this.monitoringConn.isClosed()) {
        // open a new connection
        Map<String, String> monitoringConnProperties = new HashMap<>();
        Properties originalProperties = this.propertySet.exposeAsProperties();
        if (originalProperties != null) {
          originalProperties.stringPropertyNames().stream()
              .filter(p -> p.startsWith(MONITORING_PROPERTY_PREFIX))
              .forEach(p -> monitoringConnProperties.put(
                  p.substring(MONITORING_PROPERTY_PREFIX.length()),
                  originalProperties.getProperty(p)));
        }

        start = this.getCurrentTimeMillis();
        this.monitoringConn = this.connectionProvider.connect(copy(
            this.hostInfo,
            monitoringConnProperties));
        return new ConnectionStatus(true, this.getCurrentTimeMillis() - start);
      }

      start = this.getCurrentTimeMillis();
      boolean isValid =
          this.monitoringConn.isValid(shortestFailureDetectionIntervalMillis / 1000);
      return new ConnectionStatus(isValid, this.getCurrentTimeMillis() - start);
    } catch (SQLException sqlEx) {
      //this.logger.logTrace(String.format("[Monitor] Error checking connection status: %s", sqlEx.getMessage()));
      return new ConnectionStatus(false, this.getCurrentTimeMillis() - start);
    }
  }

  // This method helps to organize unit tests.
  long getCurrentTimeMillis() {
    return System.currentTimeMillis();
  }

  int getConnectionCheckIntervalMillis() {
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
        src.isPasswordless(),
        props);
  }

  private int findShortestIntervalMillis() {
    if (this.contexts.isEmpty()) {
      return Integer.MAX_VALUE;
    }

    return this.contexts.stream()
        .min(Comparator.comparing(MonitorConnectionContext::getFailureDetectionIntervalMillis))
        .map(MonitorConnectionContext::getFailureDetectionIntervalMillis)
        .orElse(0);
  }
}
