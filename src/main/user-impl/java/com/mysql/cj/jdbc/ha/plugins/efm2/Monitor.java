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

package com.mysql.cj.jdbc.ha.plugins.efm2;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.ha.plugins.IConnectionProvider;
import com.mysql.cj.log.Log;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class uses a background thread to monitor a particular server with one or more
 * active {@link Connection}.
 */
public class Monitor implements IMonitor {

  protected static final String MONITORING_PROPERTY_PREFIX = "monitoring-";
  protected static final long THREAD_SLEEP_NANO = TimeUnit.MILLISECONDS.toNanos(1000);
  protected static final Executor ABORT_EXECUTOR = Executors.newSingleThreadExecutor();

  private final Queue<WeakReference<MonitorConnectionContext>> activeContexts = new ConcurrentLinkedQueue<>();
  private final HashMap<Long, Queue<WeakReference<MonitorConnectionContext>>> newContexts = new HashMap<>();

  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private Connection monitoringConn = null;
  private final ExecutorService threadPool = Executors.newFixedThreadPool(2, runnableTarget -> {
    final Thread monitoringThread = new Thread(runnableTarget);
    monitoringThread.setDaemon(true);
    return monitoringThread;
  });

  private final long failureDetectionTimeNano;
  private final long failureDetectionIntervalNano;
  private final int failureDetectionCount;

  private long invalidNodeStartTimeNano;
  private long failureCount;
  private boolean nodeUnhealthy = false;

  private final IConnectionProvider connectionProvider;
  private final Log logger;
  private final PropertySet propertySet;
  private final HostInfo hostInfo;
  private final String defaultTimeoutMillis;


  /**
   * Store the monitoring configuration for a connection.
   *
   * @param connectionProvider A provider for creating new connections.
   * @param hostInfo The {@link HostInfo} of the server this {@link Monitor} instance is
   *                 monitoring.
   * @param propertySet The {@link PropertySet} containing additional monitoring configuration.
   * @param failureDetectionTimeMillis Grace period after which node monitoring starts.
   * @param failureDetectionIntervalMillis Interval between each failed connection check.
   * @param failureDetectionCount Number of failed connection checks before considering
   *                              database node as unhealthy.
   * @param logger A {@link Log} implementation.
   */
  public Monitor(
      final IConnectionProvider connectionProvider,
      final HostInfo hostInfo,
      final PropertySet propertySet,
      final int failureDetectionTimeMillis,
      final int failureDetectionIntervalMillis,
      final int failureDetectionCount,
      final Log logger) {

    this.connectionProvider = connectionProvider;
    this.hostInfo = hostInfo;
    this.propertySet = propertySet;
    this.logger = logger;
    this.failureDetectionTimeNano = TimeUnit.MILLISECONDS.toNanos(failureDetectionTimeMillis);
    this.failureDetectionIntervalNano = TimeUnit.MILLISECONDS.toNanos(failureDetectionIntervalMillis);
    this.failureDetectionCount = failureDetectionCount;
    this.defaultTimeoutMillis = String.valueOf(failureDetectionIntervalMillis);

    this.threadPool.submit(this::newContextRun); // task to handle new contexts
    this.threadPool.submit(this); // task to handle active monitoring contexts
    this.threadPool.shutdown(); // No more tasks are accepted by pool.
  }

  @Override
  public boolean canDispose() {
    return this.activeContexts.isEmpty() && this.newContexts.isEmpty();
  }

  @Override
  public void close() throws Exception {
    this.stopped.set(true);

    // Waiting for 30s gives a thread enough time to exit monitoring loop and close database connection.
    if (!this.threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
      this.threadPool.shutdownNow();
    }
    if (this.logger.isTraceEnabled()) {
      this.logger.logTrace(
          String.format(
              "[efm2.Monitor.close]: Stopped monitoring thread for node '%s'.",
              this.hostInfo.getHostPortPair()));
    }
  }

  @Override
  public void startMonitoring(final MonitorConnectionContext context) {
    if (this.stopped.get()) {
      this.logger.logWarn(
          String.format(
              "[efm2.Monitor.startMonitoring]: Monitoring was already stopped for node %s.",
              this.hostInfo.getHostPortPair()));
    }

    final long currentTimeNano = this.getCurrentTimeNano();
    long startMonitoringTimeNano = this.truncateNanoToSeconds(
        currentTimeNano + this.failureDetectionTimeNano);

    Queue<WeakReference<MonitorConnectionContext>> queue =
        this.newContexts.computeIfAbsent(
            startMonitoringTimeNano,
            (key) -> new ConcurrentLinkedQueue<>());
    queue.add(new WeakReference<>(context));
  }

  private long truncateNanoToSeconds(final long timeNano) {
    return TimeUnit.SECONDS.toNanos(TimeUnit.NANOSECONDS.toSeconds(timeNano));
  }

  public void clearContexts() {
    this.newContexts.clear();
    this.activeContexts.clear();
  }

  // This method helps to organize unit tests.
  long getCurrentTimeNano() {
    return System.nanoTime();
  }

  public void newContextRun() {

    try {
      while (!this.stopped.get()) {

        final long currentTimeNano = this.getCurrentTimeNano();

        final ArrayList<Long> processedKeys = new ArrayList<>();
        this.newContexts.entrySet().stream()
            // Get entries with key (that is a time in nanos) less or equal than current time.
            .filter(entry -> entry.getKey() < currentTimeNano)
            .forEach(entry -> {
              final Queue<WeakReference<MonitorConnectionContext>> queue = entry.getValue();
              processedKeys.add(entry.getKey());
              // Each value of found entry is a queue of monitoring contexts awaiting active monitoring.
              // Add all contexts to an active monitoring contexts queue.
              // Ignore disposed contexts.
              WeakReference<MonitorConnectionContext> contextWeakRef;
              while ((contextWeakRef = queue.poll()) != null) {
                MonitorConnectionContext context = contextWeakRef.get();
                if (context != null && context.isActive()) {
                  this.activeContexts.add(contextWeakRef);
                }
              }
            });
        processedKeys.forEach(this.newContexts::remove);

        TimeUnit.SECONDS.sleep(1);
      }
    } catch (final InterruptedException intEx) {
      // do nothing; just exit the thread
    } catch (final Exception ex) {
      // this should not be reached; log and exit thread
      if (this.logger.isTraceEnabled()) {
        this.logger.logTrace(
            String.format(
                "[efm2.Monitor.newContextRun]: Stopping monitoring after unhandled exception was thrown in monitoring thread for node %s.",
                this.hostInfo.getHostPortPair()),
            ex); // We want to print full trace stack of the exception.
      }
    }
  }

  @Override
  public void run() {

    try {
      while (!this.stopped.get()) {

        if (this.activeContexts.isEmpty()) {
          TimeUnit.NANOSECONDS.sleep(THREAD_SLEEP_NANO);
          continue;
        }

        final long statusCheckStartTimeNano = this.getCurrentTimeNano();
        final boolean isValid = this.checkConnectionStatus();
        final long statusCheckEndTimeNano = this.getCurrentTimeNano();

        this.updateNodeHealthStatus(isValid, statusCheckStartTimeNano, statusCheckEndTimeNano);

        final List<WeakReference<MonitorConnectionContext>> tmpActiveContexts = new ArrayList<>();
        WeakReference<MonitorConnectionContext> monitorContextWeakRef;

        while ((monitorContextWeakRef = this.activeContexts.poll()) != null) {
          if (this.stopped.get()) {
            break;
          }

          MonitorConnectionContext monitorContext = monitorContextWeakRef.get();
          if (monitorContext == null) {
            continue;
          }

          if (this.nodeUnhealthy) {
            // Kill connection.
            monitorContext.setNodeUnhealthy(true);
            final Connection connectionToAbort = monitorContext.getConnection();
            monitorContext.setInactive();
            if (connectionToAbort != null) {
              this.abortConnection(connectionToAbort);
            }
          } else if (monitorContext.isActive()) {
            tmpActiveContexts.add(monitorContextWeakRef);
          }
        }

        // activeContexts is empty now and tmpActiveContexts contains all yet active contexts
        // Add active contexts back to the queue.
        this.activeContexts.addAll(tmpActiveContexts);

        long delayNano = this.failureDetectionIntervalNano - (statusCheckEndTimeNano - statusCheckStartTimeNano);
        if (delayNano < THREAD_SLEEP_NANO) {
          delayNano = THREAD_SLEEP_NANO;
        }
        TimeUnit.NANOSECONDS.sleep(delayNano);
      }
    } catch (final InterruptedException intEx) {
      // do nothing
    } catch (final Exception ex) {
      // this should not be reached; log and exit thread
      if (this.logger.isTraceEnabled()) {
        this.logger.logTrace(
            String.format(
                "[efm2.Monitor.run]: Stopping monitoring after unhandled exception was thrown in monitoring thread for node %s.",
                this.hostInfo.getHostPortPair()),
            ex); // We want to print full trace stack of the exception.
      }
    } finally {
      this.stopped.set(true);
      if (this.monitoringConn != null) {
        try {
          this.monitoringConn.close();
        } catch (final SQLException ex) {
          // ignore
        }
      }
    }
  }

  /**
   * Check the status of the monitored server by establishing a connection and sending a ping.
   *
   * @return True, if the server is still alive.
   */
  boolean checkConnectionStatus() {
    try {
      if (this.monitoringConn == null || this.monitoringConn.isClosed()) {
        // open a new connection
        Map<String, String> monitoringConnProperties = new HashMap<>();

        // Default values for connect and socket timeout
        monitoringConnProperties.put(PropertyKey.connectTimeout.getKeyName(), defaultTimeoutMillis);
        monitoringConnProperties.put(PropertyKey.socketTimeout.getKeyName(), defaultTimeoutMillis);

        Properties originalProperties = this.propertySet.exposeAsProperties();
        if (originalProperties != null) {
          originalProperties.stringPropertyNames().stream()
              .filter(p -> p.startsWith(MONITORING_PROPERTY_PREFIX))
              .forEach(p -> monitoringConnProperties.put(
                  p.substring(MONITORING_PROPERTY_PREFIX.length()),
                  originalProperties.getProperty(p)));
        }


        if (this.logger.isTraceEnabled()) {
          this.logger.logTrace(
              "[efm2.Monitor.checkConnectionStatus]: Opening a monitoring connection to "
                  + this.hostInfo.getHostPortPair());
        }

        this.monitoringConn = this.connectionProvider.connect(copy(
            this.hostInfo,
            monitoringConnProperties));

        if (this.logger.isTraceEnabled()) {
          this.logger.logTrace(
              "[efm2.Monitor.checkConnectionStatus]: Opened monitoring connection: "
                  + this.monitoringConn);
        }
        return true;
      }

      final boolean isValid = this.monitoringConn.isValid(
          (int) TimeUnit.NANOSECONDS.toSeconds(this.failureDetectionIntervalNano));
      return isValid;

    } catch (final SQLException sqlEx) {
      return false;
    }
  }

  private void updateNodeHealthStatus(
      final boolean connectionValid,
      final long statusCheckStartNano,
      final long statusCheckEndNano) {

    if (!connectionValid) {
      this.failureCount++;

      if (this.invalidNodeStartTimeNano == 0) {
        this.invalidNodeStartTimeNano = statusCheckStartNano;
      }

      final long invalidNodeDurationNano = statusCheckEndNano - this.invalidNodeStartTimeNano;
      final long maxInvalidNodeDurationNano =
          this.failureDetectionIntervalNano * Math.max(0, this.failureDetectionCount);

      if (invalidNodeDurationNano >= maxInvalidNodeDurationNano) {
        if (this.logger.isTraceEnabled()) {
          this.logger.logTrace(String.format(
              "[efm2.Monitor.updateNodeHealthStatus]: Host %s is *dead*.",
              this.hostInfo.getHostPortPair()));
        }
        this.nodeUnhealthy = true;
        return;
      }

      if (this.logger.isTraceEnabled()) {
        this.logger.logTrace(String.format(
            "[efm2.Monitor.updateNodeHealthStatus]: Host %s is not *responding* %d.",
            this.hostInfo.getHostPortPair(),
            this.failureCount));
      }
      return;
    }

    if (this.failureCount > 0) {
      // Node is back alive
      if (this.logger.isTraceEnabled()) {
        this.logger.logTrace(String.format(
            "[efm2.Monitor.updateNodeHealthStatus]: Host %s is *alive*.",
            this.hostInfo.getHostPortPair()));
      }
    }

    this.failureCount = 0;
    this.invalidNodeStartTimeNano = 0;
    this.nodeUnhealthy = false;
  }

  private void abortConnection(final @NonNull Connection connectionToAbort) {
    try {
      connectionToAbort.abort(ABORT_EXECUTOR);
      connectionToAbort.close();
    } catch (final SQLException sqlEx) {
      // ignore
      if (this.logger.isTraceEnabled()) {
        this.logger.logTrace(String.format(
            "[efm2.Monitor.abortConnection]: Exception during aborting connection: %s",
            sqlEx.getMessage()));
      }
    }
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
}
