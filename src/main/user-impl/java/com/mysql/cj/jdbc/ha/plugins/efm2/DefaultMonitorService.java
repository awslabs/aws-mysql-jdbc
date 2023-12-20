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
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.plugins.BasicConnectionProvider;
import com.mysql.cj.jdbc.ha.util.SlidingExpirationCacheWithCleanupThread;
import com.mysql.cj.log.Log;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class handles the creation and clean up of monitoring threads to servers with one
 * or more active connections.
 */
public class DefaultMonitorService implements IMonitorService {
  protected static final long CACHE_CLEANUP_NANO = TimeUnit.MINUTES.toNanos(1);

  protected static final Executor ABORT_EXECUTOR = Executors.newSingleThreadExecutor();

  protected static final SlidingExpirationCacheWithCleanupThread<String, IMonitor> monitors =
      new SlidingExpirationCacheWithCleanupThread<>(
          IMonitor::canDispose,
          (monitor) -> {
            try {
              monitor.close();
            } catch (Exception ex) {
              // ignore
            }
          },
          CACHE_CLEANUP_NANO);

  protected final Log logger;
  protected final IMonitorInitializer monitorInitializer;

  public DefaultMonitorService(Log logger) {
    this(
        (hostInfo,
            propertySet,
            failureDetectionTimeMillis,
            failureDetectionIntervalMillis,
            failureDetectionCount) ->
            new Monitor(
              new BasicConnectionProvider(),
              hostInfo,
              propertySet,
              failureDetectionTimeMillis,
              failureDetectionIntervalMillis,
              failureDetectionCount,
              logger),
        logger
    );
  }

  DefaultMonitorService(IMonitorInitializer monitorInitializer, Log logger) {
    this.monitorInitializer = monitorInitializer;
    this.logger = logger;
  }

  @Override
  public MonitorConnectionContext startMonitoring(
      JdbcConnection connectionToAbort,
      HostInfo hostInfo,
      PropertySet propertySet,
      int failureDetectionTimeMillis,
      int failureDetectionIntervalMillis,
      int failureDetectionCount) {

    final IMonitor monitor = this.getMonitor(
        hostInfo,
        propertySet,
        failureDetectionTimeMillis,
        failureDetectionIntervalMillis,
        failureDetectionCount);

    final MonitorConnectionContext context = new MonitorConnectionContext(connectionToAbort);
    monitor.startMonitoring(context);

    return context;
  }

  @Override
  public void stopMonitoring(
      @NonNull final MonitorConnectionContext context,
      @NonNull Connection connectionToAbort) {

    if (context.shouldAbort()) {
      context.setInactive();
      try {
        connectionToAbort.abort(ABORT_EXECUTOR);
        connectionToAbort.close();
      } catch (final SQLException sqlEx) {
        // ignore
        if (logger.isTraceEnabled()) {
          logger.logTrace(
              String.format(
                  "[efm2.DefaultMonitorService.stopMonitoring]: Exception during aborting connection: %s",
                  sqlEx.getMessage()));
        }
      }
    } else {
      context.setInactive();
    }
  }

  @Override
  public void releaseResources() {
    // do nothing
  }

  protected IMonitor getMonitor(
      HostInfo hostInfo,
      PropertySet propertySet,
      final int failureDetectionTimeMillis,
      final int failureDetectionIntervalMillis,
      final int failureDetectionCount) {

    final String monitorKey = String.format("%d:%d:%d:%s",
        failureDetectionTimeMillis,
        failureDetectionIntervalMillis,
        failureDetectionCount,
        hostInfo.getHostPortPair());

    final long cacheExpirationNano = TimeUnit.MILLISECONDS.toNanos(
        propertySet.getIntegerProperty(PropertyKey.monitorDisposalTime).getValue());

    return monitors.computeIfAbsent(
        monitorKey,
        (key) -> monitorInitializer.createMonitor(
            hostInfo,
            propertySet,
            failureDetectionTimeMillis,
            failureDetectionIntervalMillis,
            failureDetectionCount),
        cacheExpirationNano);
  }
}
