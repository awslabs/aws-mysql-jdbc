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

import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;

import java.sql.SQLException;
import java.util.Set;

/**
 * Monitoring context for each connection. This contains each connection's criteria for
 * whether a server should be considered unhealthy.
 */
public class MonitorConnectionContext {
  private final int failureDetectionIntervalMillis;
  private final int failureDetectionTimeMillis;
  private final int failureDetectionCount;

  private final Set<String> nodeKeys;
  private final Log log;
  private final JdbcConnection connectionToAbort;

  private long startMonitorTime;
  private long invalidNodeStartTime;
  private int failureCount;
  private boolean nodeUnhealthy;
  private boolean activeContext = true;

  /**
   * Constructor.
   *
   * @param connectionToAbort A reference to the connection associated with this context
   *                          that will be aborted in case of server failure.
   * @param nodeKeys All valid references to the server.
   * @param log A {@link Log} implementation.
   * @param failureDetectionTimeMillis Grace period after which node monitoring starts.
   * @param failureDetectionIntervalMillis Interval between each failed connection check.
   * @param failureDetectionCount Number of failed connection checks before considering
   *                              database node as unhealthy.
   */
  public MonitorConnectionContext(
      JdbcConnection connectionToAbort,
      Set<String> nodeKeys,
      Log log,
      int failureDetectionTimeMillis,
      int failureDetectionIntervalMillis,
      int failureDetectionCount) {
    this.connectionToAbort = connectionToAbort;
    this.nodeKeys = nodeKeys;
    this.log = log;
    this.failureDetectionTimeMillis = failureDetectionTimeMillis;
    this.failureDetectionIntervalMillis = failureDetectionIntervalMillis;
    this.failureDetectionCount = failureDetectionCount;
  }

  void setStartMonitorTime(long startMonitorTime) {
    this.startMonitorTime = startMonitorTime;
  }

  Set<String> getNodeKeys() {
    return this.nodeKeys;
  }

  public int getFailureDetectionTimeMillis() {
    return failureDetectionTimeMillis;
  }

  public int getFailureDetectionIntervalMillis() {
    return failureDetectionIntervalMillis;
  }

  public int getFailureDetectionCount() {
    return failureDetectionCount;
  }

  public int getFailureCount() {
    return this.failureCount;
  }

  void setFailureCount(int failureCount) {
    this.failureCount = failureCount;
  }

  void setInvalidNodeStartTime(long invalidNodeStartTimeMillis) {
    this.invalidNodeStartTime = invalidNodeStartTimeMillis;
  }

  void resetInvalidNodeStartTime() {
    this.invalidNodeStartTime = 0;
  }

  boolean isInvalidNodeStartTimeDefined() {
    return this.invalidNodeStartTime > 0;
  }

  public long getInvalidNodeStartTime() {
    return this.invalidNodeStartTime;
  }

  public boolean isNodeUnhealthy() {
    return this.nodeUnhealthy;
  }

  void setNodeUnhealthy(boolean nodeUnhealthy) {
    this.nodeUnhealthy = nodeUnhealthy;
  }

  public boolean isActiveContext() {
    return this.activeContext;
  }

  public void invalidate() {
    this.activeContext = false;
  }

  synchronized void abortConnection() {
    if (this.connectionToAbort == null || !this.activeContext) {
      return;
    }

    try {
      this.connectionToAbort.abortInternal();
    } catch (SQLException sqlEx) {
      // ignore
      this.log.logTrace(String.format(
          "Exception during aborting connection: %s",
          sqlEx.getMessage()));
    }
  }

  /**
   * Update whether the connection is still valid if the total elapsed time has passed the
   * grace period.
   *
   * @param statusCheckStartTime The time when connection status check started in milliseconds.
   * @param currentTime The time when connection status check ended in milliseconds.
   * @param isValid Whether the connection is valid.
   */
  public void updateConnectionStatus(
      long statusCheckStartTime,
      long currentTime,
      boolean isValid) {
    if (!this.activeContext) {
      return;
    }

    final long totalElapsedTimeMillis = currentTime - this.startMonitorTime;

    if (totalElapsedTimeMillis > this.failureDetectionTimeMillis) {
      this.setConnectionValid(isValid, statusCheckStartTime, currentTime);
    }
  }

  /**
   * Set whether the connection to the server is still valid based on the monitoring
   * settings set in the {@link JdbcConnection}.
   *
   * <p>These monitoring settings include:
   * <ul>
   *   <li>{@code failureDetectionInterval}</li>
   *   <li>{@code failureDetectionTime}</li>
   *   <li>{@code failureDetectionCount}</li>
   * </ul>
   *
   * @param connectionValid Boolean indicating whether the server is still responsive.
   * @param statusCheckStartTime The time when connection status check started in milliseconds.
   * @param currentTime The time when connection status check ended in milliseconds.
   */
  void setConnectionValid(
      boolean connectionValid,
      long statusCheckStartTime,
      long currentTime) {
    if (!connectionValid) {
      this.failureCount++;

      if (!this.isInvalidNodeStartTimeDefined()) {
        this.setInvalidNodeStartTime(statusCheckStartTime);
      }

      final long invalidNodeDurationMillis = currentTime - this.getInvalidNodeStartTime();
      final long maxInvalidNodeDurationMillis =
          (long) this.getFailureDetectionIntervalMillis() * Math.max(0, this.getFailureDetectionCount());

      if (invalidNodeDurationMillis >= maxInvalidNodeDurationMillis) {
        this.log.logTrace(
            String.format(
                "[MonitorConnectionContext] node '%s' is *dead*.",
                nodeKeys));
        this.setNodeUnhealthy(true);
        this.abortConnection();
        return;
      }

      this.log.logTrace(String.format(
          "[MonitorConnectionContext] node '%s' is not *responding* (%d).",
          nodeKeys,
          this.getFailureCount()));
      return;
    }

    this.setFailureCount(0);
    this.resetInvalidNodeStartTime();
    this.setNodeUnhealthy(false);

    this.log.logTrace(
        String.format(
            "[MonitorConnectionContext] node '%s' is *alive*.",
            nodeKeys));
  }
}
