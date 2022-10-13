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

package com.mysql.cj.jdbc.ha.plugins.failover;

import com.mysql.cj.log.Log;

/**
 * A simple implementation of hit-miss metric. It collects a total number of events been reported as
 * well as number of "hit" events.
 *
 * <p>Example of hit-miss metric: loaded or not loaded data, data found in cache or not found.
 */
public class ClusterAwareHitMissMetricsHolder {

  protected String metricName;
  protected int numberOfReports;
  protected int numberOfHits;

  private final Object lockObject = new Object();

  /**
   * Initialize a metric holder with a metric name.
   *
   * @param metricName Metric name
   */
  public ClusterAwareHitMissMetricsHolder(String metricName) {
    this.metricName = metricName;
  }

  /**
   * Register (notify) a metric holder about event.
   *
   * @param isHit True if event is a "hit" event.
   */
  public void register(boolean isHit) {
    synchronized (this.lockObject) {
      this.numberOfReports++;
      if (isHit) {
        this.numberOfHits++;
      }
    }
  }

  /**
   * Report collected metric to a provided logger.
   *
   * @param log A logger to report collected metric.
   */
  public void reportMetrics(Log log) {
    StringBuilder logMessage = new StringBuilder(256);

    logMessage.append("** Performance Metrics Report for '");
    logMessage.append(this.metricName);
    logMessage.append("' **\n");
    logMessage.append("\nNumber of reports: ").append(this.numberOfReports);
    if (this.numberOfReports > 0) {
      logMessage.append("\nNumber of hits: ").append(this.numberOfHits);
      logMessage.append("\nRatio : ").append(this.numberOfHits * 100.0 / this.numberOfReports).append(" %");
    }

    log.logInfo(logMessage);
  }
}
