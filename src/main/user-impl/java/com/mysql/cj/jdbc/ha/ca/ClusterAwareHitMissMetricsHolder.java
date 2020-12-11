/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
 * Modifications Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc.ha.ca;

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

  private Object lockObject = new Object();

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
    logMessage.append("\nNumber of reports: " + this.numberOfReports);
    if (this.numberOfReports > 0) {
      logMessage.append("\nNumber of hits: " + this.numberOfHits);
      logMessage.append("\nRatio : " + (this.numberOfHits * 100.0 / this.numberOfReports) + " %");
    }

    log.logInfo(logMessage);
  }
}
