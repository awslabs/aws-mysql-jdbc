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

import com.mysql.cj.log.BaseMetricsHolder;
import com.mysql.cj.log.Log;

/**
 * A simple implementation of timing metric. It collects an execution time for particular
 * case/event.
 *
 * <p>Use registerQueryExecutionTime(long queryTimeMs) to report an execution time.
 */
public class ClusterAwareTimeMetricsHolder extends BaseMetricsHolder {

  protected String metricName;

  /**
   * Initialize a metric holder with a metric name.
   *
   * @param metricName Metric name
   */
  public ClusterAwareTimeMetricsHolder(String metricName) {
    super();
    this.metricName = metricName;
  }

  /**
   * Report collected metric to a provided logger.
   *
   * @param log A logger to report collected metric.
   */
  @Override
  public void reportMetrics(Log log) {
    StringBuilder logMessage = new StringBuilder(256);

    logMessage.append("** Performance Metrics Report for '").append(this.metricName).append("' **\n");
    if (this.numberOfQueriesIssued > 0) {
      logMessage.append("\nLongest reported time: ").append(this.longestQueryTimeMs).append(" ms");
      logMessage.append("\nShortest reported time: ").append(this.shortestQueryTimeMs).append(" ms");
      double avgTime = this.totalQueryTimeMs / this.numberOfQueriesIssued;
      logMessage.append("\nAverage query execution time: ").append(avgTime).append(" ms");
    }
    logMessage.append("\nNumber of reports: ").append(this.numberOfQueriesIssued);

    if (this.numberOfQueriesIssued > 0 && this.perfMetricsHistBreakpoints != null) {
      logMessage.append("\n\n\tTiming Histogram:\n");
      int maxNumPoints = 20;
      int highestCount = Integer.MIN_VALUE;

      for (int i = 0; i < (HISTOGRAM_BUCKETS); i++) {
        if (this.perfMetricsHistCounts[i] > highestCount) {
          highestCount = this.perfMetricsHistCounts[i];
        }
      }

      if (highestCount == 0) {
        highestCount = 1; // avoid DIV/0
      }

      for (int i = 0; i < (HISTOGRAM_BUCKETS - 1); i++) {

        if (i == 0) {
          logMessage.append("\n\tless than ")
                  .append(this.perfMetricsHistBreakpoints[i + 1])
                  .append(" ms: \t")
                  .append(this.perfMetricsHistCounts[i]);
        } else {
          logMessage.append("\n\tbetween ")
                  .append(this.perfMetricsHistBreakpoints[i])
                  .append(" and ")
                  .append(this.perfMetricsHistBreakpoints[i + 1])
                  .append(" ms: \t")
                  .append(this.perfMetricsHistCounts[i]);
        }

        logMessage.append("\t");

        int numPointsToGraph =
            (int) (maxNumPoints * ((double) this.perfMetricsHistCounts[i] / highestCount));

        for (int j = 0; j < numPointsToGraph; j++) {
          logMessage.append("*");
        }

        if (this.longestQueryTimeMs < this.perfMetricsHistCounts[i + 1]) {
          break;
        }
      }

      if (this.perfMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 2] < this.longestQueryTimeMs) {
        logMessage.append("\n\tbetween ");
        logMessage.append(this.perfMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 2]);
        logMessage.append(" and ");
        logMessage.append(this.perfMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 1]);
        logMessage.append(" ms: \t");
        logMessage.append(this.perfMetricsHistCounts[HISTOGRAM_BUCKETS - 1]);
      }
    }

    log.logInfo(logMessage);
  }
}
