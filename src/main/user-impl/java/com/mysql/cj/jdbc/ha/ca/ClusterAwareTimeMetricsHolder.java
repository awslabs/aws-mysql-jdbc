/*
 * AWS JDBC Driver for MySQL
 * Copyright 2020 Amazon.com Inc. or affiliates.
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
 * part of this connector, is also subject to the Universal FOSS Exception,
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

    logMessage.append("** Performance Metrics Report for '");
    logMessage.append(this.metricName);
    logMessage.append("' **\n");
    if (this.numberOfQueriesIssued > 0) {
      logMessage.append("\nLongest reported time: " + this.longestQueryTimeMs + " ms");
      logMessage.append("\nShortest reported time: " + this.shortestQueryTimeMs + " ms");
      logMessage.append(
          "\nAverage query execution time: "
              + (this.totalQueryTimeMs / this.numberOfQueriesIssued)
              + " ms");
    }
    logMessage.append("\nNumber of reports: " + this.numberOfQueriesIssued);

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
          logMessage.append(
              "\n\tless than "
                  + this.perfMetricsHistBreakpoints[i + 1]
                  + " ms: \t"
                  + this.perfMetricsHistCounts[i]);
        } else {
          logMessage.append(
              "\n\tbetween "
                  + this.perfMetricsHistBreakpoints[i]
                  + " and "
                  + this.perfMetricsHistBreakpoints[i + 1]
                  + " ms: \t"
                  + this.perfMetricsHistCounts[i]);
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
