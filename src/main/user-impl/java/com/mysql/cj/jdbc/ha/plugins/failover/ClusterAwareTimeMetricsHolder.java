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
