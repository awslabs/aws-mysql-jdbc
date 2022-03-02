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
