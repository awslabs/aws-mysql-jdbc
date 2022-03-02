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
 * Collect certain performance metrics for ClusterAwareConnectionProxy.
 *
 * <p>Performance (duration in time) metrics: - Failover Detection Duration of time between a start
 * of executing sql statement to the moment when driver identifies a communication error and starts
 * a failover process (a process to re-connect to another cluster instance). - Writer Failover
 * Procedure Duration of time running writer failover procedure. - Reader Failover Procedure
 * Duration of time running reader failover procedure.
 *
 * <p>Performance (hit-miss) metrics: - Successful Failover Reconnects A total number of failover
 * events vs a number of successful ones - Used Last Connected Reader Number of time driver utilises
 * details about a last successfully connected reader host. Knowing the last successfully connected
 * reader host helps driver to connect to this host in the first turn rather than trying to randomly
 * connect to any reader hosts. This metric is applicable for read-only connections only. - Invalid
 * Initial Connection As a side-effect of using last connected reader host from a cache, there's a
 * chance that this host may turn out to be a writer, not a reader as expected. In such a case,
 * driver has to drop connection to this host since it's not accurate and initiate a new one to
 * another host. Such cases are measured by this metric. "Hit" event corresponds to dropping
 * connection and connecting to another host. - Used Cached Topology Number of time driver utilises
 * a cached cluster topology
 */
public class ClusterAwareMetrics {

  private final ClusterAwareTimeMetricsHolder failureDetection =
      new ClusterAwareTimeMetricsHolder("Failover Detection");
  private final ClusterAwareTimeMetricsHolder writerFailoverProcedure =
      new ClusterAwareTimeMetricsHolder("Writer Failover Procedure");
  private final ClusterAwareTimeMetricsHolder readerFailoverProcedure =
      new ClusterAwareTimeMetricsHolder("Reader Failover Procedure");
  private final ClusterAwareHitMissMetricsHolder failoverConnects =
      new ClusterAwareHitMissMetricsHolder("Successful Failover Reconnects");
  private final ClusterAwareHitMissMetricsHolder invalidInitialConnection =
      new ClusterAwareHitMissMetricsHolder("Invalid Initial Connection");
  private final ClusterAwareHitMissMetricsHolder useLastConnectedReader =
      new ClusterAwareHitMissMetricsHolder("Used Last Connected Reader");
  private final ClusterAwareHitMissMetricsHolder useCachedTopology =
      new ClusterAwareHitMissMetricsHolder("Used Cached Topology");

  public ClusterAwareMetrics() {}

  public void registerFailureDetectionTime(long timeMs) {
    this.failureDetection.registerQueryExecutionTime(timeMs);
  }

  public void registerWriterFailoverProcedureTime(long timeMs) {
    this.writerFailoverProcedure.registerQueryExecutionTime(timeMs);
  }

  public void registerReaderFailoverProcedureTime(long timeMs) {
    this.readerFailoverProcedure.registerQueryExecutionTime(timeMs);
  }

  public void registerFailoverConnects(boolean isHit) {
    this.failoverConnects.register(isHit);
  }

  public void registerInvalidInitialConnection(boolean isHit) {
    this.invalidInitialConnection.register(isHit);
  }

  public void registerUseLastConnectedReader(boolean isHit) {
    this.useLastConnectedReader.register(isHit);
  }

  public void registerUseCachedTopology(boolean isHit) {
    this.useCachedTopology.register(isHit);
  }

  /**
   * Report metrics.
   * */
  public void reportMetrics(Log log) {
    this.failoverConnects.reportMetrics(log);
    this.failureDetection.reportMetrics(log);
    this.writerFailoverProcedure.reportMetrics(log);
    this.readerFailoverProcedure.reportMetrics(log);
    this.useCachedTopology.reportMetrics(log);
    this.useLastConnectedReader.reportMetrics(log);
    this.invalidInitialConnection.reportMetrics(log);
  }
}
