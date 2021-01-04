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

  private ClusterAwareTimeMetricsHolder failureDetection =
      new ClusterAwareTimeMetricsHolder("Failover Detection");
  private ClusterAwareTimeMetricsHolder writerFailoverProcedure =
      new ClusterAwareTimeMetricsHolder("Writer Failover Procedure");
  private ClusterAwareTimeMetricsHolder readerFailoverProcedure =
      new ClusterAwareTimeMetricsHolder("Reader Failover Procedure");
  private ClusterAwareHitMissMetricsHolder failoverConnects =
      new ClusterAwareHitMissMetricsHolder("Successful Failover Reconnects");
  private ClusterAwareHitMissMetricsHolder invalidInitialConnection =
      new ClusterAwareHitMissMetricsHolder("Invalid Initial Connection");
  private ClusterAwareHitMissMetricsHolder useLastConnectedReader =
      new ClusterAwareHitMissMetricsHolder("Used Last Connected Reader");
  private ClusterAwareHitMissMetricsHolder useCachedTopology =
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
