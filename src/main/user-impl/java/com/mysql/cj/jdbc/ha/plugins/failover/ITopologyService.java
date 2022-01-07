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

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.jdbc.JdbcConnection;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * It's a generic interface for all topology services. It's expected that every implementation of
 * topology service covers different cluster types or cluster deployment.
 *
 * <p>It's expected that each instance of ClusterAwareConnectionProxy uses its own instance of
 * topology service.
 */
public interface ITopologyService {

  /**
   * Set unique cluster identifier for topology service instance.
   *
   * <p>Cluster could be accessed through different connection strings like IP address, cluster dns
   * endpoint, instance dns endpoint, custom domain alias (CNAME), etc. Cluster Id can be any string
   * that unique identify a cluster despite the way it's been accessed.
   *
   * @param clusterId Cluster unique identifier.
   */
  void setClusterId(String clusterId);

  /**
   * Sets host details common to each instance in the cluster, including the host dns pattern. "?"
   * (question mark) in a host dns pattern will be replaced with a host instance name to form a
   * fully qualified dns host endpoint.
   *
   * <p>Examples: "?.mydomain.com", "db-instance.?.mydomain.com"
   *
   * @param clusterInstanceTemplate Cluster instance details including host dns pattern.
   */
  void setClusterInstanceTemplate(HostInfo clusterInstanceTemplate);

  /**
   * Get cluster topology. A writer host is always at position 0.
   *
   * @param conn A connection to database to fetch the latest topology, if needed.
   * @param forceUpdate If true, it forces a service to ignore cached copy of topology and to fetch
   *     a fresh one.
   * @return A list of hosts that describes cluster topology. A writer is always at position 0.
   *     Returns an empty list if topology isn't available or is invalid (doesn't contain a writer).
   */
  List<HostInfo> getTopology(JdbcConnection conn, boolean forceUpdate)
      throws SQLException;

  /**
   * Get cached topology.
   *
   * @return List of hosts that represents topology. If there's no topology in cache, it returns
   *     null.
   */
  List<HostInfo> getCachedTopology();

  /**
   * Get details about the most recent reader that the driver has successfully connected to.
   *
   * @return The host details of the most recent reader connection.
   */
  HostInfo getLastUsedReaderHost();

  /**
   * Set details about the most recent reader that the driver has connected to.
   *
   * @param reader A reader host.
   */
  void setLastUsedReaderHost(HostInfo reader);

  /**
   * Return the {@link HostInfo} object that is associated with a provided connection from the topology host list.
   *
   * @param conn A connection to database.
   * @return The HostInfo object from the topology host list. Returns null if the connection host is
   *     not found in the latest topology.
   */
  HostInfo getHostByName(JdbcConnection conn);

  /**
   * Get a set of instance names that were marked down.
   *
   * @return A set of instance dns names with port. For example: "instance-1.my-domain.com:3306".
   */
  Set<String> getDownHosts();

  /**
   * Mark host as down. Host stays marked down until next topology refresh.
   *
   * @param downHost The {@link HostInfo} object representing the host to mark as down
   */
  void addToDownHostList(HostInfo downHost);

  /**
   * Unmark host as down. The host is removed from the list of down hosts
   *
   * @param host The {@link HostInfo} object representing the host to remove from the list of down hosts
   */
  void removeFromDownHostList(HostInfo host);

  /**
   * Check if topology belongs to multi-writer cluster.
   *
   * @return True, if it's multi-writer cluster.
   */
  boolean isMultiWriterCluster();

  /**
   * Set new topology refresh rate. Topology is considered valid (up to date) within provided
   * duration of time.
   *
   * @param refreshRate Topology refresh rate in millis.
   */
  void setRefreshRate(int refreshRate);

  /** Clear topology service for all clusters. */
  void clearAll();

  /** Clear topology service for the current cluster. */
  void clear();
}
