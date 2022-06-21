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
   * Returns the unique cluster identifier for topology service instance
   *
   * @return cluster Id
   */
  String getClusterId();

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
