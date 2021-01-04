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

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.ExpiringCache;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * An implementation of topology service for Aurora RDS. It uses
 * information_schema.replica_host_status table to obtain cluster topology and caches it. Different
 * instances of this service with the same 'clusterId' shares the same topology cache. Cache also
 * includes a list of down hosts. That helps to avoid unnecessary attempts to connect them.
 */
public class AuroraTopologyService implements TopologyService, CanCollectPerformanceMetrics {

  static final int DEFAULT_REFRESH_RATE_IN_MILLISECONDS = 30000;
  static final int DEFAULT_CACHE_EXPIRE_MS = 5 * 60 * 1000; // 5 min

  private int refreshRateInMilliseconds;
  static final String RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID, SESSION_ID, LAST_UPDATE_TIMESTAMP, REPLICA_LAG_IN_MILLISECONDS "
          + "FROM information_schema.replica_host_status "
          + "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 " // 5 min
          + "ORDER BY LAST_UPDATE_TIMESTAMP DESC";
  static final String GET_INSTANCE_NAME_SQL = "SELECT @@aurora_server_id";
  static final String GET_INSTANCE_NAME_COL = "@@aurora_server_id";
  static final String WRITER_SESSION_ID = "MASTER_SESSION_ID";

  static final String FIELD_SERVER_ID = "SERVER_ID";
  static final String FIELD_SESSION_ID = "SESSION_ID";
  static final String FIELD_LAST_UPDATED = "LAST_UPDATE_TIMESTAMP";
  static final String FIELD_REPLICA_LAG = "REPLICA_LAG_IN_MILLISECONDS";

  protected static final int NO_CONNECTION_INDEX = -1;

  protected static final ExpiringCache<String, ClusterTopologyInfo> topologyCache =
      new ExpiringCache<>(DEFAULT_CACHE_EXPIRE_MS);
  private static final Object cacheLock = new Object();

  protected String clusterId;
  protected HostInfo clusterInstanceHost;

  protected ClusterAwareTimeMetricsHolder queryTopologyMetrics =
      new ClusterAwareTimeMetricsHolder("Topology Query");
  protected boolean gatherPerfMetrics = false;

  /** Initializes a service with topology default refresh rate. */
  public AuroraTopologyService() {
    this(DEFAULT_REFRESH_RATE_IN_MILLISECONDS);
  }

  /**
   * Initializes a service with provided topology refresh rate.
   *
   * @param refreshRateInMilliseconds Topology refresh rate in millis
   */
  public AuroraTopologyService(int refreshRateInMilliseconds) {
    this.refreshRateInMilliseconds = refreshRateInMilliseconds;
    this.clusterId = UUID.randomUUID().toString();
    this.clusterInstanceHost = new HostInfo(null, "?", HostInfo.NO_PORT, null, null);
  }

  /**
   * Service instances with the same cluster Id share cluster topology. Shared topology is cached
   * for a specified period of time. This method sets cache expiration time in millis.
   *
   * @param expireTimeMs Topology cache expiration time in millis
   */
  public static void setExpireTime(int expireTimeMs) {
    topologyCache.setExpireTime(expireTimeMs);
  }

  /**
   * Sets cluster Id for a service instance. Different service instances with the same cluster Id
   * share topology cache.
   *
   * @param clusterId Topology cluster Id
   */
  @Override
  public void setClusterId(String clusterId) {
    this.clusterId = clusterId;
  }

  /**
   * Sets host details common to each instance in the cluster, including the host dns pattern. "?"
   * (question mark) in a host dns pattern will be replaced with a host instance name to form a
   * fully qualified dns host endpoint.
   *
   * <p>Examples: "?.mydomain.com", "db-instance.?.mydomain.com"
   *
   * @param clusterInstanceHost Cluster instance details including host dns pattern.
   */
  @Override
  public void setClusterInstanceHost(HostInfo clusterInstanceHost) {
    this.clusterInstanceHost = clusterInstanceHost;
  }

  /**
   * Get cluster topology. It may require an extra call to database to fetch the latest topology. A
   * cached copy of topology is returned if it's not yet outdated (controlled by {@link
   * #refreshRateInMilliseconds }).
   *
   * @param conn A connection to database to fetch the latest topology, if needed.
   * @param forceUpdate If true, it forces a service to ignore cached copy of topology and to fetch
   *     a fresh one.
   * @return A list of hosts that describes cluster topology. A writer is always at position 0.
   *     Returns null if topology isn't available.
   */
  @Override
  public List<HostInfo> getTopology(JdbcConnection conn, boolean forceUpdate) {
    ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);

    if (clusterTopologyInfo == null
        || clusterTopologyInfo.hosts == null
        || forceUpdate
        || refreshNeeded(clusterTopologyInfo)) {

      ClusterTopologyInfo latestTopologyInfo = queryForTopology(conn);

      if (latestTopologyInfo != null && latestTopologyInfo.hosts != null) {
        synchronized (cacheLock) {
          if (clusterTopologyInfo == null) {
            clusterTopologyInfo = new ClusterTopologyInfo();
          }
          clusterTopologyInfo.hosts = latestTopologyInfo.hosts;
          clusterTopologyInfo.isMultiWriterCluster = latestTopologyInfo.isMultiWriterCluster;
          clusterTopologyInfo.lastUpdated = Instant.now();
          clusterTopologyInfo.downHosts = new HashSet<>();
          topologyCache.put(this.clusterId, clusterTopologyInfo);
        }
      } else {
        return (clusterTopologyInfo == null || forceUpdate) ? null : clusterTopologyInfo.hosts;
      }
    }

    return clusterTopologyInfo.hosts;
  }

  private boolean refreshNeeded(ClusterTopologyInfo info) {
    Instant lastUpdateTime = info.lastUpdated;
    return lastUpdateTime == null
        || Duration.between(lastUpdateTime, Instant.now()).toMillis() > refreshRateInMilliseconds;
  }

  /**
   * Obtain a cluster topology from database.
   *
   * @param conn A connection to database to fetch the latest topology.
   * @return Cluster topology details.
   */
  protected ClusterTopologyInfo queryForTopology(JdbcConnection conn) {
    long startTimeMs = this.gatherPerfMetrics ? System.currentTimeMillis() : 0;

    ClusterTopologyInfo result = new ClusterTopologyInfo();
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet resultSet = stmt.executeQuery(RETRIEVE_TOPOLOGY_SQL)) {
        result.hosts = new ArrayList<>();
        result.hosts.add(null); // reserve space for a writer node
        int writerCount = 0;

        int i = 1;
        while (resultSet.next()) {
          if (WRITER_SESSION_ID.equalsIgnoreCase(resultSet.getString(FIELD_SESSION_ID))) {
            if (writerCount == 0) {
              // store the first writer to its expected position [0]
              result.hosts.set(
                      ClusterAwareConnectionProxy.WRITER_CONNECTION_INDEX, createHost(resultSet));
            } else {
              // store other writers, if any, to reader position
              // the goal is to not lose them
              result.hosts.add(i, createHost(resultSet));
              i++;
            }
            writerCount++;
          } else {
            result.hosts.add(i, createHost(resultSet));
            i++;
          }
        }
        result.isMultiWriterCluster = (writerCount > 1);
      }
    } catch (SQLException e) {
      // eat
    }

    if (this.gatherPerfMetrics) {
      long currentTimeMs = System.currentTimeMillis();
      this.queryTopologyMetrics.registerQueryExecutionTime(currentTimeMs - startTimeMs);
    }

    return result;
  }

  private HostInfo createHost(ResultSet resultSet) throws SQLException {
    String hostEndpoint = getHostEndpoint(resultSet.getString(FIELD_SERVER_ID));
    ConnectionUrl hostUrl =
        ConnectionUrl.getConnectionUrlInstance(
            getUrlFromEndpoint(
                hostEndpoint,
                this.clusterInstanceHost.getPort(),
                this.clusterInstanceHost.getDatabase()),
            new Properties());
    return new HostInfo(
        hostUrl,
        hostEndpoint,
        this.clusterInstanceHost.getPort(),
        this.clusterInstanceHost.getUser(),
        this.clusterInstanceHost.getPassword(),
        this.clusterInstanceHost.isPasswordless(),
        getPropertiesFromTopology(resultSet));
  }

  /**
   * Build an instance dns endpoint based on instance/node name.
   *
   * @param nodeName An instance name.
   * @return Instance dns endpoint
   */
  private String getHostEndpoint(String nodeName) {
    String host = this.clusterInstanceHost.getHost();
    return host.replace("?", nodeName);
  }

  private String getUrlFromEndpoint(String endpoint, int port, String dbname) {
    return String.format(
        "%s//%s:%d/%s", ConnectionUrl.Type.SINGLE_CONNECTION_AWS.getScheme(), endpoint, port, dbname);
  }

  private Map<String, String> getPropertiesFromTopology(ResultSet resultSet) throws SQLException {
    Map<String, String> properties = new HashMap<>();

    if (this.clusterInstanceHost != null) {
      properties.putAll(this.clusterInstanceHost.getHostProperties());
    }

    properties.put(TopologyServicePropertyKeys.INSTANCE_NAME, resultSet.getString(FIELD_SERVER_ID));
    properties.put(TopologyServicePropertyKeys.SESSION_ID, resultSet.getString(FIELD_SESSION_ID));
    properties.put(
        TopologyServicePropertyKeys.LAST_UPDATED,
        convertTimestampToString(resultSet.getTimestamp(FIELD_LAST_UPDATED)));
    properties.put(
        TopologyServicePropertyKeys.REPLICA_LAG,
        Double.valueOf(resultSet.getDouble(FIELD_REPLICA_LAG)).toString());
    return properties;
  }

  private String convertTimestampToString(Timestamp timestamp) {
    return timestamp == null ? null : timestamp.toString();
  }

  /**
   * Get cached topology.
   *
   * @return List of hosts that represents topology. If there's no topology in the cache or the
   *     cached topology is outdated, it returns null.
   */
  @Override
  public List<HostInfo> getCachedTopology() {
    ClusterTopologyInfo info = topologyCache.get(this.clusterId);
    return info == null || refreshNeeded(info) ? null : info.hosts;
  }

  /**
   * Get details about the most recent reader that the driver has successfully connected to.
   *
   * @return The host details of the most recent reader connection. Returns null if the driver has
   *     not connected to a reader within the refresh rate period.
   */
  @Override
  public HostInfo getLastUsedReaderHost() {
    ClusterTopologyInfo info = topologyCache.get(this.clusterId);
    return info == null || refreshNeeded(info) ? null : info.lastUsedReader;
  }

  /**
   * Set details about the most recent reader that the driver has connected to.
   *
   * @param reader A reader host.
   */
  @Override
  public void setLastUsedReaderHost(HostInfo reader) {
    if (reader != null) {
      synchronized (cacheLock) {
        ClusterTopologyInfo info = topologyCache.get(this.clusterId);
        if (info != null) {
          info.lastUsedReader = reader;
        }
      }
    }
  }

  /**
   * Return the {@link HostInfo} object that is associated with a provided connection from the topology host list.
   *
   * @param conn A connection to database.
   * @return The HostInfo object from the topology host list. Returns null if the connection host is
   *     not found in the latest topology.
   */
  @Override
  public HostInfo getHostByName(JdbcConnection conn) {
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet resultSet = stmt.executeQuery(GET_INSTANCE_NAME_SQL)) {
        String instanceName = null;
        if (resultSet.next()) {
          instanceName = resultSet.getString(GET_INSTANCE_NAME_COL);
        }
        ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
        return instanceNameToHost(
                instanceName, clusterTopologyInfo == null ? null : clusterTopologyInfo.hosts);
      }
    } catch (SQLException e) {
      return null;
    }
  }

  private HostInfo instanceNameToHost(String name, List<HostInfo> hosts) {
    if (name == null || hosts == null) {
      return null;
    }

    for (HostInfo host : hosts) {
      if (host != null && name.equalsIgnoreCase(
              host.getHostProperties().get(TopologyServicePropertyKeys.INSTANCE_NAME))) {
        return host;
      }
    }
    return null;
  }

  /**
   * Get a set of instance names that were marked down.
   *
   * @return A set of instance dns names with port (example: "instance-1.my-domain.com:3306")
   */
  @Override
  public Set<String> getDownHosts() {
    synchronized (cacheLock) {
      ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
      return clusterTopologyInfo != null && clusterTopologyInfo.downHosts != null
          ? clusterTopologyInfo.downHosts
          : new HashSet<>();
    }
  }

  /**
   * Mark host as down. Host stays marked down until next topology refresh.
   *
   * @param downHost The {@link HostInfo} object representing the host to mark as down
   */
  @Override
  public void addToDownHostList(HostInfo downHost) {
    if (downHost == null) {
      return;
    }
    synchronized (cacheLock) {
      ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
      if (clusterTopologyInfo == null) {
        clusterTopologyInfo = new ClusterTopologyInfo();
        topologyCache.put(this.clusterId, clusterTopologyInfo);
      }
      if (clusterTopologyInfo.downHosts == null) {
        clusterTopologyInfo.downHosts = new HashSet<>();
      }
      clusterTopologyInfo.downHosts.add(downHost.getHostPortPair());
    }
  }

  /**
   * Unmark host as down. The host is removed from the list of down hosts
   *
   * @param host The {@link HostInfo} object representing the host to remove from the list of down hosts
   */
  @Override
  public void removeFromDownHostList(HostInfo host) {
    if (host == null) {
      return;
    }
    synchronized (cacheLock) {
      ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
      if (clusterTopologyInfo != null && clusterTopologyInfo.downHosts != null) {
        clusterTopologyInfo.downHosts.remove(host.getHostPortPair());
      }
    }
  }

  /**
   * Check if cached topology belongs to multi-writer cluster.
   *
   * @return True, if it's multi-writer cluster.
   */
  @Override
  public boolean isMultiWriterCluster() {
    synchronized (cacheLock) {
      ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
      return (clusterTopologyInfo != null
          && clusterTopologyInfo.downHosts != null
          && clusterTopologyInfo.isMultiWriterCluster);
    }
  }

  /**
   * Set new topology refresh rate. Different service instances may have different topology refresh
   * rate while sharing the same topology cache.
   *
   * @param refreshRate Topology refresh rate in millis.
   */
  @Override
  public void setRefreshRate(int refreshRate) {
    this.refreshRateInMilliseconds = refreshRate;
    if (topologyCache.getExpireTime() < this.refreshRateInMilliseconds) {
      synchronized (cacheLock) {
        if (topologyCache.getExpireTime() < this.refreshRateInMilliseconds) {
          topologyCache.setExpireTime(this.refreshRateInMilliseconds);
        }
      }
    }
  }

  /** Clear topology cache for all clusters. */
  @Override
  public void clearAll() {
    synchronized (cacheLock) {
      topologyCache.clear();
    }
  }

  /** Clear topology cache for the current cluster. */
  @Override
  public void clear() {
    synchronized (cacheLock) {
      topologyCache.remove(this.clusterId);
    }
  }

  /**
   * This service implementation supports metrics. This method enables collecting internal metrics.
   * Metrics are disabled by default.
   *
   * @param isEnabled True to enable internal metrics.
   */
  @Override
  public void setPerformanceMetricsEnabled(boolean isEnabled) {
    this.gatherPerfMetrics = isEnabled;
  }

  /**
   * Report collected metrics to a provided logger.
   *
   * @param log Logger to report collected metrics to.
   */
  @Override
  public void reportMetrics(Log log) {
    this.queryTopologyMetrics.reportMetrics(log);

    StringBuilder logMessage = new StringBuilder(256);

    logMessage.append("** Cached Topology Keys **\n");
    topologyCache.keySet().forEach(x -> logMessage.append("'").append(x).append("'\n"));

    log.logInfo(logMessage);
  }

  private static class ClusterTopologyInfo {
    public boolean isMultiWriterCluster;
    public Instant lastUpdated;
    public Set<String> downHosts;
    public List<HostInfo> hosts;
    public HostInfo lastUsedReader;
  }
}
