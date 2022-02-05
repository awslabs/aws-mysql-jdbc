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

import com.mysql.cj.Messages;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;
import com.mysql.cj.util.ExpiringCache;
import com.mysql.cj.util.Util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
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
import java.util.function.Supplier;

/**
 * An implementation of topology service for Aurora RDS. It uses
 * information_schema.replica_host_status table to obtain cluster topology and caches it. Different
 * instances of this service with the same 'clusterId' shares the same topology cache. Cache also
 * includes a list of down hosts. That helps to avoid unnecessary attempts to connect them.
 */
public class AuroraTopologyService implements ITopologyService {

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

  public static final ExpiringCache<String, ClusterTopologyInfo> topologyCache =
      new ExpiringCache<>(DEFAULT_CACHE_EXPIRE_MS);
  private static final Object cacheLock = new Object();

  protected String clusterId;
  protected HostInfo clusterInstanceTemplate;

  protected IClusterAwareMetricsContainer metricsContainer;

  /** Null logger shared by all connections at startup. */
  protected static final Log NULL_LOGGER = new NullLogger(Log.LOGGER_INSTANCE_NAME);

  /** The logger we're going to use. */
  protected transient Log log = NULL_LOGGER;

  /** Initializes a service with topology default refresh rate. */
  public AuroraTopologyService(Log log) {
    this(DEFAULT_REFRESH_RATE_IN_MILLISECONDS, log, ClusterAwareMetricsContainer::new);
  }

  /**
   * Initializes a service with provided topology refresh rate.
   *
   * @param refreshRateInMilliseconds Topology refresh rate in millis
   */
  public AuroraTopologyService(int refreshRateInMilliseconds, Log log,
      Supplier<IClusterAwareMetricsContainer> metricsContainerSupplier) {
    this.refreshRateInMilliseconds = refreshRateInMilliseconds;
    this.clusterId = UUID.randomUUID().toString();
    this.clusterInstanceTemplate = new HostInfo(null, "?", HostInfo.NO_PORT, null, null);
    this.metricsContainer = metricsContainerSupplier.get();

    if (log != null) {
      this.log = log;
    }
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
    this.log.logTrace(Messages.getString(
        "AuroraTopologyService.1",
        new Object[] {clusterId}));
    this.clusterId = clusterId;
    this.metricsContainer.setClusterId(clusterId);
  }

  /**
   * Returns the unique cluster identifier for topology service instance
   *
   * @return cluster Id
   */
  @Override
  public String getClusterId() {
    return clusterId;
  }

  /**
   * Sets host details common to each instance in the cluster, including the host dns pattern. "?"
   * (question mark) in a host dns pattern will be replaced with a host instance name to form a
   * fully qualified dns host endpoint.
   *
   * <p>Examples: "?.mydomain.com", "db-instance.?.mydomain.com"
   *
   * @param clusterInstanceTemplate Cluster instance details including host dns pattern.
   */
  @Override
  public void setClusterInstanceTemplate(HostInfo clusterInstanceTemplate) {
    this.log.logTrace(Messages.getString(
        "AuroraTopologyService.2",
        new Object[] {clusterInstanceTemplate.getHost(),
            clusterInstanceTemplate.getPort(),
            clusterInstanceTemplate.getDatabase()}));
    this.clusterInstanceTemplate = clusterInstanceTemplate;
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
   *     Returns an empty list if topology isn't available or is invalid (doesn't contain a writer).
   */
  @Override
  public List<HostInfo> getTopology(JdbcConnection conn, boolean forceUpdate)
      throws SQLException {
    ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);

    if (clusterTopologyInfo == null
        || Util.isNullOrEmpty(clusterTopologyInfo.hosts)
        || forceUpdate
        || refreshNeeded(clusterTopologyInfo)) {

      ClusterTopologyInfo latestTopologyInfo = queryForTopology(conn);

      if (!Util.isNullOrEmpty(latestTopologyInfo.hosts)) {
        clusterTopologyInfo = updateCache(clusterTopologyInfo, latestTopologyInfo);
      } else if (clusterTopologyInfo == null
          || clusterTopologyInfo.hosts == null
          || forceUpdate) {
        return new ArrayList<>();
      }
    }

    return clusterTopologyInfo.hosts;
  }

  private boolean refreshNeeded(ClusterTopologyInfo info) {
    Instant lastUpdateTime = info.lastUpdated;
    return lastUpdateTime == null || Duration.between(lastUpdateTime, Instant.now()).toMillis() > refreshRateInMilliseconds;
  }

  /**
   * Query the database for the cluster topology and use the results to record information about the topology.
   *
   * @param conn A connection to database to fetch the latest topology.
   * @return Cluster topology details.
   */
  protected ClusterTopologyInfo queryForTopology(JdbcConnection conn) throws SQLException {
    long startTimeMs = System.currentTimeMillis();

    ClusterTopologyInfo topologyInfo = null;

    try (Statement stmt = conn.createStatement()) {
      try (ResultSet resultSet = stmt.executeQuery(RETRIEVE_TOPOLOGY_SQL)) {
        topologyInfo = processQueryResults(resultSet);
      }
    } catch (SQLSyntaxErrorException e) {
      // We may get SQLSyntaxErrorException like the following from MySQL databases:
      // "Unknown table 'REPLICA_HOST_STATUS' in information_schema"
      // Ignore this kind of exceptions.
    } finally {
      if (gatherPerfMetrics(conn.getPropertySet())) {
        long currentTimeMs = System.currentTimeMillis();
        this.metricsContainer.registerTopologyQueryExecutionTime(currentTimeMs - startTimeMs);
      }
    }

    return topologyInfo != null ? topologyInfo
        : new ClusterTopologyInfo(
            new ArrayList<>(),
            new HashSet<>(),
            null,
            Instant.now(),
            false);
  }

  /**
   * Checks the connection's property if performance metrics should be gathered for topology service
   * @param props Property set
   * @return true if performance metrics for topology service should be gathered
   */
  private boolean gatherPerfMetrics(PropertySet props) {
    return props != null &&
        props.getProperty(PropertyKey.gatherPerfMetrics.getKeyName()) != null &&
        props.getBooleanProperty(PropertyKey.gatherPerfMetrics.getKeyName()).getValue();
  }

  /**
   * Process the results of the topology query to the database. This method creates a {@link HostInfo} object for each
   * host in the topology, as well as storing some additional information about the cluster.
   *
   * @param resultSet The {@link ResultSet} returned by the topology database query.
   * @return The {@link ClusterTopologyInfo} representing the results of the query. The host list in this object will
   *         be empty if the topology query returned an invalid topology (no writer instance).
   */
  private ClusterTopologyInfo processQueryResults(ResultSet resultSet)
      throws SQLException {
    int writerCount = 0;

    List<HostInfo> hosts = new ArrayList<>();
    while (resultSet.next()) {
      if (!WRITER_SESSION_ID.equalsIgnoreCase(resultSet.getString(FIELD_SESSION_ID))) {
        hosts.add(createHost(resultSet));
        continue;
      }

      if (writerCount == 0) {
        // store the first writer to its expected position [0]
        hosts.add(
            FailoverConnectionPlugin.WRITER_CONNECTION_INDEX,
            createHost(resultSet));
      } else {
        // append other writers, if any, to the end of the host list
        hosts.add(createHost(resultSet));
      }
      writerCount++;
    }

    if (writerCount == 0) {
      this.log.logError(Messages.getString("AuroraTopologyService.3"));
      hosts.clear();
    }

    return new ClusterTopologyInfo(
        hosts,
        new HashSet<>(),
        null,
        Instant.now(),
        writerCount > 1);
  }

  private HostInfo createHost(ResultSet resultSet) throws SQLException {
    String hostEndpoint = getHostEndpoint(resultSet.getString(FIELD_SERVER_ID));
    ConnectionUrl hostUrl = ConnectionUrl.getConnectionUrlInstance(
        getUrlFromEndpoint(
            hostEndpoint,
            this.clusterInstanceTemplate.getPort(),
            this.clusterInstanceTemplate.getDatabase()),
        new Properties());
    return new HostInfo(
        hostUrl,
        hostEndpoint,
        this.clusterInstanceTemplate.getPort(),
        this.clusterInstanceTemplate.getUser(),
        this.clusterInstanceTemplate.getPassword(),
        this.clusterInstanceTemplate.isPasswordless(),
        getPropertiesFromTopology(resultSet));
  }

  /**
   * Build an instance dns endpoint based on instance/node name.
   *
   * @param nodeName An instance name.
   * @return Instance dns endpoint
   */
  private String getHostEndpoint(String nodeName) {
    String host = this.clusterInstanceTemplate.getHost();
    return host.replace("?", nodeName);
  }

  private String getUrlFromEndpoint(String endpoint, int port, String dbname) {
    return String.format(
        "%s//%s:%d/%s",
        ConnectionUrl.Type.SINGLE_CONNECTION_AWS.getScheme(),
        endpoint,
        port,
        dbname);
  }

  private Map<String, String> getPropertiesFromTopology(ResultSet resultSet)
      throws SQLException {
    Map<String, String> properties =
        new HashMap<>(this.clusterInstanceTemplate.getHostProperties());
    properties.put(
        TopologyServicePropertyKeys.INSTANCE_NAME,
        resultSet.getString(FIELD_SERVER_ID));
    properties.put(
        TopologyServicePropertyKeys.SESSION_ID,
        resultSet.getString(FIELD_SESSION_ID));
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
   * Store the information for the topology in the cache, creating the information object if it did not previously exist
   * in the cache.
   *
   * @param clusterTopologyInfo The cluster topology info that existed in the cache before the topology query. This parameter
   *                            will be null if no topology info for the cluster has been created in the cache yet.
   * @param latestTopologyInfo The results of the current topology query
   * @return The {@link ClusterTopologyInfo} stored in the cache by this method, representing the most up-to-date
   *         information we have about the topology.
   */
  private ClusterTopologyInfo updateCache(
      ClusterTopologyInfo clusterTopologyInfo,
      ClusterTopologyInfo latestTopologyInfo) {
    if (clusterTopologyInfo == null) {
      clusterTopologyInfo = latestTopologyInfo;
    } else {
      clusterTopologyInfo.hosts = latestTopologyInfo.hosts;
      clusterTopologyInfo.downHosts = latestTopologyInfo.downHosts;
      clusterTopologyInfo.isMultiWriterCluster = latestTopologyInfo.isMultiWriterCluster;
    }
    clusterTopologyInfo.lastUpdated = Instant.now();

    synchronized (cacheLock) {
      topologyCache.put(this.clusterId, clusterTopologyInfo);
    }
    return clusterTopologyInfo;
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
        clusterTopologyInfo = new ClusterTopologyInfo(
            new ArrayList<>(),
            new HashSet<>(),
            null,
            Instant.now(),
            false);
        topologyCache.put(this.clusterId, clusterTopologyInfo);
      } else if (clusterTopologyInfo.downHosts == null) {
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

  private static class ClusterTopologyInfo {
    public Instant lastUpdated;
    public Set<String> downHosts;
    public List<HostInfo> hosts;
    public HostInfo lastUsedReader;
    public boolean isMultiWriterCluster;

    ClusterTopologyInfo(
        List<HostInfo> hosts, Set<String> downHosts, HostInfo lastUsedReader,
        Instant lastUpdated, boolean isMultiWriterCluster) {
      this.hosts = hosts;
      this.downHosts = downHosts;
      this.lastUsedReader = lastUsedReader;
      this.lastUpdated = lastUpdated;
      this.isMultiWriterCluster = isMultiWriterCluster;
    }
  }
}
