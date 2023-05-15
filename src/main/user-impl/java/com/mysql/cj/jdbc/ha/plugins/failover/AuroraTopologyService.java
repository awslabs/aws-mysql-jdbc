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

import com.mysql.cj.Messages;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;
import com.mysql.cj.util.CacheMap;
import com.mysql.cj.util.Util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An implementation of topology service for Aurora RDS. It uses
 * information_schema.replica_host_status table to obtain cluster topology and caches it. Different
 * instances of this service with the same 'clusterId' shares the same topology cache. Cache also
 * includes a list of down hosts. That helps to avoid unnecessary attempts to connect them.
 */
public class AuroraTopologyService implements ITopologyService {

  static final int DEFAULT_REFRESH_RATE_IN_MILLISECONDS = 30000;
  static final int DEFAULT_CACHE_EXPIRE_MS = 5 * 60 * 1000; // 5 min

  private long refreshRateNanos;
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

  public static final CacheMap<String, List<HostInfo>> topologyCache = new CacheMap<>();
  public static final CacheMap<String, Set<String>> downHostCache = new CacheMap<>();
  public static final CacheMap<String, HostInfo> lastUsedReaderCache = new CacheMap<>();

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
    this.refreshRateNanos = TimeUnit.MILLISECONDS.toNanos(refreshRateInMilliseconds);
    this.clusterId = UUID.randomUUID().toString();
    this.clusterInstanceTemplate = new HostInfo(null, "?", HostInfo.NO_PORT, null, null);
    this.metricsContainer = metricsContainerSupplier.get();

    if (log != null) {
      this.log = log;
    }
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
   * #refreshRateNanos }).
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

    List<HostInfo> hosts = topologyCache.get(this.clusterId);

    if (hosts == null || forceUpdate) {

      ClusterTopologyInfo latestTopologyInfo = queryForTopology(conn);

      if (latestTopologyInfo != null) {
        downHostCache.get(this.clusterId, ConcurrentHashMap.newKeySet(), this.refreshRateNanos).clear();

        if (!Util.isNullOrEmpty(latestTopologyInfo.getHosts())) {
          topologyCache.put(this.clusterId, latestTopologyInfo.getHosts(), this.refreshRateNanos);
          return latestTopologyInfo.getHosts();
        }
      }
    }

    return forceUpdate ? new ArrayList<>() : hosts;
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

    return topologyInfo != null
        ? topologyInfo
        : new ClusterTopologyInfo(new ArrayList<>());
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

    List<HostInfo> hosts = new ArrayList<>();
    List<HostInfo> writers = new ArrayList<>();
    while (resultSet.next()) {
      HostInfo currentHost = createHost(resultSet);

      if (!WRITER_SESSION_ID.equalsIgnoreCase(resultSet.getString(FIELD_SESSION_ID))) {
        hosts.add(currentHost);
        continue;
      }

      writers.add(currentHost);
    }

    int writersCount = writers.size();

    if (writersCount == 0) {
      this.log.logError(Messages.getString("AuroraTopologyService.3"));
      hosts.clear();
    } else if (writersCount == 1) {
      hosts.add(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX, writers.get(0));
    } else {
      // Store the first writer to its expected position [0]. If there are other writers or stale records, ignore them.
      List<HostInfo> sortedWriters = writers.stream()
          .sorted(Comparator.comparing(HostInfo::getLastUpdatedTime).reversed()).collect(Collectors.toList());
      hosts.add(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX, sortedWriters.get(0));
    }

    return new ClusterTopologyInfo(hosts);
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
    try {
      properties.put(
          TopologyServicePropertyKeys.LAST_UPDATED,
          convertTimestampToString(resultSet.getTimestamp(FIELD_LAST_UPDATED)));
    } catch (WrongArgumentException ex) {
      // Fallback in case timestamp is invalid time. (ex. daylight savings)
      properties.put(
          TopologyServicePropertyKeys.LAST_UPDATED,
          convertTimestampToString(Timestamp.from(Instant.now())));
    }

    properties.put(
        TopologyServicePropertyKeys.REPLICA_LAG,
        Double.valueOf(resultSet.getDouble(FIELD_REPLICA_LAG)).toString());
    return properties;
  }

  private String convertTimestampToString(Timestamp timestamp) {
    DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    return timestamp == null ? null : formatter.format(timestamp.toLocalDateTime());
  }

  /**
   * Get cached topology.
   *
   * @return List of hosts that represents topology. If there's no topology in the cache or the
   *     cached topology is outdated, it returns null.
   */
  @Override
  public List<HostInfo> getCachedTopology() {
    return topologyCache.get(this.clusterId);
  }

  /**
   * Get details about the most recent reader that the driver has successfully connected to.
   *
   * @return The host details of the most recent reader connection. Returns null if the driver has
   *     not connected to a reader within the refresh rate period.
   */
  @Override
  public HostInfo getLastUsedReaderHost() {
    return lastUsedReaderCache.get(this.clusterId);
  }

  /**
   * Set details about the most recent reader that the driver has connected to.
   *
   * @param reader A reader host.
   */
  @Override
  public void setLastUsedReaderHost(HostInfo reader) {
    if (reader == null) {
      return;
    }
    lastUsedReaderCache.put(this.clusterId, reader, this.refreshRateNanos);
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
        List<HostInfo> hosts = topologyCache.get(this.clusterId);
        return instanceNameToHost(instanceName, hosts);
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
    return downHostCache.get(this.clusterId, ConcurrentHashMap.newKeySet(), this.refreshRateNanos);
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
    downHostCache.get(this.clusterId, ConcurrentHashMap.newKeySet(), this.refreshRateNanos)
        .add(downHost.getHostPortPair());
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
    downHostCache.get(this.clusterId, ConcurrentHashMap.newKeySet(), this.refreshRateNanos)
      .remove(host.getHostPortPair());
  }

  /**
   * Set new topology refresh rate. Different service instances may have different topology refresh
   * rate while sharing the same topology cache.
   *
   * @param refreshRateMillis Topology refresh rate in millis.
   */
  @Override
  public void setRefreshRate(int refreshRateMillis) {
    this.refreshRateNanos = TimeUnit.MILLISECONDS.toNanos(refreshRateMillis);
  }

  /** Clear topology cache for all clusters. */
  @Override
  public void clearAll() {
    topologyCache.clear();
    downHostCache.clear();
    lastUsedReaderCache.clear();
  }

  /** Clear topology cache for the current cluster. */
  @Override
  public void clear() {
    topologyCache.remove(this.clusterId);
    downHostCache.remove(this.clusterId);
    lastUsedReaderCache.remove(this.clusterId);
  }

  private static class ClusterTopologyInfo {
    private List<HostInfo> hosts;

    ClusterTopologyInfo(List<HostInfo> hosts) {
      this.hosts = hosts;
    }

    List<HostInfo> getHosts() { return this.hosts; }
  }
}
