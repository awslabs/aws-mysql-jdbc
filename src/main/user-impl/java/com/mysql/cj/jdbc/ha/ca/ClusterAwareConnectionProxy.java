/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of this program hereby grant you an additional permission to link the
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

import com.mysql.cj.Messages;
import com.mysql.cj.NativeSession;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.ha.MultiHostConnectionProxy;
import com.mysql.cj.jdbc.interceptors.ConnectionLifecycleInterceptor;
import com.mysql.cj.jdbc.interceptors.ConnectionLifecycleInterceptorProvider;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.LogFactory;
import com.mysql.cj.log.NullLogger;
import com.mysql.cj.log.StandardLogger;
import com.mysql.cj.util.IpAddressUtils;
import com.mysql.cj.util.StringUtils;

import javax.net.ssl.SSLException;
import java.io.EOFException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A proxy for a dynamic com.mysql.cj.jdbc.JdbcConnection implementation that provides cluster-aware
 * failover features. Connection switching occurs on communications related exceptions and/or
 * cluster topology changes.
 */
public class ClusterAwareConnectionProxy extends MultiHostConnectionProxy
    implements ConnectionLifecycleInterceptorProvider {

  static final String METHOD_SET_READ_ONLY = "setReadOnly";
  static final String METHOD_SET_AUTO_COMMIT = "setAutoCommit";
  static final String METHOD_COMMIT = "commit";
  static final String METHOD_ROLLBACK = "rollback";
  static final String METHOD_CLOSE = "close";

  private final Pattern auroraDnsPattern =
      Pattern.compile(
          "(.+)\\.(proxy-|cluster-|cluster-ro-|cluster-custom-)?([a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
  private final Pattern auroraCustomClusterPattern =
      Pattern.compile(
          "(.+)\\.(cluster-custom-[a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
  private final Pattern auroraProxyDnsPattern =
      Pattern.compile(
          "(.+)\\.(proxy-[a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);

  /** Null logger shared by all connections at startup. */
  protected static final Log NULL_LOGGER = new NullLogger(Log.LOGGER_INSTANCE_NAME);

  /** The logger we're going to use. */
  protected transient Log log = NULL_LOGGER;

  protected static final int DEFAULT_SOCKET_TIMEOUT_MS = 10000;
  protected static final int DEFAULT_CONNECT_TIMEOUT_MS = 30000;
  protected static final int NO_CONNECTION_INDEX = -1;
  protected static final int WRITER_CONNECTION_INDEX = 0; // writer host is always stored at index 0

  protected int currentHostIndex = NO_CONNECTION_INDEX;
  protected Boolean explicitlyReadOnly = null;
  protected boolean inTransaction = false;
  protected boolean explicitlyAutoCommit = true;
  protected boolean isClusterTopologyAvailable = false;
  protected boolean isMultiWriterCluster = false;
  protected boolean isRdsProxy = false;
  protected boolean isRds = false;
  protected TopologyService topologyService;
  protected List<HostInfo> hosts;
  protected WriterFailoverHandler writerFailoverHandler;
  protected ReaderFailoverHandler readerFailoverHandler;
  protected ConnectionProvider connectionProvider;

  protected ClusterAwareMetrics metrics = new ClusterAwareMetrics();
  private long invokeStartTimeMs;
  private long failoverStartTimeMs;

  // Configuration settings
  protected boolean enableFailoverSetting = true;
  protected int clusterTopologyRefreshRateMsSetting;
  protected boolean gatherPerfMetricsSetting;
  protected int failoverTimeoutMsSetting;
  protected int failoverClusterTopologyRefreshRateMsSetting;
  protected int failoverWriterReconnectIntervalMsSetting;
  protected int failoverReaderConnectTimeoutMsSetting;
  protected String clusterIdSetting;
  protected String clusterInstanceHostPatternSetting;
  protected int failoverConnectTimeoutMs;
  protected int failoverSocketTimeoutMs;

  /**
   * Instantiates a new AuroraConnectionProxy for the given list of hosts and connection properties.
   *
   * @param connectionUrl {@link ConnectionUrl} instance containing the lists of hosts available to
   *     switch on.
   * @throws SQLException if an error occurs
   */
  public ClusterAwareConnectionProxy(ConnectionUrl connectionUrl) throws SQLException {
    super(connectionUrl);
    initSettings(connectionUrl);
    initLogger(connectionUrl);

    AuroraTopologyService topologyService = new AuroraTopologyService();
    topologyService.setPerformanceMetricsEnabled(this.gatherPerfMetricsSetting);
    topologyService.setRefreshRate(this.clusterTopologyRefreshRateMsSetting);
    this.topologyService = topologyService;

    this.connectionProvider = new BasicConnectionProvider();
    this.readerFailoverHandler =
        new ClusterAwareReaderFailoverHandler(
            this.topologyService,
            this.connectionProvider,
            this.failoverReaderConnectTimeoutMsSetting,
            this.log);
    this.writerFailoverHandler =
        new ClusterAwareWriterFailoverHandler(
            this.topologyService,
            this.connectionProvider,
            this.readerFailoverHandler,
            this.failoverTimeoutMsSetting,
            this.failoverClusterTopologyRefreshRateMsSetting,
            this.failoverWriterReconnectIntervalMsSetting,
            this.log);

    initClusterInfo(connectionUrl);
  }

  ClusterAwareConnectionProxy(
      ConnectionUrl connectionUrl,
      ConnectionProvider connectionProvider,
      TopologyService service,
      WriterFailoverHandler writerFailoverHandler,
      ReaderFailoverHandler readerFailoverHandler)
      throws SQLException {
    super(connectionUrl);
    initSettings(connectionUrl);
    initLogger(connectionUrl);

    this.topologyService = service;
    this.topologyService.setRefreshRate(this.clusterTopologyRefreshRateMsSetting);
    if (this.topologyService instanceof CanCollectPerformanceMetrics) {
      ((CanCollectPerformanceMetrics) this.topologyService)
          .setPerformanceMetricsEnabled(this.gatherPerfMetricsSetting);
    }

    this.connectionProvider = connectionProvider;
    this.writerFailoverHandler = writerFailoverHandler;
    this.readerFailoverHandler = readerFailoverHandler;

    initClusterInfo(connectionUrl);
  }

  /**
   * Checks if connection is associated with Aurora cluster and instantiates a new
   * AuroraConnectionProxy if needed. Otherwise it returns a single-host connection.
   *
   * @param connectionUrl {@link ConnectionUrl} instance containing the lists of hosts available to
   *     switch on.
   * @throws SQLException if an error occurs
   */
  public static JdbcConnection autodetectClusterAndCreateProxyInstance(ConnectionUrl connectionUrl)
      throws SQLException {

    ClusterAwareConnectionProxy connProxy = new ClusterAwareConnectionProxy(connectionUrl);

    if (connProxy.isFailoverEnabled()) {
      return (JdbcConnection)
          java.lang.reflect.Proxy.newProxyInstance(
              JdbcConnection.class.getClassLoader(),
              new Class<?>[] {JdbcConnection.class},
              connProxy);
    }
    // If failover is disabled, reset proxy settings from the connection.
    connProxy.currentConnection.setProxy(null);
    return connProxy.currentConnection;
  }

  /**
   * Instantiates a new AuroraConnectionProxy.
   *
   * @param connectionUrl {@link ConnectionUrl} instance containing the lists of hosts available to
   *     switch on.
   * @throws SQLException if an error occurs
   */
  public static JdbcConnection createProxyInstance(ConnectionUrl connectionUrl)
      throws SQLException {
    ClusterAwareConnectionProxy connProxy = new ClusterAwareConnectionProxy(connectionUrl);
    return (JdbcConnection)
        java.lang.reflect.Proxy.newProxyInstance(
            JdbcConnection.class.getClassLoader(),
            new Class<?>[] {JdbcConnection.class},
            connProxy);
  }

  protected synchronized void initSettings(ConnectionUrl connectionUrl) throws SQLException {
    JdbcPropertySetImpl connProps = new JdbcPropertySetImpl();
    try {
      connProps.initializeProperties(connectionUrl.getMainHost().exposeAsProperties());
    } catch (CJException e) {
      throw SQLExceptionsMapping.translateException(e, null);
    }

    this.enableFailoverSetting =
        connProps.getBooleanProperty(PropertyKey.enableClusterAwareFailover).getValue();
    this.clusterTopologyRefreshRateMsSetting =
        connProps.getIntegerProperty(PropertyKey.clusterTopologyRefreshRateMs).getValue();
    this.gatherPerfMetricsSetting =
        connProps.getBooleanProperty(PropertyKey.gatherPerfMetrics).getValue();
    this.failoverTimeoutMsSetting =
        connProps.getIntegerProperty(PropertyKey.failoverTimeoutMs).getValue();
    this.failoverClusterTopologyRefreshRateMsSetting =
        connProps.getIntegerProperty(PropertyKey.failoverClusterTopologyRefreshRateMs).getValue();
    this.failoverWriterReconnectIntervalMsSetting =
        connProps.getIntegerProperty(PropertyKey.failoverWriterReconnectIntervalMs).getValue();
    this.failoverReaderConnectTimeoutMsSetting =
        connProps.getIntegerProperty(PropertyKey.failoverReaderConnectTimeoutMs).getValue();
    this.clusterIdSetting = connProps.getStringProperty(PropertyKey.clusterId).getValue();
    this.clusterInstanceHostPatternSetting =
        connProps.getStringProperty(PropertyKey.clusterInstanceHostPattern).getValue();

    RuntimeProperty<Integer> connectTimeout = connProps.getIntegerProperty(PropertyKey.connectTimeout);
    this.failoverConnectTimeoutMs = connectTimeout.isExplicitlySet() ? connectTimeout.getValue() : DEFAULT_CONNECT_TIMEOUT_MS;

    RuntimeProperty<Integer> socketTimeout = connProps.getIntegerProperty(PropertyKey.socketTimeout);
    this.failoverSocketTimeoutMs = socketTimeout.isExplicitlySet() ? socketTimeout.getValue() : DEFAULT_SOCKET_TIMEOUT_MS;
  }

  protected synchronized void initLogger(ConnectionUrl connUrl) {
    String loggerClassName = connUrl.getOriginalProperties().get(PropertyKey.logger.getKeyName());
    if (!StringUtils.isNullOrEmpty(loggerClassName)) {
      this.log = LogFactory.getLogger(loggerClassName, Log.LOGGER_INSTANCE_NAME);
    }
  }

  /**
   * Checks if proxy is connected to cluster that can report its topology.
   *
   * @return true if proxy is connected to cluster that can report its topology
   */
  public boolean isClusterTopologyAvailable() {
    return this.isClusterTopologyAvailable;
  }

  /**
   * Checks if proxy is connected to RDS-hosted cluster.
   *
   * @return true if proxy is connected to RDS-hosted cluster
   */
  public boolean isRds() {
    return this.isRds;
  }

  /**
   * Checks if proxy is connected to cluster through RDS proxy.
   *
   * @return true if proxy is connected to cluster through RDS proxy
   */
  public boolean isRdsProxy() {
    return this.isRdsProxy;
  }

  /**
   * Checks if proxy is connected to multi-writer cluster.
   *
   * @return true if proxy is connected to multi-writer cluster
   */
  public boolean isMultiWriterCluster() {
    return this.isMultiWriterCluster;
  }

  /**
   * Checks if cluster-aware failover is enabled/possible.
   *
   * @return true if cluster-aware failover is enabled
   */
  public boolean isFailoverEnabled() {
    return this.enableFailoverSetting
        && !this.isRdsProxy
        && this.isClusterTopologyAvailable
        && !this.isMultiWriterCluster;
  }

  protected void initClusterInfo(ConnectionUrl connUrl) throws SQLException {
    if (!this.enableFailoverSetting) {
      // Use a standard default connection - no further initialization required
      this.currentConnection = this.connectionProvider.connect(connUrl.getMainHost());
      return;
    }
    this.log.logDebug(Messages.getString("ClusterAwareConnectionProxy.9"));

    this.log.logTrace(
        Messages.getString(
            "ClusterAwareConnectionProxy.10", new Object[] {"clusterId", this.clusterIdSetting}));
    this.log.logTrace(
        Messages.getString(
            "ClusterAwareConnectionProxy.10",
            new Object[] {"clusterInstanceHostPattern", this.clusterInstanceHostPatternSetting}));

    HostInfo mainHost = this.connectionUrl.getMainHost();

    if (!StringUtils.isNullOrEmpty(this.clusterInstanceHostPatternSetting)) {

      ConnectionUrlParser.Pair<String, Integer> pair =
          ConnectionUrlParser.parseHostPortPair(this.clusterInstanceHostPatternSetting);
      if (pair == null) {
        // "Invalid value in 'clusterInstanceHostPattern' configuration property."
        throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.5"));
      }
      String instanceHostPattern = pair.left;
      int instanceHostPort = pair.right != HostInfo.NO_PORT ? pair.right : mainHost.getPort();

      if (!isDnsPatternValid(instanceHostPattern)) {
        // "Invalid value in 'clusterInstanceHostPattern' configuration property."
        this.log.logError(Messages.getString("ClusterAwareConnectionProxy.5"));
        throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.5"));
      }

      // Instance host info is similar to original main host except host and port which go from
      // configuration property
      this.topologyService.setClusterInstanceHost(
          createClusterInstanceHost(mainHost, instanceHostPattern, instanceHostPort));
      this.log.logTrace(
          Messages.getString(
              "ClusterAwareConnectionProxy.11",
              new Object[] {instanceHostPattern, instanceHostPort}));

      this.isRds = isRdsDns(instanceHostPattern);
      this.log.logTrace(
          Messages.getString("ClusterAwareConnectionProxy.12", new Object[] {"isRds", this.isRds}));

      this.isRdsProxy = isRdsProxyDns(instanceHostPattern);
      this.log.logTrace(
          Messages.getString(
              "ClusterAwareConnectionProxy.12", new Object[] {"isRdsProxy", this.isRdsProxy}));

      boolean isRdsCustomCluster = isRdsCustomClusterDns(instanceHostPattern);
      this.log.logTrace(
          Messages.getString(
              "ClusterAwareConnectionProxy.12",
              new Object[] {"isRdsCustomCluster", isRdsCustomCluster}));

      if (this.isRdsProxy) {
        // "RDS Proxy url can't be used as an instance pattern."
        this.log.logError(Messages.getString("ClusterAwareConnectionProxy.8"));
        throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.8"));
      }

      if (isRdsCustomCluster) {
        // "RDS Custom Cluster endpoint can't be used as an instance pattern."
        this.log.logError(Messages.getString("ClusterAwareConnectionProxy.24"));
        throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.24"));
      }

      if (!StringUtils.isNullOrEmpty(this.clusterIdSetting)) {
        this.topologyService.setClusterId(this.clusterIdSetting);
        this.log.logTrace(
            Messages.getString(
                "ClusterAwareConnectionProxy.13",
                new Object[] {"clusterId", this.clusterIdSetting}));
      } else if (this.isRds) {
        // If it's a cluster endpoint, or a reader cluster endpoint, then let's use as cluster
        // identification
        String clusterRdsHostUrl = getRdsClusterHostUrl(instanceHostPattern);
        if (!StringUtils.isNullOrEmpty(clusterRdsHostUrl)) {
          this.topologyService.setClusterId(clusterRdsHostUrl + ":" + instanceHostPort);
          this.log.logTrace(
              Messages.getString(
                  "ClusterAwareConnectionProxy.13",
                  new Object[] {"clusterId", clusterRdsHostUrl + ":" + instanceHostPort}));
        }
      }

      createConnectionAndInitializeTopology(connUrl);
    } else if (IpAddressUtils.isIPv4(mainHost.getHost())
        || IpAddressUtils.isIPv6(mainHost.getHost())) {
      this.topologyService.setClusterInstanceHost(createClusterInstanceHost(mainHost, mainHost.getHost(), mainHost.getPort()));
      this.log.logTrace(
          Messages.getString(
              "ClusterAwareConnectionProxy.11",
              new Object[] {mainHost.getHost(), mainHost.getPort()}));

      if (!StringUtils.isNullOrEmpty(this.clusterIdSetting)) {
        this.topologyService.setClusterId(this.clusterIdSetting);
        this.log.logTrace(
            Messages.getString(
                "ClusterAwareConnectionProxy.13",
                new Object[] {"clusterId", this.clusterIdSetting}));
      }

      createConnectionAndInitializeTopology(connUrl);

      if (this.isClusterTopologyAvailable) {
        // "'clusterInstanceHostPattern' configuration property is required when IP address is used
        // to connect to cluster."
        this.log.logError(Messages.getString("ClusterAwareConnectionProxy.6"));
        throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.6"));
      }

      this.isRds = false; // actually we don't know
      this.log.logTrace(
          Messages.getString("ClusterAwareConnectionProxy.12", new Object[] {"isRds", this.isRds}));

      this.isRdsProxy = false; // actually we don't know
      this.log.logTrace(
          Messages.getString(
              "ClusterAwareConnectionProxy.12", new Object[] {"isRdsProxy", this.isRdsProxy}));

    } else {

      this.isRds = isRdsDns(mainHost.getHost());
      this.log.logTrace(
          Messages.getString("ClusterAwareConnectionProxy.12", new Object[] {"isRds", this.isRds}));

      this.isRdsProxy = isRdsProxyDns(mainHost.getHost());
      this.log.logTrace(
          Messages.getString(
              "ClusterAwareConnectionProxy.12", new Object[] {"isRdsProxy", this.isRdsProxy}));

      if (!this.isRds) {
        // it's not RDS, maybe custom domain (CNAME)
        this.topologyService.setClusterInstanceHost(createClusterInstanceHost(mainHost, mainHost.getHost(), mainHost.getPort()));
        this.log.logTrace(
            Messages.getString(
                "ClusterAwareConnectionProxy.11",
                new Object[] {mainHost.getHost(), mainHost.getPort()}));

        if (!StringUtils.isNullOrEmpty(this.clusterIdSetting)) {
          this.topologyService.setClusterId(this.clusterIdSetting);
          this.log.logTrace(
              Messages.getString(
                  "ClusterAwareConnectionProxy.13",
                  new Object[] {"clusterId", this.clusterIdSetting}));
        }

        createConnectionAndInitializeTopology(connUrl);

        if (this.isClusterTopologyAvailable) {
          // "The provided connection string appears to be a custom domain. The driver requires the
          // 'clusterInstanceHostPattern' configuration property to be set for custom domains."
          this.log.logError(Messages.getString("ClusterAwareConnectionProxy.7"));
          throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.7"));
        }
      } else {
        // It's RDS
        String rdsInstanceHostPattern = getRdsInstanceHostPattern(mainHost.getHost());
        this.topologyService.setClusterInstanceHost(
            createClusterInstanceHost(mainHost, rdsInstanceHostPattern, mainHost.getPort()));
        this.log.logTrace(
            Messages.getString(
                "ClusterAwareConnectionProxy.11",
                new Object[] {rdsInstanceHostPattern, mainHost.getPort()}));

        if (!StringUtils.isNullOrEmpty(this.clusterIdSetting)) {
          this.topologyService.setClusterId(this.clusterIdSetting);
          this.log.logTrace(
              Messages.getString(
                  "ClusterAwareConnectionProxy.13",
                  new Object[] {"clusterId", this.clusterIdSetting}));
        } else if (this.isRdsProxy) {
          // Each proxy is associated with a single cluster so it's safe to use RDS Proxy Url as
          // cluster identification
          this.topologyService.setClusterId(mainHost.getHost() + ":" + mainHost.getPort());
          this.log.logTrace(
              Messages.getString(
                  "ClusterAwareConnectionProxy.13",
                  new Object[] {"clusterId", mainHost.getHost() + ":" + mainHost.getPort()}));
        } else {
          // If it's cluster endpoint or reader cluster endpoint,
          // then let's use as cluster identification
          String clusterRdsHostUrl = getRdsClusterHostUrl(mainHost.getHost());
          if (!StringUtils.isNullOrEmpty(clusterRdsHostUrl)) {
            this.topologyService.setClusterId(clusterRdsHostUrl + ":" + mainHost.getPort());
            this.log.logTrace(
                Messages.getString(
                    "ClusterAwareConnectionProxy.13",
                    new Object[] {"clusterId", clusterRdsHostUrl + ":" + mainHost.getPort()}));
          }
        }
        createConnectionAndInitializeTopology(connUrl);
      }
    }
  }

  private HostInfo createClusterInstanceHost(HostInfo mainHost, String host, int port) {
    Map<String, String> properties = new HashMap<>(mainHost.getHostProperties());
    properties.put(PropertyKey.connectTimeout.getKeyName(), String.valueOf(this.failoverConnectTimeoutMs));
    properties.put(PropertyKey.socketTimeout.getKeyName(), String.valueOf(this.failoverSocketTimeoutMs));

    return new HostInfo(
            this.connectionUrl,
            host,
            port,
            mainHost.getUser(),
            mainHost.getPassword(),
            mainHost.isPasswordless(),
            properties);
  }

  protected synchronized void createConnectionAndInitializeTopology(ConnectionUrl connUrl) throws SQLException {
    createInitialConnection(connUrl);
    initTopology();
    if (this.isFailoverEnabled()) {
      validateInitialConnection();
      HostInfo currentHost = this.hosts.get(this.currentHostIndex);
      if (currentHost != null && isExplicitlyReadOnly()) {
        topologyService.setLastUsedReaderHost(currentHost);
      }
      this.currentConnection.getPropertySet().getIntegerProperty(PropertyKey.socketTimeout).setValue(this.failoverSocketTimeoutMs);
      ((NativeSession) this.currentConnection.getSession()).setSocketTimeout(this.failoverSocketTimeoutMs);
    }
  }

  private synchronized void createInitialConnection(ConnectionUrl connUrl) throws SQLException {
    String host = connUrl.getMainHost().getHost();
    if (isRdsClusterDns(host)) {
      this.explicitlyReadOnly = isReaderClusterDns(host);
      this.log.logTrace(
              Messages.getString(
                      "ClusterAwareConnectionProxy.12",
                      new Object[] {"explicitlyReadOnly", this.explicitlyReadOnly}));

      try {
        attemptConnectionUsingCachedTopology();
      } catch (SQLException e) {
        // do nothing - attempt to connect directly will be made below
      }
    }

    if (!isConnected()) {
      // Either URL was not a cluster endpoint or cached topology did not exist - connect directly
      // to URL
      this.currentConnection = this.connectionProvider.connect(connUrl.getMainHost());
      setConnectionProxy(this.currentConnection);
    }
  }

  private void attemptConnectionUsingCachedTopology() throws SQLException {
    this.hosts = topologyService.getCachedTopology();
    if (this.hosts == null || this.hosts.isEmpty()) {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerUseCachedTopology(false);
      }
      return;
    }

    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerUseCachedTopology(true);
    }

    int candidateIndex = getCandidateIndexForInitialConnection();
    if (candidateIndex != NO_CONNECTION_INDEX) {
      connectTo(candidateIndex);
    }
  }

  private int getCandidateIndexForInitialConnection() {
    if (isExplicitlyReadOnly()) {
      int candidateReaderIndex = getCandidateReaderForInitialConnection();
      if (candidateReaderIndex != NO_CONNECTION_INDEX) {
        return candidateReaderIndex;
      }
    }
    return this.hosts.get(WRITER_CONNECTION_INDEX) != null ? WRITER_CONNECTION_INDEX : NO_CONNECTION_INDEX;
  }

  private int getCandidateReaderForInitialConnection() {
    int lastUsedReaderIndex = getHostIndex(topologyService.getLastUsedReaderHost());
    if (lastUsedReaderIndex != NO_CONNECTION_INDEX) {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerUseLastConnectedReader(true);
      }
      return lastUsedReaderIndex;
    }

    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerUseLastConnectedReader(false);
    }

    if (clusterContainsReader()) {
      return getRandomReaderIndex();
    } else {
      return NO_CONNECTION_INDEX;
    }
  }

  private synchronized void initTopology() {
    this.hosts = this.topologyService.getTopology(this.currentConnection, false);
    this.isClusterTopologyAvailable = this.hosts != null && !this.hosts.isEmpty();
    this.isMultiWriterCluster = this.topologyService.isMultiWriterCluster();
    this.log.logTrace(
        Messages.getString(
            "ClusterAwareConnectionProxy.12",
            new Object[] {"isClusterTopologyAvailable", this.isClusterTopologyAvailable}));

    if (this.isFailoverEnabled()) {
      logTopology();
    }
  }

  private synchronized void validateInitialConnection() throws SQLException {
    this.currentHostIndex = getHostIndex(topologyService.getHostByName(this.currentConnection));
    if (!isConnected()) {
      pickNewConnection();
      return;
    }

    if (!invalidWriterConnection()) {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerInvalidInitialConnection(false);
      }
      return;
    }

    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerInvalidInitialConnection(true);
    }

    if (this.hosts.get(WRITER_CONNECTION_INDEX) == null) {
      if (this.gatherPerfMetricsSetting) {
        this.failoverStartTimeMs = System.currentTimeMillis();
      }
      failover(WRITER_CONNECTION_INDEX);
      return;
    }

    try {
      connectTo(WRITER_CONNECTION_INDEX);
    } catch (SQLException e) {
      if (this.gatherPerfMetricsSetting) {
        this.failoverStartTimeMs = System.currentTimeMillis();
      }
      failover(WRITER_CONNECTION_INDEX);
    }
  }

  private boolean isDnsPatternValid(String pattern) {
    return pattern.contains("?");
  }

  private boolean isRdsDns(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    return matcher.find();
  }

  private boolean isRdsProxyDns(String host) {
    Matcher matcher = auroraProxyDnsPattern.matcher(host);
    return matcher.find();
  }

  private boolean isRdsCustomClusterDns(String host) {
    Matcher matcher = auroraCustomClusterPattern.matcher(host);
    return matcher.find();
  }

  private String getRdsClusterHostUrl(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    String clusterKeyword = getClusterKeyword(matcher);
    if ("cluster-".equalsIgnoreCase(clusterKeyword)
        || "cluster-ro-".equalsIgnoreCase(clusterKeyword)) {
      return matcher.group(1) + ".cluster-" + matcher.group(3); // always RDS cluster endpoint
    }
    return null;
  }

  private String getRdsInstanceHostPattern(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    if (matcher.find()) {
      return "?." + matcher.group(3);
    }
    return null;
  }

  private boolean isRdsClusterDns(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    String clusterKeyword = getClusterKeyword(matcher);
    return "cluster-".equalsIgnoreCase(clusterKeyword)
        || "cluster-ro-".equalsIgnoreCase(clusterKeyword);
  }

  private boolean isReaderClusterDns(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    return "cluster-ro-".equalsIgnoreCase(getClusterKeyword(matcher));
  }

  private String getClusterKeyword(Matcher matcher) {
    if (matcher.find()
        && matcher.group(2) != null
        && matcher.group(1) != null
        && !matcher.group(1).isEmpty()) {
      return matcher.group(2);
    }
    return null;
  }

  private int getRandomReaderIndex() {
    int max = this.hosts.size() - 1;
    int min = WRITER_CONNECTION_INDEX + 1;
    return (int) (Math.random() * ((max - min) + 1)) + min;
  }

  private boolean invalidWriterConnection() {
    return this.explicitlyReadOnly != null
        && !this.explicitlyReadOnly
        && !isWriterHostIndex(this.currentHostIndex);
  }

  private boolean isExplicitlyReadOnly() {
    return this.explicitlyReadOnly != null && this.explicitlyReadOnly;
  }

  private boolean shouldPerformWriterFailover() {
    return this.explicitlyReadOnly == null || !this.explicitlyReadOnly;
  }

  private void logTopology() {
    StringBuilder msg = new StringBuilder();
    for (int i = 0; i < this.hosts.size(); i++) {
      HostInfo hostInfo = this.hosts.get(i);
      msg.append("\n   [")
              .append(i)
              .append("]: ")
              .append(hostInfo == null ? "<null>" : hostInfo.getHost());
    }
    this.log.logTrace(
        Messages.getString("ClusterAwareConnectionProxy.16", new Object[] {msg.toString()}));
  }

  /**
   * Invalidates the current connection.
   *
   * @throws SQLException if an error occurs
   */
  @Override
  protected synchronized void invalidateCurrentConnection() throws SQLException {
    if (this.inTransaction) {
      try {
        this.currentConnection.rollback();
      } catch (SQLException e) {
        // eat
      }
    }
    super.invalidateConnection(this.currentConnection);
  }

  /**
   * Deals with InvocationException from proxied objects.
   *
   * @param e The Exception instance to check.
   * @throws SQLException if an error occurs
   * @throws Throwable if an error occurs
   * @throws InvocationTargetException if an error occurs
   */
  @Override
  protected synchronized void dealWithInvocationException(InvocationTargetException e)
      throws SQLException, Throwable, InvocationTargetException {
    Throwable t = e.getTargetException();

    if (t != null) {
      this.log.logTrace(Messages.getString("ClusterAwareConnectionProxy.17"), t);
      if (this.lastExceptionDealtWith != t && shouldExceptionTriggerConnectionSwitch(t)) {
        if (this.gatherPerfMetricsSetting) {
          long currentTimeMs = System.currentTimeMillis();
          this.metrics.registerFailureDetectionTime(currentTimeMs - this.invokeStartTimeMs);
          this.invokeStartTimeMs = 0;
          this.failoverStartTimeMs = currentTimeMs;
        }
        invalidateCurrentConnection();
        pickNewConnection();
        this.lastExceptionDealtWith = t;
      }
      throw t;
    }
    throw e;
  }

  /*
   * Local implementation for the connection switch exception checker.
   */
  @Override
  protected boolean shouldExceptionTriggerConnectionSwitch(Throwable t) {

    if (!isFailoverEnabled()) {
      this.log.logDebug(Messages.getString("ClusterAwareConnectionProxy.18"));
      return false;
    }

    String sqlState = null;
    if (t instanceof CommunicationsException || t instanceof CJCommunicationsException) {
      return true;
    } else if (t instanceof SQLException) {
      sqlState = ((SQLException) t).getSQLState();
    } else if (t instanceof CJException) {
      if (t.getCause() instanceof EOFException) { // Can not read response from server
        return true;
      }
      if (t.getCause() instanceof SSLException) { // Incomplete packets from server may cause SSL communication issues
        return true;
      }
      sqlState = ((CJException) t).getSQLState();
    }

    if (sqlState != null) {
      // connection error
      return sqlState.startsWith("08");
    }

    return false;
  }

  /** Checks if current connection is to a master (writer) host. */
  @Override
  protected boolean isMasterConnection() {
    return isWriterHostIndex(this.currentHostIndex);
  }

  /**
   * Checks if there is a underlying connection for this proxy.
   *
   * @return true if there is a connection
   */
  synchronized boolean isConnected() {
    return this.currentHostIndex != NO_CONNECTION_INDEX;
  }

  /*
   * Local implementation for the new connection picker.
   */
  @Override
  protected synchronized void pickNewConnection() throws SQLException {
    if (this.isClosed && this.closedExplicitly) {
      this.log.logDebug(Messages.getString("ClusterAwareConnectionProxy.19"));
      return;
    }

    if (isConnected()) {
      failover(this.currentHostIndex);
      return;
    }

    if (shouldAttemptReaderConnection()) {
      failoverReader(NO_CONNECTION_INDEX);
      return;
    }

    if (this.hosts.get(WRITER_CONNECTION_INDEX) == null) {
      failover(WRITER_CONNECTION_INDEX);
      return;
    }

    try {
      connectTo(WRITER_CONNECTION_INDEX);
      if (isExplicitlyReadOnly()) {
        topologyService.setLastUsedReaderHost(this.hosts.get(this.currentHostIndex));
      }
    } catch (SQLException e) {
      failover(WRITER_CONNECTION_INDEX);
    }
  }

  private boolean shouldAttemptReaderConnection() {
    return isExplicitlyReadOnly() && clusterContainsReader();
  }

  private boolean clusterContainsReader() {
    return this.hosts.size() > 1;
  }

  private int getHostIndex(HostInfo host) {
    if (host == null) {
      return NO_CONNECTION_INDEX;
    }
    for (int i = 0; i < this.hosts.size(); i++) {
      HostInfo potentialMatch = this.hosts.get(i);
      if (potentialMatch != null && potentialMatch.equalHostPortPair(host)) {
        return i;
      }
    }
    return NO_CONNECTION_INDEX;
  }

  /**
   * Connects this dynamic failover connection proxy to the host pointed out by the given host
   * index.
   *
   * @param hostIndex The host index in the global hosts list.
   * @throws SQLException if an error occurs
   */
  private synchronized void connectTo(int hostIndex) throws SQLException {
    try {
      switchCurrentConnectionTo(hostIndex, createConnectionForHostIndex(hostIndex));
      this.log.logDebug(
          Messages.getString(
              "ClusterAwareConnectionProxy.20", new Object[] {this.hosts.get(hostIndex)}));
    } catch (SQLException e) {
      if (this.currentConnection != null) {
        HostInfo host = this.hosts.get(hostIndex);
        StringBuilder msg =
            new StringBuilder("Connection to ")
                .append(isWriterHostIndex(hostIndex) ? "writer" : "reader")
                .append(" host '")
                .append(host == null ? "<null>" : host.getHostPortPair())
                .append("' failed");
        try {
          this.log.logWarn(msg.toString(), e);
        } catch (CJException ex) {
          throw SQLExceptionsMapping.translateException(
              e, this.currentConnection.getExceptionInterceptor());
        }
      }
      throw e;
    }
  }

  /**
   * Creates a new connection instance for host pointed out by the given host index.
   *
   * @param hostIndex The host index in the global hosts list.
   * @return The new connection instance.
   * @throws SQLException if an error occurs
   */
  private synchronized ConnectionImpl createConnectionForHostIndex(int hostIndex)
      throws SQLException {
    return createConnectionForHost(this.hosts.get(hostIndex));
  }

  /**
   * Creates a new physical connection for the given {@link HostInfo}.
   *
   * @param hostInfo The host info instance.
   * @return The new Connection instance.
   * @throws SQLException if an error occurs
   */
  @Override
  protected synchronized ConnectionImpl createConnectionForHost(HostInfo hostInfo)
      throws SQLException {
    ConnectionImpl conn = this.connectionProvider.connect(hostInfo);
    setConnectionProxy(conn);
    return conn;
  }

  protected void setConnectionProxy(JdbcConnection conn) {
    JdbcConnection topmostProxy = getProxy();
    if (topmostProxy != this.thisAsConnection) {
      conn.setProxy(
          this
              .thisAsConnection);
      // First call sets this connection as underlying connection parent proxy (its creator).
    }
    conn.setProxy(topmostProxy); // Set the topmost proxy in the underlying connection.
  }

  /**
   * Replaces the previous underlying connection by the connection given. State from previous
   * connection, if any, is synchronized with the new one.
   *
   * @param hostIndex The host index in the global hosts list that matches the given connection.
   * @param connection The connection instance to switch to.
   * @throws SQLException if an error occurs
   */
  private synchronized void switchCurrentConnectionTo(int hostIndex, JdbcConnection connection)
      throws SQLException {
    invalidateCurrentConnection();

    boolean readOnly;
    if (isWriterHostIndex(hostIndex)) {
      readOnly = isExplicitlyReadOnly();
    } else if (this.explicitlyReadOnly != null) {
      readOnly = this.explicitlyReadOnly;
    } else if (this.currentConnection != null) {
      readOnly = this.currentConnection.isReadOnly();
    } else {
      readOnly = false;
    }
    syncSessionState(this.currentConnection, connection, readOnly);
    this.currentConnection = connection;
    this.currentHostIndex = hostIndex;
    this.inTransaction = false;
  }

  /**
   * Checks if the given host index points to the primary host.
   *
   * @param hostIndex The host index in the global hosts list.
   * @return true if so
   */
  private synchronized boolean isWriterHostIndex(int hostIndex) {
    return hostIndex == WRITER_CONNECTION_INDEX;
  }

  protected synchronized boolean isCurrentConnectionReadOnly() {
    return isConnected() && !isWriterHostIndex(this.currentHostIndex);
  }

  protected synchronized boolean isCurrentConnectionWriter() {
    return isWriterHostIndex(this.currentHostIndex);
  }

  /**
   * Initiates a default failover procedure starting at the given host index. This process tries to
   * connect, sequentially, to the next host in the list. The primary host may or may not be
   * excluded from the connection attempts.
   *
   * @param failedHostIdx The host index where to start from. First connection attempt will be the
   *     next one.
   * @throws SQLException if an error occurs
   */
  protected synchronized void failover(int failedHostIdx) throws SQLException {
    if (shouldPerformWriterFailover()) {
      failoverWriter();
    } else {
      failoverReader(failedHostIdx);
    }

    if (this.inTransaction) {
      this.inTransaction = false;

      // "Transaction resolution unknown. Please re-configure session state if required and try
      // restarting transaction."
      this.log.logError(Messages.getString("ClusterAwareConnectionProxy.1"));
      throw new SQLException(
          Messages.getString("ClusterAwareConnectionProxy.1"),
          MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN);
    } else {
      // "The active SQL connection has changed due to a connection failure. Please re-configure
      // session state if required."
      this.log.logError(Messages.getString("ClusterAwareConnectionProxy.3"));
      throw new SQLException(
          Messages.getString("ClusterAwareConnectionProxy.3"),
          MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_CHANGED);
    }
  }

  protected void failoverWriter() throws SQLException {
    this.log.logDebug(Messages.getString("ClusterAwareConnectionProxy.21"));

    ResolvedHostInfo failoverResult = this.writerFailoverHandler.failover(this.hosts);

    if (this.gatherPerfMetricsSetting) {
      long currentTimeMs = System.currentTimeMillis();
      this.metrics.registerWriterFailoverProcedureTime(currentTimeMs - this.failoverStartTimeMs);
      this.failoverStartTimeMs = 0;
    }

    if (failoverResult == null || !failoverResult.isConnected()) {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerFailoverConnects(false);
      }

      // "Unable to establish SQL connection to writer node"
      this.log.logError(Messages.getString("ClusterAwareConnectionProxy.2"));
      throw new SQLException(
          Messages.getString("ClusterAwareConnectionProxy.2"),
          MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE);
    } else if (failoverResult.isNewHost()) {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerFailoverConnects(true);
      }

      // connected to a new writer host; take it over
      this.hosts = failoverResult.getTopology();
      this.currentHostIndex = WRITER_CONNECTION_INDEX;
      this.currentConnection = failoverResult.getNewConnection();
      setConnectionProxy(this.currentConnection);

      this.log.logDebug(
          Messages.getString(
              "ClusterAwareConnectionProxy.20",
              new Object[] {this.hosts.get(this.currentHostIndex)}));
    } else {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerFailoverConnects(true);
      }

      // successfully re-connected to the same writer node
      this.currentHostIndex = WRITER_CONNECTION_INDEX;
      this.currentConnection = failoverResult.getNewConnection();
      setConnectionProxy(this.currentConnection);

      this.log.logDebug(
          Messages.getString(
              "ClusterAwareConnectionProxy.20",
              new Object[] {this.hosts.get(this.currentHostIndex)}));
    }
  }

  protected void failoverReader(int failedHostIdx) throws SQLException {
    this.log.logDebug(Messages.getString("ClusterAwareConnectionProxy.23"));

    HostInfo failedHost =
        failedHostIdx == NO_CONNECTION_INDEX ? null : this.hosts.get(failedHostIdx);
    ConnectionAttemptResult result = readerFailoverHandler.failover(this.hosts, failedHost);

    if (this.gatherPerfMetricsSetting) {
      long currentTimeMs = System.currentTimeMillis();
      this.metrics.registerReaderFailoverProcedureTime(currentTimeMs - this.failoverStartTimeMs);
      this.failoverStartTimeMs = 0;
    }

    if (result == null || !result.isSuccess()) {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerFailoverConnects(false);
      }

      // "Unable to establish SQL connection to reader node"
      this.log.logError(Messages.getString("ClusterAwareConnectionProxy.4"));
      throw new SQLException(
          Messages.getString("ClusterAwareConnectionProxy.4"),
          MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE);
    } else {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerFailoverConnects(true);
      }

      this.currentConnection = result.getConnection();
      setConnectionProxy(this.currentConnection);
      this.currentHostIndex = result.getConnectionIndex();
      updateTopologyAndConnectIfNeeded(true);
      this.log.logDebug(
          Messages.getString(
              "ClusterAwareConnectionProxy.20",
              new Object[] {this.hosts.get(this.currentHostIndex)}));
      HostInfo currentHost = this.hosts.get(this.currentHostIndex);
      if (currentHost != null) {
        topologyService.setLastUsedReaderHost(currentHost);
      }
    }
  }

  /**
   * Closes current connection.
   *
   * @throws SQLException if an error occurs
   */
  @Override
  protected synchronized void doClose() throws SQLException {
    this.currentConnection.close();
  }

  /**
   * Aborts current connection.
   *
   * @throws SQLException if an error occurs
   */
  @Override
  protected synchronized void doAbortInternal() throws SQLException {
    this.currentConnection.abortInternal();
  }

  /**
   * Aborts current connection using the given executor.
   *
   * @throws SQLException if an error occurs
   */
  @Override
  protected synchronized void doAbort(Executor executor) throws SQLException {
    this.currentConnection.abort(executor);
  }

  protected void updateTopologyAndConnectIfNeeded(boolean forceUpdate) throws SQLException {
    if (!isFailoverEnabled() || this.currentConnection.isClosed()) {
      return;
    }

    List<HostInfo> latestTopology =
        this.topologyService.getTopology(this.currentConnection, forceUpdate);
    if (latestTopology == null) {
      return;
    }

    if (!isConnected()) {
      this.hosts = latestTopology;
      pickNewConnection();
      return;
    }

    HostInfo currentHost = this.hosts.get(this.currentHostIndex);

    int latestHostIndex = NO_CONNECTION_INDEX;
    for (int i = 0; i < latestTopology.size(); i++) {
      HostInfo host = latestTopology.get(i);
      if (host != null && currentHost != null && host.equalHostPortPair(currentHost)) {
        latestHostIndex = i;
        break;
      }
    }

    if (latestHostIndex == NO_CONNECTION_INDEX) {
      // current connection host isn't found in the latest topology
      // switch to another connection;
      this.hosts = latestTopology;
      this.currentHostIndex = NO_CONNECTION_INDEX;
      pickNewConnection();
    } else {
      // found the same node at different position in the topology
      // adjust current index only; connection is still valid
      this.hosts = latestTopology;
      this.currentHostIndex = latestHostIndex;
    }
  }

  @Override
  public synchronized Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    this.invokeStartTimeMs = this.gatherPerfMetricsSetting ? System.currentTimeMillis() : 0;

    Object result = super.invoke(proxy, method, args);

    if (METHOD_CLOSE.equals(method.getName())) {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.reportMetrics(this.log);
        if (this.topologyService instanceof CanCollectPerformanceMetrics) {
          ((CanCollectPerformanceMetrics) this.topologyService).reportMetrics(this.log);
        }
      }
    }

    return result;
  }

  /*
   * Local method invocation handling for this proxy.
   * This is the continuation of MultiHostConnectionProxy#invoke(Object, Method, Object[]).
   */
  @Override
  public synchronized Object invokeMore(Object proxy, Method method, Object[] args)
      throws Throwable {
    final String methodName = method.getName();

    updateTopologyAndConnectIfNeeded(false);

    if (this.isClosed && !allowedOnClosedConnection(method)) {
      if (this.autoReconnect && !this.closedExplicitly) {
        this.currentHostIndex = NO_CONNECTION_INDEX; // Act as if this is the first connection but let it sync with the previous one.
        this.isClosed = false;
        this.closedReason = null;
        pickNewConnection();

        // "The active SQL connection has changed. Please re-configure session state if required."
        this.log.logError(Messages.getString("ClusterAwareConnectionProxy.25"));
        throw new SQLException(
                Messages.getString("ClusterAwareConnectionProxy.25"),
                MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_CHANGED);
      } else {
        String reason = "No operations allowed after connection closed.";
        if (this.closedReason != null) {
          reason += ("  " + this.closedReason);
        }
        throw SQLError.createSQLException(reason, MysqlErrorNumbers.SQL_STATE_CONNECTION_NOT_OPEN, null /* no access to a interceptor here... */);
      }
    }

    Object result = null;
    try {
      result = method.invoke(this.thisAsConnection, args);
      result = proxyIfReturnTypeIsJdbcInterface(method.getReturnType(), result);
    } catch (InvocationTargetException e) {
      dealWithInvocationException(e);
    }

    if (METHOD_SET_AUTO_COMMIT.equals(methodName)) {
      this.explicitlyAutoCommit = (Boolean) args[0];
      this.inTransaction = !this.explicitlyAutoCommit;
    }

    if (METHOD_COMMIT.equals(methodName) || METHOD_ROLLBACK.equals(methodName)) {
      this.inTransaction = false;
    }

    if (METHOD_SET_READ_ONLY.equals(methodName)) {
      this.explicitlyReadOnly = (Boolean) args[0];
      this.log.logTrace(
          Messages.getString(
              "ClusterAwareConnectionProxy.12",
              new Object[] {"explicitlyReadOnly", this.explicitlyReadOnly}));
      connectToWriterIfRequired(this.explicitlyReadOnly);
    }

    return result;
  }

  private void connectToWriterIfRequired(Boolean readOnly) throws SQLException {
    if (readOnly != null && !readOnly && !isWriterHostIndex(this.currentHostIndex)) {
      if (this.hosts.get(WRITER_CONNECTION_INDEX) == null) {
        failover(WRITER_CONNECTION_INDEX);
        return;
      }

      try {
        connectTo(WRITER_CONNECTION_INDEX);
      } catch (SQLException e) {
        failover(WRITER_CONNECTION_INDEX);
      }
    }
  }

  @Override
  public ConnectionLifecycleInterceptor getConnectionLifecycleInterceptor() {
    return new ClusterAwareConnectionLifecycleInterceptor(this);
  }

  protected JdbcConnection getConnection() {
    return this.currentConnection;
  }
}
