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
import com.mysql.cj.NativeSession;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.ha.ConnectionUtils;
import com.mysql.cj.jdbc.ha.plugins.BasicConnectionProvider;
import com.mysql.cj.jdbc.ha.plugins.ICanCollectPerformanceMetrics;
import com.mysql.cj.jdbc.ha.plugins.IConnectionPlugin;
import com.mysql.cj.jdbc.ha.plugins.IConnectionProvider;
import com.mysql.cj.jdbc.ha.plugins.ICurrentConnectionProvider;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.IpAddressUtils;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

import java.io.EOFException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLException;

/**
 * A {@link IConnectionPlugin} implementation that provides cluster-aware failover
 * features. Connection switching occurs on communications related exceptions and/or
 * cluster topology changes.
 */
public class FailoverConnectionPlugin implements IConnectionPlugin {
  public static final int NO_CONNECTION_INDEX = -1;
  public static final int WRITER_CONNECTION_INDEX = 0;
  static final String METHOD_SET_READ_ONLY = "setReadOnly";
  static final String METHOD_SET_AUTO_COMMIT = "setAutoCommit";
  static final String METHOD_COMMIT = "commit";
  static final String METHOD_ROLLBACK = "rollback";
  static final String METHOD_CLOSE = "close";
  private static final String METHOD_GET_AUTO_COMMIT = "getAutoCommit";
  private static final String METHOD_GET_CATALOG = "getCatalog";
  private static final String METHOD_GET_SCHEMA = "getSchema";
  private static final String METHOD_GET_DATABASE = "getDatabase";
  private static final String METHOD_GET_TRANSACTION_ISOLATION =
      "getTransactionIsolation";
  private static final String METHOD_GET_SESSION_MAX_ROWS = "getSessionMaxRows";
  protected final IConnectionProvider connectionProvider;
  protected final ClusterAwareMetrics metrics = new ClusterAwareMetrics();
  private final ICurrentConnectionProvider currentConnectionProvider;
  private final PropertySet propertySet;
  private final IConnectionPlugin nextPlugin;
  private final Log logger;
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
  protected IWriterFailoverHandler writerFailoverHandler = null;
  protected IReaderFailoverHandler readerFailoverHandler = null;
  // writer host is always stored at index 0
  protected int currentHostIndex = NO_CONNECTION_INDEX;
  protected Map<String, String> initialConnectionProps;
  protected Boolean explicitlyReadOnly = null;
  protected boolean inTransaction = false;
  protected boolean explicitlyAutoCommit = true;
  protected boolean isClusterTopologyAvailable = false;
  protected boolean isMultiWriterCluster = false;
  protected boolean isRdsProxy = false;
  protected boolean isRds = false;
  protected ITopologyService topologyService;
  protected List<HostInfo> hosts = new ArrayList<>();

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
  protected boolean isClosed = false;
  protected boolean closedExplicitly = false;
  protected String closedReason = null;
  // Keep track of the last exception processed in 'dealWithInvocationException()' in order to avoid creating connections repeatedly from each time the same
  // exception is caught in every proxy instance belonging to the same call stack.
  protected Throwable lastExceptionDealtWith = null;
  protected boolean autoReconnect;

  private long invokeStartTimeMs;
  private long failoverStartTimeMs;

  public FailoverConnectionPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger) throws SQLException {
    this(
        currentConnectionProvider,
        propertySet,
        nextPlugin,
        logger,
        new BasicConnectionProvider(),
        () -> new AuroraTopologyService(logger));
  }

  FailoverConnectionPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger,
      IConnectionProvider connectionProvider,
      Supplier<ITopologyService> topologyServiceSupplier) throws SQLException {
    this.currentConnectionProvider = currentConnectionProvider;
    this.propertySet = propertySet;
    this.nextPlugin = nextPlugin;
    this.logger = logger;
    this.connectionProvider = connectionProvider;

    this.initialConnectionProps = new HashMap<>();
    Properties originalProperties = this.propertySet.exposeAsProperties();
    if (originalProperties != null) {
      for (String p : originalProperties.stringPropertyNames()) {
        this.initialConnectionProps.put(p, originalProperties.getProperty(p));
      }
    }

    initSettings();

    if (!this.enableFailoverSetting) {
      return;
    }

    this.topologyService = topologyServiceSupplier.get();
    if (this.topologyService instanceof  ICanCollectPerformanceMetrics) {
      ((ICanCollectPerformanceMetrics)topologyService)
        .setPerformanceMetricsEnabled(this.gatherPerfMetricsSetting);
    }
    topologyService.setRefreshRate(this.clusterTopologyRefreshRateMsSetting);

    this.readerFailoverHandler =
        new ClusterAwareReaderFailoverHandler(
            this.topologyService,
            this.connectionProvider,
            this.initialConnectionProps,
            this.failoverTimeoutMsSetting,
            this.failoverReaderConnectTimeoutMsSetting,
            this.logger);
    this.writerFailoverHandler =
        new ClusterAwareWriterFailoverHandler(
            this.topologyService,
            this.connectionProvider,
            this.readerFailoverHandler,
            this.initialConnectionProps,
            this.failoverTimeoutMsSetting,
            this.failoverClusterTopologyRefreshRateMsSetting,
            this.failoverWriterReconnectIntervalMsSetting,
            this.logger);

    initProxy();
  }

  @Override
  public void openInitialConnection(ConnectionUrl connectionUrl) throws SQLException {
    createConnection(connectionUrl);

    if (this.enableFailoverSetting) {
      initProxy();
    }
  }

  @Override
  public Object execute(
      Class<?> methodInvokeOn,
      String methodName,
      Callable<?> executeSqlFunc,
      Object[] args)
      throws Exception {

    if (!this.enableFailoverSetting) {
      return this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc, args);
    }

    if (this.isClosed && !allowedOnClosedConnection(methodName)) {
      invalidInvocationOnClosedConnection();
    }

    this.invokeStartTimeMs =
        this.gatherPerfMetricsSetting ? System.currentTimeMillis() : 0;

    Object result = null;


    try {
      updateTopologyAndConnectIfNeeded(false);
      result = this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc, args);
    } catch (IllegalStateException e) {
      dealWithIllegalStateException(e);
    } catch (Exception e) {
      this.dealWithOriginalException(e, null);
    }

    performSpecialMethodHandlingIfRequired(args, methodName);

    if (METHOD_CLOSE.equals(methodName)) {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.reportMetrics(this.logger);
        if (this.topologyService instanceof ICanCollectPerformanceMetrics) {
          ((ICanCollectPerformanceMetrics) this.topologyService).reportMetrics(this.logger);
        }
      }
    }

    return result;
  }

  @Override
  public void transactionBegun() {
    this.inTransaction = true;
    this.nextPlugin.transactionBegun();
  }

  @Override
  public void transactionCompleted() {
    this.inTransaction = false;
    this.nextPlugin.transactionCompleted();
  }

  @Override
  public void releaseResources() {
    this.nextPlugin.releaseResources();
  }

  public boolean isFailoverEnabled() {
    return this.enableFailoverSetting
        && !this.isRdsProxy
        && this.isClusterTopologyAvailable
        && !this.isMultiWriterCluster
        && (this.hosts == null || this.hosts.size() > 1);
  }

  /**
   * Checks if the proxy is connected to an RDS-hosted cluster.
   *
   * @return true if the proxy is connected to an RDS-hosted cluster
   */
  public boolean isRds() {
    return this.isRds;
  }

  /**
   * Checks if the proxy is connected to a cluster using RDS proxy.
   *
   * @return true if the proxy is connected to a cluster using RDS proxy
   */
  public boolean isRdsProxy() {
    return this.isRdsProxy;
  }

  void initSettings() {
    this.enableFailoverSetting =
      propertySet.getBooleanProperty(PropertyKey.enableClusterAwareFailover).getValue();

    this.clusterTopologyRefreshRateMsSetting =
      propertySet
        .getIntegerProperty(PropertyKey.clusterTopologyRefreshRateMs)
        .getValue();
    this.gatherPerfMetricsSetting =
      propertySet.getBooleanProperty(PropertyKey.gatherPerfMetrics).getValue();
    this.failoverTimeoutMsSetting =
      propertySet.getIntegerProperty(PropertyKey.failoverTimeoutMs).getValue();
    this.failoverClusterTopologyRefreshRateMsSetting =
      propertySet
        .getIntegerProperty(PropertyKey.failoverClusterTopologyRefreshRateMs)
        .getValue();
    this.failoverWriterReconnectIntervalMsSetting =
      propertySet
        .getIntegerProperty(PropertyKey.failoverWriterReconnectIntervalMs)
        .getValue();
    this.failoverReaderConnectTimeoutMsSetting =
      propertySet
        .getIntegerProperty(PropertyKey.failoverReaderConnectTimeoutMs)
        .getValue();
    this.clusterIdSetting =
      propertySet.getStringProperty(PropertyKey.clusterId).getValue();
    this.clusterInstanceHostPatternSetting =
      propertySet.getStringProperty(PropertyKey.clusterInstanceHostPattern).getValue();

    this.failoverConnectTimeoutMs =
      propertySet.getIntegerProperty(PropertyKey.connectTimeout).getValue();
    this.failoverSocketTimeoutMs =
      propertySet.getIntegerProperty(PropertyKey.socketTimeout).getValue();

    this.autoReconnect =
      propertySet.getBooleanProperty(PropertyKey.autoReconnect.getKeyName()).getValue()
      || propertySet.getBooleanProperty(PropertyKey.autoReconnectForPools.getKeyName()).getValue();
  }

  /**
   * Checks if there is an underlying connection for this proxy.
   *
   * @return true if there is a connection
   */
  boolean isConnected() {
    return this.currentHostIndex != NO_CONNECTION_INDEX;
  }

  private void validateConnection() throws SQLException {
    this.currentHostIndex =
            getHostIndex(topologyService.getHostByName(this.currentConnectionProvider.getCurrentConnection()));
    if (!isConnected()) {
      pickNewConnection();
      return;
    }

    if (validWriterConnection()) {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerInvalidInitialConnection(false);
      }
      return;
    }

    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerInvalidInitialConnection(true);
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

  private boolean validWriterConnection() {
    return this.explicitlyReadOnly == null
            || this.explicitlyReadOnly
            || isWriterHostIndex(this.currentHostIndex);
  }

  private void attemptConnectionUsingCachedTopology() throws SQLException {
    List<HostInfo> cachedHosts = topologyService.getCachedTopology();
    if (Util.isNullOrEmpty(cachedHosts)) {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerUseCachedTopology(false);
      }
      return;
    }

    this.hosts = cachedHosts;

    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerUseCachedTopology(true);
    }

    int candidateIndex = getCandidateIndexForInitialConnection();
    if (candidateIndex != NO_CONNECTION_INDEX) {
      connectTo(candidateIndex);
    }
  }

  private int getCandidateIndexForInitialConnection() {
    if (Util.isNullOrEmpty(this.hosts)) {
      return NO_CONNECTION_INDEX;
    }

    if (isExplicitlyReadOnly()) {
      int candidateReaderIndex = getCandidateReaderForInitialConnection();
      if (candidateReaderIndex != NO_CONNECTION_INDEX) {
        return candidateReaderIndex;
      }
    }
    return WRITER_CONNECTION_INDEX;
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

  private int getRandomReaderIndex() {
    int max = this.hosts.size() - 1;
    int min = WRITER_CONNECTION_INDEX + 1;
    return (int) (Math.random() * ((max - min) + 1)) + min;
  }

  protected void initializeTopology() throws SQLException {
    if (this.currentConnectionProvider.getCurrentConnection() == null) {
      return;
    }

    fetchTopology();
    if (this.isFailoverEnabled()) {
      validateConnection();

      if (this.currentHostIndex != NO_CONNECTION_INDEX
          && !Util.isNullOrEmpty(this.hosts)) {
        HostInfo currentHost = this.hosts.get(this.currentHostIndex);
        if (isExplicitlyReadOnly()) {
          topologyService.setLastUsedReaderHost(currentHost);
        }
      }

      final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();

      currentConnection
          .getPropertySet()
          .getIntegerProperty(PropertyKey.socketTimeout)
          .setValue(this.failoverSocketTimeoutMs);
      ((NativeSession) currentConnection.getSession()).setSocketTimeout(this.failoverSocketTimeoutMs);
    }
  }

  /**
   * Creates a new physical connection for the given {@link HostInfo}.
   *
   * @param baseHostInfo The host info instance to base the connection off of.
   * @return The new Connection instance.
   * @throws SQLException if an error occurs
   */
  protected ConnectionImpl createConnectionForHost(HostInfo baseHostInfo)
      throws SQLException {
    HostInfo hostInfoWithInitialProps = ConnectionUtils.copyWithAdditionalProps(
        baseHostInfo,
        this.currentConnectionProvider.getCurrentHostInfo());
    return this.connectionProvider.connect(hostInfoWithInitialProps);
  }

  protected void dealWithIllegalStateException(IllegalStateException e) throws Exception {
    dealWithOriginalException(e.getCause(), e);
  }

  /**
   * Initiates the failover procedure. This process tries to establish a new connection to an instance in the topology.
   *
   * @param failedHostIdx The index of the host that failed
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
      this.logger.logError(Messages.getString("ClusterAwareConnectionProxy.1"));
      throw new SQLException(
          Messages.getString("ClusterAwareConnectionProxy.1"),
          MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN);
    } else {
      // "The active SQL connection has changed due to a connection failure. Please re-configure
      // session state if required."
      this.logger.logError(Messages.getString("ClusterAwareConnectionProxy.3"));
      throw new SQLException(
          Messages.getString("ClusterAwareConnectionProxy.3"),
          MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_CHANGED);
    }
  }

  protected void failoverReader(int failedHostIdx) throws SQLException {
    this.logger.logDebug(Messages.getString("ClusterAwareConnectionProxy.17"));

    HostInfo failedHost = null;
    if (failedHostIdx != NO_CONNECTION_INDEX && !Util.isNullOrEmpty(this.hosts)) {
      failedHost = this.hosts.get(failedHostIdx);
    }
    ReaderFailoverResult result = readerFailoverHandler.failover(this.hosts, failedHost);

    if (this.gatherPerfMetricsSetting) {
      long currentTimeMs = System.currentTimeMillis();
      this.metrics.registerReaderFailoverProcedureTime(
          currentTimeMs - this.failoverStartTimeMs);
      this.failoverStartTimeMs = 0;
    }

    if (result == null || !result.isConnected()) {
      // "Unable to establish SQL connection to reader node"
      processFailoverFailure(Messages.getString("ClusterAwareConnectionProxy.4"));
      return;
    }

    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerFailoverConnects(true);
    }

    updateCurrentConnection(
        result.getConnection(),
        result.getConnectionIndex());
    updateTopologyAndConnectIfNeeded(true);

    if (this.currentHostIndex != NO_CONNECTION_INDEX && !Util.isNullOrEmpty(this.hosts)) {
      HostInfo currentHost = this.hosts.get(this.currentHostIndex);
      topologyService.setLastUsedReaderHost(currentHost);
      this.logger.logDebug(
          Messages.getString(
              "ClusterAwareConnectionProxy.15",
              new Object[] {currentHost}));
    }
  }

  protected void failoverWriter() throws SQLException {
    this.logger.logDebug(Messages.getString("ClusterAwareConnectionProxy.16"));
    WriterFailoverResult failoverResult = this.writerFailoverHandler.failover(this.hosts);

    if (this.gatherPerfMetricsSetting) {
      long currentTimeMs = System.currentTimeMillis();
      this.metrics.registerWriterFailoverProcedureTime(
          currentTimeMs - this.failoverStartTimeMs);
      this.failoverStartTimeMs = 0;
    }

    if (failoverResult == null || !failoverResult.isConnected()) {
      // "Unable to establish SQL connection to writer node"
      processFailoverFailure(Messages.getString("ClusterAwareConnectionProxy.2"));
      return;
    }

    if (!Util.isNullOrEmpty(failoverResult.getTopology())) {
      this.hosts = failoverResult.getTopology();
    }

    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerFailoverConnects(true);
    }

    // successfully re-connected to the same writer node
    updateCurrentConnection(
        failoverResult.getNewConnection(),
        WRITER_CONNECTION_INDEX);

    this.logger.logDebug(
        Messages.getString(
            "ClusterAwareConnectionProxy.15",
            new Object[] {this.hosts.get(this.currentHostIndex)}));
  }

  protected void invalidateCurrentConnection() {
    final JdbcConnection conn = this.currentConnectionProvider.getCurrentConnection();
    if (this.inTransaction) {
      try {
        conn.rollback();
      } catch (SQLException e) {
        // eat
      }
    }

    try {
      if (conn != null && !conn.isClosed()) {
        conn.realClose(true, !conn.getAutoCommit(), true, null);
      }
    } catch (SQLException e) {
      // swallow this exception, current connection should be useless anyway.
    }
  }

  protected synchronized void pickNewConnection() throws SQLException {
    if (this.isClosed && this.closedExplicitly) {
      this.logger.logDebug(Messages.getString("ClusterAwareConnectionProxy.1"));

      return;
    }

    if (isConnected() || Util.isNullOrEmpty(this.hosts)) {
      failover(this.currentHostIndex);
      return;
    }

    if (shouldAttemptReaderConnection()) {
      failoverReader(NO_CONNECTION_INDEX);
      return;
    }

    try {
      connectTo(WRITER_CONNECTION_INDEX);
      if (isExplicitlyReadOnly() && this.currentHostIndex != NO_CONNECTION_INDEX) {
        topologyService.setLastUsedReaderHost(this.hosts.get(this.currentHostIndex));
      }
    } catch (SQLException e) {
      failover(WRITER_CONNECTION_INDEX);
    }
  }

  protected boolean shouldExceptionTriggerConnectionSwitch(Throwable t) {

    if (!isFailoverEnabled()) {
      this.logger.logDebug(Messages.getString("ClusterAwareConnectionProxy.13"));
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

  /**
   * Synchronizes session state between two connections, allowing to override the read-only status.
   *
   * @param source
   *            The connection where to get state from.
   * @param target
   *            The connection where to set state.
   * @param readOnly
   *            The new read-only status.
   * @throws SQLException
   *             if an error occurs
   */
  protected void syncSessionState(
      JdbcConnection source,
      JdbcConnection target,
      boolean readOnly) throws SQLException {
    if (target != null) {
      target.setReadOnly(readOnly);
    }

    if (source == null || target == null) {
      return;
    }

    RuntimeProperty<Boolean> sourceUseLocalSessionState =
        source.getPropertySet().getBooleanProperty(PropertyKey.useLocalSessionState);
    boolean prevUseLocalSessionState = sourceUseLocalSessionState.getValue();
    sourceUseLocalSessionState.setValue(true);

    target.setAutoCommit(source.getAutoCommit());
    String db = source.getDatabase();
    if (db != null && !db.isEmpty()) {
      target.setDatabase(db);
    }
    target.setTransactionIsolation(source.getTransactionIsolation());
    target.setSessionMaxRows(source.getSessionMaxRows());

    sourceUseLocalSessionState.setValue(prevUseLocalSessionState);
  }

  protected void updateTopologyAndConnectIfNeeded(boolean forceUpdate)
      throws SQLException {
    JdbcConnection connection = this.currentConnectionProvider.getCurrentConnection();
    if (!isFailoverEnabled()
        || connection == null
        || connection.isClosed()) {
      return;
    }

    List<HostInfo> latestTopology =
        this.topologyService.getTopology(connection, forceUpdate);

    this.hosts = latestTopology;
    updateHostIndex(latestTopology);
  }

  boolean isCurrentConnectionReadOnly() {
    return isConnected() && !isWriterHostIndex(this.currentHostIndex);
  }

  protected boolean isCurrentConnectionWriter() {
    return isWriterHostIndex(this.currentHostIndex);
  }

  /**
   * Checks if the given method is allowed on closed connections.
   *
   * @return true if the given method is allowed on closed connections
   */
  private boolean allowedOnClosedConnection(String methodName) {
    return methodName.equals(METHOD_GET_AUTO_COMMIT) || methodName.equals(
        METHOD_GET_CATALOG) || methodName.equals(METHOD_GET_SCHEMA)
        || methodName.equals(METHOD_GET_DATABASE) || methodName.equals(
        METHOD_GET_TRANSACTION_ISOLATION)
        || methodName.equals(METHOD_GET_SESSION_MAX_ROWS);
  }

  private boolean clusterContainsReader() {
    return this.hosts.size() > 1;
  }

  /**
   * Connects this dynamic failover connection proxy to the host pointed out by the given host
   * index.
   *
   * @param hostIndex The host index in the global hosts list.
   * @throws SQLException if an error occurs
   */
  private void connectTo(int hostIndex) throws SQLException {
    try {
      switchCurrentConnectionTo(hostIndex, createConnectionForHostIndex(hostIndex));
      this.logger.logDebug(
          Messages.getString(
              "ClusterAwareConnectionProxy.15",
              new Object[] {this.hosts.get(hostIndex)}));
    } catch (SQLException e) {
      if (this.currentConnectionProvider.getCurrentConnection() != null) {
        HostInfo host = this.hosts.get(hostIndex);
        StringBuilder msg =
            new StringBuilder("Connection to ")
                .append(isWriterHostIndex(hostIndex) ? "writer" : "reader")
                .append(" host '")
                .append(host == null ? "<null>" : host.getHostPortPair())
                .append("' failed");
        try {
          this.logger.logWarn(msg.toString(), e);
        } catch (CJException ex) {
          throw SQLExceptionsMapping.translateException(
              e,
              this.currentConnectionProvider
                  .getCurrentConnection()
                  .getExceptionInterceptor());
        }
      }
      throw e;
    }
  }

  private void connectToWriterIfRequired(Boolean readOnly) throws SQLException {
    if (shouldReconnectToWriter(readOnly) && !Util.isNullOrEmpty(this.hosts)) {
      try {
        connectTo(WRITER_CONNECTION_INDEX);
      } catch (SQLException e) {
        failover(WRITER_CONNECTION_INDEX);
      }
    }
  }

  private HostInfo createClusterInstanceTemplate(
      HostInfo hostInfo,
      String host,
      int port) {
    // TODO: review whether we still need this method
    Map<String, String> properties = new HashMap<>(this.initialConnectionProps);
    properties.put(
        PropertyKey.connectTimeout.getKeyName(),
        String.valueOf(this.failoverConnectTimeoutMs));
    properties.put(
        PropertyKey.socketTimeout.getKeyName(),
        String.valueOf(this.failoverSocketTimeoutMs));

    if (!Objects.equals(hostInfo.getDatabase(), "")) {
      properties.put(
          PropertyKey.DBNAME.getKeyName(),
          hostInfo.getDatabase());
    }

    final ConnectionUrl connectionUrl = ConnectionUrl.getConnectionUrlInstance(
            hostInfo.getDatabaseUrl(), this.propertySet.exposeAsProperties());

    return new HostInfo(
        connectionUrl,
        host,
        port,
        hostInfo.getUser(),
        hostInfo.getPassword(),
        hostInfo.isPasswordless(),
        properties);
  }

  /**
   * Creates a new connection instance for host pointed out by the given host index.
   *
   * @param hostIndex The host index in the global hosts list.
   * @return The new connection instance.
   * @throws SQLException if an error occurs
   */
  private ConnectionImpl createConnectionForHostIndex(int hostIndex)
      throws SQLException {
    return createConnectionForHost(this.hosts.get(hostIndex));
  }

  private void dealWithOriginalException(
      Throwable originalException,
      Exception wrapperException) throws Exception {
    if (originalException != null) {
      this.logger.logTrace(
          Messages.getString("ClusterAwareConnectionProxy.12"),
          originalException);
      if (this.lastExceptionDealtWith != originalException
          && shouldExceptionTriggerConnectionSwitch(originalException)) {
        if (this.gatherPerfMetricsSetting) {
          long currentTimeMs = System.currentTimeMillis();
          this.metrics.registerFailureDetectionTime(
              currentTimeMs - this.invokeStartTimeMs);
          this.invokeStartTimeMs = 0;
          this.failoverStartTimeMs = currentTimeMs;
        }
        invalidateCurrentConnection();
        pickNewConnection();
        this.lastExceptionDealtWith = originalException;
      }

      if (originalException instanceof Error) {
        throw (Error) originalException;
      }
      throw (Exception) originalException;
    }
    throw wrapperException;
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

  private int getHostIndex(HostInfo host) {
    if (host == null || Util.isNullOrEmpty(this.hosts)) {
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

  private ConnectionUrlParser.Pair<String, Integer> getHostPortPairFromHostPatternSetting()
      throws SQLException {
    ConnectionUrlParser.Pair<String, Integer> pair = ConnectionUrlParser.parseHostPortPair(
            this.clusterInstanceHostPatternSetting);
    if (pair == null) {
      // "Invalid value for the 'clusterInstanceHostPattern' configuration setting - the value could not be parsed"
      throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.5"));
    }

    validateHostPatternSetting(pair.left);
    return pair;
  }

  private String getRdsClusterHostUrl(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    String clusterKeyword = getClusterKeyword(matcher);
    if ("cluster-".equalsIgnoreCase(clusterKeyword)
        || "cluster-ro-".equalsIgnoreCase(clusterKeyword)) {
      return matcher.group(1) + ".cluster-"
          + matcher.group(3); // always RDS cluster endpoint
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

  private void identifyRdsType(String host) {
    this.isRds = isRdsDns(host);
    this.logger.logTrace(
        Messages.getString(
            "ClusterAwareConnectionProxy.10",
            new Object[] {"isRds", this.isRds}));

    this.isRdsProxy = isRdsProxyDns(host);
    this.logger.logTrace(
        Messages.getString(
            "ClusterAwareConnectionProxy.10",
            new Object[] {"isRdsProxy", this.isRdsProxy}));
  }

  private void initExpectingNoTopology(HostInfo hostInfo)
      throws SQLException {
    setClusterId(hostInfo.getHost(), hostInfo.getPort());
    this.topologyService.setClusterInstanceTemplate(
        createClusterInstanceTemplate(
            hostInfo,
            hostInfo.getHost(),
            hostInfo.getPort()));
    initializeTopology();

    if (this.isClusterTopologyAvailable) {
      // "The 'clusterInstanceHostPattern' configuration property is required when an IP address or custom domain is used
      // to connect to a cluster that provides topology information. If you would instead like to connect without failover
      // functionality, set the 'enableClusterAwareFailover' configuration property to false."
      this.logger.logError(Messages.getString("ClusterAwareConnectionProxy.6"));
      throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.6"));
    }
  }

  private void initFromConnectionString(HostInfo hostInfo)
      throws SQLException {
    String rdsInstanceHostPattern = getRdsInstanceHostPattern(hostInfo.getHost());
    if (rdsInstanceHostPattern == null) {
      this.logger.logError(Messages.getString("ClusterAwareConnectionProxy.20"));
      throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.20"));
    }

    setClusterId(hostInfo.getHost(), hostInfo.getPort());
    this.topologyService.setClusterInstanceTemplate(
        createClusterInstanceTemplate(
            hostInfo,
            rdsInstanceHostPattern,
            hostInfo.getPort()));
    initializeTopology();
  }

  private void initFromHostPatternSetting(HostInfo hostInfo)
      throws SQLException {
    ConnectionUrlParser.Pair<String, Integer> pair = getHostPortPairFromHostPatternSetting();

    final String instanceHostPattern = pair.left;
    int instanceHostPort =
        pair.right != HostInfo.NO_PORT ? pair.right : hostInfo.getPort();

    // Instance host info is similar to original main host except host and port which
    // come from the configuration property.
    setClusterId(instanceHostPattern, instanceHostPort);
    this.topologyService.setClusterInstanceTemplate(
        createClusterInstanceTemplate(hostInfo, instanceHostPattern, instanceHostPort));
    initializeTopology();
  }

  private void initProxy() throws SQLException {
    final HostInfo hostInfo = this.currentConnectionProvider.getCurrentHostInfo();
    final String hostname = hostInfo.getHost();

    if (!StringUtils.isNullOrEmpty(this.clusterInstanceHostPatternSetting)) {
      initFromHostPatternSetting(hostInfo);
    } else if (IpAddressUtils.isIPv4(hostname) || IpAddressUtils.isIPv6(hostname)) {
      initExpectingNoTopology(hostInfo);
    } else {
      identifyRdsType(hostname);
      if (!this.isRds) {
        initExpectingNoTopology(hostInfo);
      } else {
        initFromConnectionString(hostInfo);
      }
    }

    if (isRdsClusterDns(hostname)) {
      this.explicitlyReadOnly = isReaderClusterDns(hostname);
      this.logger.logTrace(
              Messages.getString(
                      "ClusterAwareConnectionProxy.10",
                      new Object[] {"explicitlyReadOnly", this.explicitlyReadOnly}));
    }
  }

  private void fetchTopology() throws SQLException {
    final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    List<HostInfo> topology = this.topologyService.getTopology(currentConnection, false);
    if (!Util.isNullOrEmpty(topology)) {
      this.hosts = topology;
    }

    this.isClusterTopologyAvailable = !Util.isNullOrEmpty(this.hosts);
    this.logger.logTrace(
        Messages.getString(
            "ClusterAwareConnectionProxy.10",
            new Object[] {"isClusterTopologyAvailable",
                this.isClusterTopologyAvailable}));
    this.isMultiWriterCluster = this.topologyService.isMultiWriterCluster();
    this.currentHostIndex =
            getHostIndex(topologyService.getHostByName(this.currentConnectionProvider.getCurrentConnection()));

    if (this.isFailoverEnabled()) {
      logTopology();
    }
  }

  private void createConnection(ConnectionUrl connectionUrl) throws SQLException {

    if (this.enableFailoverSetting) {
      // Connection isn't created - try to use cached topology to create it
      if (this.currentConnectionProvider.getCurrentConnection() == null) {
        final String host = connectionUrl.getMainHost().getHost();
        if (isRdsClusterDns(host)) {
          this.explicitlyReadOnly = isReaderClusterDns(host);
          this.logger.logTrace(
              Messages.getString(
                  "ClusterAwareConnectionProxy.10",
                  new Object[]{"explicitlyReadOnly", this.explicitlyReadOnly}));

          try {
            attemptConnectionUsingCachedTopology();
          } catch (SQLException e) {
            // do nothing - attempt to connect directly will be made below
          }
        }
      }
    }

    // Connection isn't created - let other plugins to create it
    if (this.currentConnectionProvider.getCurrentConnection() == null) {
      this.nextPlugin.openInitialConnection(connectionUrl);
    }

    // Connection isn't created - take it over and create it
    if (this.currentConnectionProvider.getCurrentConnection() == null) {
      final HostInfo mainHost = connectionUrl.getMainHost();
      JdbcConnection connection = this.connectionProvider.connect(mainHost);
      this.currentConnectionProvider.setCurrentConnection(connection, mainHost);
    }
  }

  private void invalidInvocationOnClosedConnection() throws SQLException {
    if (this.autoReconnect && !this.closedExplicitly) {
      this.currentHostIndex =
          NO_CONNECTION_INDEX; // Act as if this is the first connection but let it sync with the previous one.
      this.isClosed = false;
      this.closedReason = null;
      pickNewConnection();

      // "The active SQL connection has changed. Please re-configure session state if required."
      this.logger.logError(Messages.getString("ClusterAwareConnectionProxy.19"));
      throw new SQLException(
          Messages.getString("ClusterAwareConnectionProxy.19"),
          MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_CHANGED);
    } else {
      String reason = "No operations allowed after connection closed.";
      if (this.closedReason != null) {
        reason += (" " + this.closedReason);
      }

      throw SQLError.createSQLException(
          reason,
          MysqlErrorNumbers.SQL_STATE_CONNECTION_NOT_OPEN,
          null /* no access to an interceptor here... */);
    }
  }

  private boolean isDnsPatternValid(String pattern) {
    return pattern.contains("?");
  }

  private boolean isExplicitlyReadOnly() {
    return this.explicitlyReadOnly != null && this.explicitlyReadOnly;
  }

  private boolean isRdsClusterDns(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    String clusterKeyword = getClusterKeyword(matcher);
    return "cluster-".equalsIgnoreCase(clusterKeyword)
        || "cluster-ro-".equalsIgnoreCase(clusterKeyword);
  }

  private boolean isRdsCustomClusterDns(String host) {
    Matcher matcher = auroraCustomClusterPattern.matcher(host);
    return matcher.find();
  }

  private boolean isRdsDns(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    return matcher.find();
  }

  private boolean isRdsProxyDns(String host) {
    Matcher matcher = auroraProxyDnsPattern.matcher(host);
    return matcher.find();
  }

  private boolean isReaderClusterDns(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    return "cluster-ro-".equalsIgnoreCase(getClusterKeyword(matcher));
  }

  /**
   * Checks if the given host index points to the primary host.
   *
   * @param hostIndex The host index in the global hosts list.
   * @return true if so
   */
  private boolean isWriterHostIndex(int hostIndex) {
    return hostIndex == WRITER_CONNECTION_INDEX;
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
    this.logger.logTrace(
        Messages.getString(
            "ClusterAwareConnectionProxy.11",
            new Object[] {msg.toString()}));
  }

  private void performSpecialMethodHandlingIfRequired(Object[] args, String methodName)
      throws SQLException {
    if (METHOD_SET_AUTO_COMMIT.equals(methodName)) {
      this.explicitlyAutoCommit = (Boolean) args[0];
      this.inTransaction = !this.explicitlyAutoCommit;
    }

    if (METHOD_COMMIT.equals(methodName) || METHOD_ROLLBACK.equals(methodName)) {
      this.inTransaction = false;
    }

    if (METHOD_SET_READ_ONLY.equals(methodName)) {
      this.explicitlyReadOnly = (Boolean) args[0];
      this.logger.logTrace(
          Messages.getString(
              "ClusterAwareConnectionProxy.10",
              new Object[] {"explicitlyReadOnly", this.explicitlyReadOnly}));
      connectToWriterIfRequired(this.explicitlyReadOnly);
    }
  }

  private void processFailoverFailure(String message) throws SQLException {
    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerFailoverConnects(false);
    }

    this.logger.logError(message);
    throw new SQLException(
        message,
        MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE);
  }

  private void setClusterId(String host, int port) {
    if (!StringUtils.isNullOrEmpty(this.clusterIdSetting)) {
      this.topologyService.setClusterId(this.clusterIdSetting);
    } else if (this.isRdsProxy) {
      // Each proxy is associated with a single cluster so it's safe to use RDS Proxy Url as cluster identification
      this.topologyService.setClusterId(host + ":" + port);
    } else if (this.isRds) {
      // If it's a cluster endpoint, or a reader cluster endpoint, then let's use it as the cluster ID
      String clusterRdsHostUrl = getRdsClusterHostUrl(host);
      if (!StringUtils.isNullOrEmpty(clusterRdsHostUrl)) {
        this.topologyService.setClusterId(clusterRdsHostUrl + ":" + port);
      }
    }
  }

  private boolean shouldAttemptReaderConnection() {
    return isExplicitlyReadOnly() && clusterContainsReader();
  }

  private boolean shouldPerformWriterFailover() {
    return this.explicitlyReadOnly == null || !this.explicitlyReadOnly;
  }

  private boolean shouldReconnectToWriter(Boolean readOnly) {
    return readOnly != null && !readOnly && !isWriterHostIndex(this.currentHostIndex);
  }

  /**
   * Replaces the previous underlying connection by the connection given. State from previous
   * connection, if any, is synchronized with the new one.
   *
   * @param hostIndex The host index in the global hosts list that matches the given connection.
   * @param connection The connection instance to switch to.
   * @throws SQLException if an error occurs
   */
  private void switchCurrentConnectionTo(int hostIndex, JdbcConnection connection)
      throws SQLException {
    invalidateCurrentConnection();

    boolean readOnly;
    final JdbcConnection currentConnection =
        this.currentConnectionProvider.getCurrentConnection();
    if (isWriterHostIndex(hostIndex)) {
      readOnly = isExplicitlyReadOnly();
    } else if (this.explicitlyReadOnly != null) {
      readOnly = this.explicitlyReadOnly;
    } else if (currentConnection != null) {
      readOnly = currentConnection.isReadOnly();
    } else {
      readOnly = false;
    }
    syncSessionState(currentConnection, connection, readOnly);
    updateCurrentConnection(connection, hostIndex);
    this.inTransaction = false;
  }

  private void updateCurrentConnection(
      JdbcConnection connection,
      int hostIndex) {
    this.currentHostIndex = hostIndex;
    updateCurrentConnection(connection, this.hosts.get(this.currentHostIndex));
  }

  private void updateCurrentConnection(
      JdbcConnection connection,
      HostInfo hostInfo) {
    this.currentConnectionProvider.setCurrentConnection(
        connection,
        hostInfo);
  }

  private void updateHostIndex(List<HostInfo> latestTopology) throws SQLException {
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
      this.currentHostIndex = NO_CONNECTION_INDEX;
      pickNewConnection();
    } else {
      // found the same node at different position in the topology
      // adjust current index only; connection is still valid
      this.currentHostIndex = latestHostIndex;
    }
  }

  private void validateHostPatternSetting(String hostPattern) throws SQLException {
    if (!isDnsPatternValid(hostPattern)) {
      // "Invalid value for the 'clusterInstanceHostPattern' configuration setting - the host pattern must contain a '?'
      // character as a placeholder for the DB instance identifiers of the instances in the cluster"
      this.logger.logError(Messages.getString("ClusterAwareConnectionProxy.21"));
      throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.21"));
    }

    identifyRdsType(hostPattern);
    if (this.isRdsProxy) {
      // "An RDS Proxy url can't be used as the 'clusterInstanceHostPattern' configuration setting."
      this.logger.logError(Messages.getString("ClusterAwareConnectionProxy.7"));
      throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.7"));
    }

    if (isRdsCustomClusterDns(hostPattern)) {
      // "An RDS Custom Cluster endpoint can't be used as the 'clusterInstanceHostPattern' configuration setting."
      this.logger.logError(Messages.getString("ClusterAwareConnectionProxy.18"));
      throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.18"));
    }
  }
}
