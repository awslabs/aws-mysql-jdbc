/*
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Copyright (c) 2015, 2021, Oracle and/or its affiliates.
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
import com.mysql.cj.jdbc.ha.plugins.BasicConnectionProvider;
import com.mysql.cj.jdbc.ha.plugins.IConnectionPlugin;
import com.mysql.cj.jdbc.ha.plugins.IConnectionProvider;
import com.mysql.cj.jdbc.ha.plugins.ICurrentConnectionProvider;
import com.mysql.cj.jdbc.ha.util.ConnectionUtils;
import com.mysql.cj.jdbc.ha.util.RdsUtils;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.IpAddressUtils;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;
import java.io.EOFException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
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
  private static final String METHOD_GET_AUTO_COMMIT = "getAutoCommit";
  private static final String METHOD_GET_CATALOG = "getCatalog";
  private static final String METHOD_GET_SCHEMA = "getSchema";
  private static final String METHOD_GET_DATABASE = "getDatabase";
  static final String METHOD_ABORT = "abort";
  static final String METHOD_CLOSE = "close";
  static final String METHOD_IS_CLOSED = "isClosed";

  private final static Set<String> METHODS_REQUIRE_UPDATED_TOPOLOGY = ConcurrentHashMap.newKeySet();

  static {
    METHODS_REQUIRE_UPDATED_TOPOLOGY.addAll(Arrays.asList(
        METHOD_COMMIT,
        "connect",
        "isValid",
        "rollback",
        "setAutoCommit",
        "setReadOnly",
        "execute",
        "executeBatch",
        "executeLargeBatch",
        "executeLargeUpdate",
        "executeQuery",
        "executeUpdate",
        "executeWithFlags",
        "getParameterMetaData"
    ));
  }

  private static final String METHOD_GET_TRANSACTION_ISOLATION =
      "getTransactionIsolation";
  private static final String METHOD_GET_SESSION_MAX_ROWS = "getSessionMaxRows";
  protected final IConnectionProvider connectionProvider;
  protected final IClusterAwareMetricsContainer metricsContainer;
  private final ICurrentConnectionProvider currentConnectionProvider;
  private final PropertySet propertySet;
  private final IConnectionPlugin nextPlugin;
  private final Log logger;
  protected IWriterFailoverHandler writerFailoverHandler = null;
  protected IReaderFailoverHandler readerFailoverHandler = null;
  // writer host is always stored at index 0
  protected int currentHostIndex = NO_CONNECTION_INDEX;
  protected Map<String, String> initialConnectionProps;
  protected Boolean explicitlyReadOnly = null;
  protected boolean inTransaction = false;
  protected boolean explicitlyAutoCommit = true;
  protected boolean isClusterTopologyAvailable = false;
  protected boolean isRdsProxy = false;
  protected boolean isRds = false;
  protected ITopologyService topologyService;
  protected List<HostInfo> hosts = new ArrayList<>();

  // Configuration settings
  protected boolean enableFailoverSetting = true;
  protected boolean enableFailoverStrictReaderSetting;
  protected int clusterTopologyRefreshRateMsSetting;
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
  protected boolean keepSessionStateOnFailover;

  private long invokeStartTimeMs;
  private long failoverStartTimeMs;
  boolean isInitialConnectionToReader;

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
        () -> new AuroraTopologyService(logger),
        () -> new ClusterAwareMetricsContainer(currentConnectionProvider, propertySet));
  }

  FailoverConnectionPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger,
      IConnectionProvider connectionProvider,
      Supplier<ITopologyService> topologyServiceSupplier,
      Supplier<IClusterAwareMetricsContainer> metricsContainerSupplier) throws SQLException {
    this.currentConnectionProvider = currentConnectionProvider;
    this.propertySet = propertySet;
    this.nextPlugin = nextPlugin;
    this.logger = logger;
    this.connectionProvider = connectionProvider;
    this.metricsContainer = metricsContainerSupplier.get();

    initSettings();

    this.initialConnectionProps = new HashMap<>();
    this.initialConnectionProps =
        getInitialConnectionProps(this.propertySet, this.currentConnectionProvider.getCurrentHostInfo());

    if (!this.enableFailoverSetting) {
      return;
    }

    this.topologyService = topologyServiceSupplier.get();
    topologyService.setRefreshRate(this.clusterTopologyRefreshRateMsSetting);

    this.readerFailoverHandler =
        new ClusterAwareReaderFailoverHandler(
            this.topologyService,
            this.connectionProvider,
            this.initialConnectionProps,
            this.failoverTimeoutMsSetting,
            this.failoverReaderConnectTimeoutMsSetting,
            this.enableFailoverStrictReaderSetting,
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

    if (!this.enableFailoverSetting || canDirectExecute(methodName)) {
      return this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc, args);
    }

    if (this.isClosed && !allowedOnClosedConnection(methodName)) {
      invalidInvocationOnClosedConnection();
    }

    this.invokeStartTimeMs = System.currentTimeMillis();

    Object result = null;

    try {
      if (canUpdateTopology(methodName)) {
        updateTopologyIfNeeded(false);
      }
      result = this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc, args);
    } catch (IllegalStateException e) {
      dealWithIllegalStateException(e);
    } catch (Exception e) {
      this.dealWithOriginalException(e, null);
    }

    performSpecialMethodHandlingIfRequired(args, methodName);

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

    this.enableFailoverStrictReaderSetting =
        propertySet.getBooleanProperty(PropertyKey.enableFailoverStrictReader.getKeyName()).getValue();

    this.keepSessionStateOnFailover =
        propertySet.getBooleanProperty(PropertyKey.keepSessionStateOnFailover.getKeyName()).getValue();
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
      metricsContainer.registerInvalidInitialConnection(false);
      return;
    }

    metricsContainer.registerInvalidInitialConnection(true);

    try {
      connectTo(WRITER_CONNECTION_INDEX);
    } catch (SQLException e) {
      this.failoverStartTimeMs = System.currentTimeMillis();
      failover(WRITER_CONNECTION_INDEX);
    }
  }

  private boolean validWriterConnection() {
    return this.explicitlyReadOnly == null
            || this.explicitlyReadOnly
            || isWriterHostIndex(this.currentHostIndex);
  }

  private void updateTopologyFromCache() {
    List<HostInfo> cachedHosts = topologyService.getCachedTopology();
    if (Util.isNullOrEmpty(cachedHosts)) {
      metricsContainer.registerUseCachedTopology(false);
      return;
    }

    this.hosts = cachedHosts;

    metricsContainer.registerUseCachedTopology(true);
  }

  /**
   * Updating topology requires creating and executing a new statement.
   * This may cause interruptions during certain workflows. For instance,
   * the driver should not be updating topology while the connection is fetching a large streaming result set.
   *
   * @param methodName the method to check.
   * @return true if the driver should update topology before executing the method; false otherwise.
   */
  private boolean canUpdateTopology(String methodName) {
    return METHODS_REQUIRE_UPDATED_TOPOLOGY.contains(methodName);
  }

  protected void initializeTopology() throws SQLException {
    if (this.currentConnectionProvider.getCurrentConnection() == null) {
      return;
    }

    fetchTopology();
    if (this.isFailoverEnabled()) {
      validateConnection();

      if (this.currentHostIndex != NO_CONNECTION_INDEX
          && this.currentHostIndex != WRITER_CONNECTION_INDEX
          && !Util.isNullOrEmpty(this.hosts)) {
        HostInfo currentHost = this.hosts.get(this.currentHostIndex);
        topologyService.setLastUsedReaderHost(currentHost);
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
    HostInfo hostInfoWithInitialProps = ConnectionUtils.createHostWithProperties(
        baseHostInfo,
        this.initialConnectionProps);
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
    if (this.logger.isDebugEnabled()) {
      this.logger.logDebug(Messages.getString("ClusterAwareConnectionProxy.17"));
    }

    HostInfo failedHost = null;
    if (failedHostIdx != NO_CONNECTION_INDEX && !Util.isNullOrEmpty(this.hosts)) {
      failedHost = this.hosts.get(failedHostIdx);
    }
    ReaderFailoverResult result = readerFailoverHandler.failover(this.hosts, failedHost);

    long currentTimeMs = System.currentTimeMillis();
    metricsContainer.registerReaderFailoverProcedureTime(currentTimeMs - this.failoverStartTimeMs);
    this.failoverStartTimeMs = 0;

    if (result != null) {
      final SQLException exception = result.getException();
      if (exception != null) {
        throw exception;
      }
    }

    if (result == null || !result.isConnected()) {
      // "Unable to establish SQL connection to reader node"
      processFailoverFailure(Messages.getString("ClusterAwareConnectionProxy.4"));
      return;
    }

    metricsContainer.registerFailoverConnects(true);

    if (!keepSessionStateOnFailover) {
    updateCurrentConnection(
        result.getConnection(),
        result.getConnectionIndex());
    } else {
      switchCurrentConnectionTo(result.getConnectionIndex(), result.getConnection(), true);
    }

    updateTopologyIfNeeded(true);

    if (this.currentHostIndex != NO_CONNECTION_INDEX
        && this.currentHostIndex != WRITER_CONNECTION_INDEX
        && !Util.isNullOrEmpty(this.hosts)) {
      HostInfo currentHost = this.hosts.get(this.currentHostIndex);
      topologyService.setLastUsedReaderHost(currentHost);
      if (this.logger.isDebugEnabled()) {
        this.logger.logDebug(
                Messages.getString(
                        "ClusterAwareConnectionProxy.15",
                        new Object[]{currentHost}));
      }
    }
  }

  protected void failoverWriter() throws SQLException {
    if (this.logger.isDebugEnabled()) {
      this.logger.logDebug(Messages.getString("ClusterAwareConnectionProxy.16"));
    }
    WriterFailoverResult failoverResult = this.writerFailoverHandler.failover(this.hosts);

    long currentTimeMs = System.currentTimeMillis();
    metricsContainer.registerWriterFailoverProcedureTime(currentTimeMs - this.failoverStartTimeMs);
    this.failoverStartTimeMs = 0;

    if (failoverResult != null) {
      final SQLException exception = failoverResult.getException();
      if (exception != null) {
        throw exception;
      }
    }

    if (failoverResult == null || !failoverResult.isConnected()) {
      // "Unable to establish SQL connection to writer node"
      processFailoverFailure(Messages.getString("ClusterAwareConnectionProxy.2"));
      return;
    }

    if (!Util.isNullOrEmpty(failoverResult.getTopology())) {
      this.hosts = failoverResult.getTopology();
    }

    metricsContainer.registerFailoverConnects(true);

    // successfully re-connected to the same writer node
    if (!keepSessionStateOnFailover) {
      updateCurrentConnection(
          failoverResult.getNewConnection(),
          WRITER_CONNECTION_INDEX);
    } else {
      switchCurrentConnectionTo(WRITER_CONNECTION_INDEX, failoverResult.getNewConnection(), true);
    }

    if (this.logger.isDebugEnabled()) {
      this.logger.logDebug(
              Messages.getString(
                      "ClusterAwareConnectionProxy.15",
                      new Object[]{this.hosts.get(this.currentHostIndex)}));
    }
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
    pickNewConnection(null);
  }

  synchronized void pickNewConnection(List<HostInfo> latestTopology) throws SQLException {
    final List<HostInfo> currentTopology = this.hosts;
    if (latestTopology != null) {
      this.hosts = latestTopology;
    }

    if (this.isClosed && this.closedExplicitly) {
      if (this.logger.isDebugEnabled()) {
        this.logger.logDebug(Messages.getString("ClusterAwareConnectionProxy.1"));
      }

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
      if (this.currentHostIndex != NO_CONNECTION_INDEX
          && this.currentHostIndex != WRITER_CONNECTION_INDEX) {
        topologyService.setLastUsedReaderHost(currentTopology.get(this.currentHostIndex));
      }
    } catch (SQLException e) {
      failover(WRITER_CONNECTION_INDEX);
    }
  }

  protected boolean shouldExceptionTriggerConnectionSwitch(Throwable t) {

    if (!this.enableFailoverSetting
        || this.isRdsProxy
        || !this.isClusterTopologyAvailable) {
      if (this.logger.isDebugEnabled()) {
        this.logger.logDebug(Messages.getString("ClusterAwareConnectionProxy.13"));
      }
      return false;
    }

    if (t instanceof CommunicationsException || t instanceof CJCommunicationsException) {
      return true;
    }

    if (t instanceof SQLException) {
      return ConnectionUtils.isNetworkException((SQLException) t);
    }

    if (t instanceof CJException) {
      if (t.getCause() instanceof EOFException) { // Can not read response from server
        return true;
      }
      if (t.getCause() instanceof SSLException) { // Incomplete packets from server may cause SSL communication issues
        return true;
      }
      return ConnectionUtils.isNetworkException(((CJException) t).getSQLState());
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

  protected void updateTopologyIfNeeded(boolean forceUpdate)
      throws SQLException {
    final JdbcConnection connection = this.currentConnectionProvider.getCurrentConnection();
    if (
        !this.enableFailoverSetting
            || this.isRdsProxy
            || !this.isClusterTopologyAvailable
            || connection == null
            || connection.isClosed()
            || connection.isInGlobalTx()
            || connection.isInPreparedTx()) {
      return;
    }

    List<HostInfo> latestTopology = this.topologyService.getTopology(connection, forceUpdate);
    updateHostIndex(latestTopology);
    this.hosts = latestTopology;
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
      if (this.logger.isDebugEnabled()) {
        this.logger.logDebug(
                Messages.getString(
                        "ClusterAwareConnectionProxy.15",
                        new Object[]{this.hosts.get(hostIndex)}));
      }
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
      String host,
      int port) {
    return new HostInfo(
        null,
        host,
        port,
        null,
        null,
        null);
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
        long currentTimeMs = System.currentTimeMillis();
        metricsContainer.registerFailureDetectionTime(currentTimeMs - this.invokeStartTimeMs);
        this.invokeStartTimeMs = 0;
        this.failoverStartTimeMs = currentTimeMs;
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

  private void identifyRdsType(String host) {
    this.isRds = RdsUtils.isRdsDns(host);
    if (this.logger.isTraceEnabled()) {
      this.logger.logTrace(
              Messages.getString(
                      "ClusterAwareConnectionProxy.10",
                      new Object[]{"isRds", this.isRds}));
    }

    this.isRdsProxy = RdsUtils.isRdsProxyDns(host);
    if (this.logger.isTraceEnabled()) {
      this.logger.logTrace(
              Messages.getString(
                      "ClusterAwareConnectionProxy.10",
                      new Object[]{"isRdsProxy", this.isRdsProxy}));
    }
  }

  private void initExpectingNoTopology(HostInfo hostInfo)
      throws SQLException {
    setClusterId(hostInfo.getHost(), hostInfo.getPort());
    this.topologyService.setClusterInstanceTemplate(
        createClusterInstanceTemplate(
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
    String rdsInstanceHostPattern = RdsUtils.getRdsInstanceHostPattern(hostInfo.getHost());
    if (rdsInstanceHostPattern == null) {
      this.logger.logError(Messages.getString("ClusterAwareConnectionProxy.20"));
      throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.20"));
    }

    setClusterId(hostInfo.getHost(), hostInfo.getPort());
    this.topologyService.setClusterInstanceTemplate(
        createClusterInstanceTemplate(
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
        createClusterInstanceTemplate(instanceHostPattern, instanceHostPort));
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

    this.isInitialConnectionToReader = this.currentHostIndex != WRITER_CONNECTION_INDEX;

    if (RdsUtils.isRdsClusterDns(hostname)) {
      this.explicitlyReadOnly = RdsUtils.isReaderClusterDns(hostname);
      if (this.logger.isTraceEnabled()) {
        this.logger.logTrace(
                Messages.getString(
                        "ClusterAwareConnectionProxy.10",
                        new Object[]{"explicitlyReadOnly", this.explicitlyReadOnly}));
      }
    }
  }

  private void fetchTopology() throws SQLException {
    final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    List<HostInfo> topology = this.topologyService.getTopology(currentConnection, false);
    if (!Util.isNullOrEmpty(topology)) {
      this.hosts = topology;
    }

    this.isClusterTopologyAvailable = !Util.isNullOrEmpty(this.hosts);
    if (this.logger.isTraceEnabled()) {
      this.logger.logTrace(
              Messages.getString(
                      "ClusterAwareConnectionProxy.10",
                      new Object[]{"isClusterTopologyAvailable",
                              this.isClusterTopologyAvailable}));
    }
    this.currentHostIndex =
            getHostIndex(topologyService.getHostByName(this.currentConnectionProvider.getCurrentConnection()));

    if (this.isFailoverEnabled()) {
      logTopology();
    }
  }

  private void createConnection(ConnectionUrl connectionUrl) throws SQLException {
    // Update initial properties in case previous plugins have changed values
    this.initialConnectionProps.putAll(connectionUrl.getOriginalProperties());

    if (this.enableFailoverSetting) {
      // Connection isn't created - try to use cached topology to create it
      if (this.currentConnectionProvider.getCurrentConnection() == null) {
        final String host = connectionUrl.getMainHost().getHost();
        if (RdsUtils.isRdsClusterDns(host)) {
          this.explicitlyReadOnly = RdsUtils.isReaderClusterDns(host);
          if (this.logger.isTraceEnabled()) {
            this.logger.logTrace(
                    Messages.getString(
                            "ClusterAwareConnectionProxy.10",
                            new Object[]{"explicitlyReadOnly", this.explicitlyReadOnly}));
          }

          updateTopologyFromCache();
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

  private boolean isExplicitlyReadOnly() {
    return this.explicitlyReadOnly != null && this.explicitlyReadOnly;
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
    if (this.logger.isTraceEnabled()) {
      this.logger.logTrace(
              Messages.getString(
                      "ClusterAwareConnectionProxy.11",
                      new Object[]{msg.toString()}));
    }
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
      if (this.logger.isTraceEnabled()) {
        this.logger.logTrace(
                Messages.getString(
                        "ClusterAwareConnectionProxy.10",
                        new Object[] {"explicitlyReadOnly", this.explicitlyReadOnly}));
      }
      connectToWriterIfRequired(this.explicitlyReadOnly);
    }
  }

  private void processFailoverFailure(String message) throws SQLException {
    metricsContainer.registerFailoverConnects(false);

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
      String clusterRdsHostUrl = RdsUtils.getRdsClusterHostUrl(host);
      if (!StringUtils.isNullOrEmpty(clusterRdsHostUrl)) {
        this.topologyService.setClusterId(clusterRdsHostUrl + ":" + port);
      }
    }

    metricsContainer.setClusterId(this.topologyService.getClusterId());
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
   * @param failoverOccurred Whether the connection is switching due to failover.
   * @throws SQLException if an error occurs
   */
  private void switchCurrentConnectionTo(int hostIndex, JdbcConnection connection, boolean failoverOccurred)
      throws SQLException {
    boolean readOnly;
    final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();

    if (currentConnection != null && !currentConnection.isClosed()) {
      invalidateCurrentConnection();
    }

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

    if (!failoverOccurred) {
      this.inTransaction = false;
    }
  }

  private void switchCurrentConnectionTo(int hostIndex, JdbcConnection connection) throws SQLException {
    switchCurrentConnectionTo(hostIndex, connection, false);
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
    if (this.currentHostIndex == NO_CONNECTION_INDEX) {
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

    if (latestHostIndex == NO_CONNECTION_INDEX
        || (!isExplicitlyReadOnly() && latestHostIndex != WRITER_CONNECTION_INDEX && !isInitialConnectionToReader)) {
      // Current connection host isn't found in the latest topology or
      // the current connection is not read only but is connected to a reader instance.
      this.currentHostIndex = NO_CONNECTION_INDEX;
      pickNewConnection(latestTopology);
    } else {
      // found the same node at different position in the topology
      // adjust current index only; connection is still valid
      this.currentHostIndex = latestHostIndex;
    }
  }

  private void validateHostPatternSetting(String hostPattern) throws SQLException {
    if (!RdsUtils.isDnsPatternValid(hostPattern)) {
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

    if (RdsUtils.isRdsCustomClusterDns(hostPattern)) {
      // "An RDS Custom Cluster endpoint can't be used as the 'clusterInstanceHostPattern' configuration setting."
      this.logger.logError(Messages.getString("ClusterAwareConnectionProxy.18"));
      throw new SQLException(Messages.getString("ClusterAwareConnectionProxy.18"));
    }
  }

  private Map<String, String> getInitialConnectionProps(final PropertySet propertySet, HostInfo currentHost) {
    final Map<String, String> initialConnectionProperties = new HashMap<>();
    final Properties originalProperties = propertySet.exposeAsProperties();
    originalProperties.stringPropertyNames()
        .stream()
        .filter(x -> this.propertySet.getProperty(x).isExplicitlySet())
        .forEach(x -> initialConnectionProperties.put(x, originalProperties.getProperty(x)));

    initialConnectionProperties.putAll(currentHost.getHostProperties());
    initialConnectionProperties.put(PropertyKey.USER.getKeyName(), currentHost.getUser());
    initialConnectionProperties.put(PropertyKey.PASSWORD.getKeyName(), currentHost.getPassword());
    initialConnectionProperties.put(PropertyKey.connectTimeout.getKeyName(), String.valueOf(this.failoverConnectTimeoutMs));
    initialConnectionProperties.put(PropertyKey.socketTimeout.getKeyName(), String.valueOf(this.failoverSocketTimeoutMs));

    return initialConnectionProperties;
  }

  /**
   * Check whether the method provided can be executed directly without the failover functionality.
   *
   * @param methodName The name of the method that is being called
   * @return true if the method can be executed directly; false otherwise.
   */
  private boolean canDirectExecute(String methodName) {
    return (METHOD_CLOSE.equals(methodName)
        || METHOD_IS_CLOSED.equals(methodName)
        || METHOD_ABORT.equals(methodName));
  }
}
