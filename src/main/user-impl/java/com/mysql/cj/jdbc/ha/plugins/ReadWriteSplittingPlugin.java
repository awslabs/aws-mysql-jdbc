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

package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.plugins.failover.AuroraTopologyService;
import com.mysql.cj.jdbc.ha.plugins.failover.ITopologyService;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.mysql.cj.jdbc.ha.plugins.RdsUrlType.IP_ADDRESS;
import static com.mysql.cj.jdbc.ha.plugins.RdsUrlType.OTHER;

public class ReadWriteSplittingPlugin implements IConnectionPlugin {
  public static final int NO_CONNECTION_INDEX = -1;
  public static final int WRITER_INDEX = 0;

  private final Map<String, JdbcConnection> liveConnections = new HashMap<>();
  private final ICurrentConnectionProvider currentConnectionProvider;
  private final ITopologyService topologyService;
  private final IConnectionProvider connectionProvider;
  private final RdsHostUtils rdsHostUtils;
  private final PropertySet propertySet;
  private final IConnectionPlugin nextPlugin;
  private final ITransactionStateMachine stateMachine;
  private final Log logger;
  private final boolean loadBalanceReadOnlyTraffic;

  private boolean inTransaction = false;
  private boolean explicitlyReadOnly = false;
  private List<HostInfo> hosts = new ArrayList<>();
  private JdbcConnection writerConnection;
  private JdbcConnection readerConnection;
  private int currentHostIndex = NO_CONNECTION_INDEX;

  public ReadWriteSplittingPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger) {
    this(
        currentConnectionProvider,
        new AuroraTopologyService(logger),
        new BasicConnectionProvider(),
        new RdsHostUtils(logger),
        new ReadWriteSplittingStateMachine(),
        propertySet,
        nextPlugin,
        logger);
  }

  ReadWriteSplittingPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      ITopologyService topologyService,
      IConnectionProvider connectionProvider,
      RdsHostUtils rdsHostUtils,
      ITransactionStateMachine stateMachine,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger) {
    this.currentConnectionProvider = currentConnectionProvider;
    this.topologyService = topologyService;
    this.connectionProvider = connectionProvider;
    this.rdsHostUtils = rdsHostUtils;
    this.stateMachine = stateMachine;
    this.propertySet = propertySet;
    this.nextPlugin = nextPlugin;
    this.logger = logger;

    this.loadBalanceReadOnlyTraffic = propertySet.getBooleanProperty(PropertyKey.loadBalanceReadOnlyTraffic).getValue();
    this.logger.logTrace(Messages.getString("ReadWriteSplittingPlugin.logAttribute",
            new Object[]{ "loadBalanceReadOnlyTraffic", this.loadBalanceReadOnlyTraffic }));
  }

  @Override
  public Object execute(
      Class<?> methodInvokeOn,
      String methodName,
      Callable<?> executeSqlFunc,
      Object[] args) throws Exception {

    if ("setReadOnly".equals(methodName) && args != null && args.length > 0) {
      switchConnectionIfRequired((Boolean) args[0]);
    } else if (this.explicitlyReadOnly && this.loadBalanceReadOnlyTraffic && this.stateMachine.isTransactionBoundary()) {
      this.logger.logTrace(Messages.getString("ReadWriteSplittingPlugin.pickNewReader"));
      pickNewReaderConnection();
    }

    try {
      final Object result = this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc, args);
      final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
      this.stateMachine.getNextState(currentConnection, methodName, args);

      return result;
    } catch (SQLException e) {
      this.logger.logTrace(Messages.getString("ReadWriteSplittingPlugin.exceptionWhileExecuting"));
      if (isFailoverException(e)) {
        this.logger.logDebug(Messages.getString("ReadWriteSplittingPlugin.failoverDetected"));
        closeAllConnections();

        // Update connection/topology info in case the connection was changed
        final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
        final HostInfo currentHost = this.currentConnectionProvider.getCurrentHostInfo();
        this.logger.logTrace(
                Messages.getString("ReadWriteSplittingPlugin.hostAfterFailover", new Object[]{ currentHost.getHostPortPair() }));
        updateTopology(currentConnection, currentHost);
        updateInternalConnectionInfo(currentConnection, currentHost);
      }

      this.stateMachine.getNextState(e);
      throw e;
    }
  }

  private boolean isFailoverException(SQLException e) {
    return MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN.equals(e.getSQLState())
        || MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_CHANGED.equals(e.getSQLState());
  }

  private void updateTopology(JdbcConnection currentConnection, HostInfo currentHost) throws SQLException {
    try {
      final List<HostInfo> hosts = this.topologyService.getTopology(currentConnection, false);
      if (!Util.isNullOrEmpty(hosts)) {
        this.hosts = hosts;
        logTopology();
        return;
      }
    } catch (SQLException e) {
      // do nothing
    }

    final int oldHostIndex = this.currentHostIndex;
    final int newHostIndex = getHostIndex(currentConnection, currentHost);

    if (oldHostIndex == WRITER_INDEX && newHostIndex > WRITER_INDEX) {
      // The old host was a writer; we have failed over to a new writer.
      // Update the host list with the new writer at the beginning.
      Collections.swap(this.hosts, oldHostIndex, newHostIndex);
      this.logger.logTrace(
              Messages.getString(
                      "ReadWriteSplittingPlugin.failoverToNewWriter",
                      new Object[]{ this.hosts.get(newHostIndex), this.hosts.get(oldHostIndex).getHostPortPair() }));
      logTopology();
    }
  }

  void pickNewReaderConnection() {
    if (this.hosts.size() <= 2) {
      this.logger.logTrace(Messages.getString("ReadWriteSplittingPlugin.onlyOneReader"));
      return;
    }

    final ArrayDeque<Integer> readerIndexes = getRandomReaderIndexes();
    while (!readerIndexes.isEmpty()) {
      final int index = readerIndexes.poll();
      final HostInfo host = this.hosts.get(index);

      try {
        getNewReaderConnection(host);
        final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
        switchCurrentConnectionTo(currentConnection, this.readerConnection, index);
        this.logger.logTrace(
                Messages.getString("ReadWriteSplittingPlugin.switchedToNewReader", new Object[]{ host.getHostPortPair() }));
        return;
      } catch (SQLException e) {
        this.logger.logWarn(
                Messages.getString("ReadWriteSplittingPlugin.cannotConnectToReaderHost", new Object[]{ host.getHostPortPair() }));
      }
    }
    // If we get here we failed to connect to a new reader. In this case we will stick with the current one
  }

  private ArrayDeque<Integer> getRandomReaderIndexes() {
    final List<Integer> indexes = new ArrayList<>();
    for (int i = 1; i < this.hosts.size(); i++) {
      if (i != this.currentHostIndex) {
        indexes.add(i);
      }
    }
    Collections.shuffle(indexes);
    return new ArrayDeque<>(indexes);
  }

  private void getNewWriterConnection() throws SQLException {
    final JdbcConnection conn = getConnectionToHost(this.hosts.get(WRITER_INDEX));
    setWriterConnection(conn);
  }

  private void setWriterConnection(JdbcConnection conn) {
    this.writerConnection = conn;
    this.logger.logTrace(Messages.getString("ReadWriteSplittingPlugin.setWriterConnection",
            new Object[]{ this.hosts.get(WRITER_INDEX).getHostPortPair() }));
  }

  private void getNewReaderConnection(HostInfo host) throws SQLException {
    final JdbcConnection conn = getConnectionToHost(host);
    setReaderConnection(conn, host);
  }

  private void setReaderConnection(JdbcConnection conn, HostInfo host) {
    this.readerConnection = conn;
    this.logger.logTrace(
            Messages.getString("ReadWriteSplittingPlugin.setReaderConnection", new Object[]{ host.getHostPortPair() }));
  }

  private JdbcConnection getConnectionToHost(HostInfo host) throws SQLException {
    JdbcConnection conn = liveConnections.get(host.getHostPortPair());
    if (conn != null && !conn.isClosed()) {
      return conn;
    }

    conn = this.connectionProvider.connect(host);
    liveConnections.put(host.getHostPortPair(), conn);
    return conn;
  }

  void switchConnectionIfRequired(Boolean readOnly) throws SQLException {
    if (readOnly == null) {
      this.logger.logError(Messages.getString("ReadWriteSplittingPlugin.setReadOnlyNullArgument"));
      throw new SQLException(Messages.getString("ReadWriteSplittingPlugin.setReadOnlyNullArgument"));
    }

    if (!readOnly && isInReaderTransaction()) {
      this.logger.logError(Messages.getString("ReadWriteSplittingPlugin.setReadOnlyFalseInTransaction"));
      throw new SQLException(Messages.getString("ReadWriteSplittingPlugin.setReadOnlyFalseInTransaction"),
              MysqlErrorNumbers.SQL_STATE_ACTIVE_SQL_TRANSACTION);
    }

    this.explicitlyReadOnly = readOnly;
    final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    if (readOnly) {
      if (!isInWriterTransaction() && (!isCurrentConnectionReader() || currentConnection.isClosed())) {
        try {
          switchToReaderConnection(currentConnection);
        } catch (SQLException e) {
          if (!isConnectionUsable(currentConnection)) {
            this.logger.logError(Messages.getString("ReadWriteSplittingPlugin.errorSwitchingToReader"));
            throw new SQLException(
                Messages.getString("ReadWriteSplittingPlugin.errorSwitchingToReader"),
                MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e);
          }

          // Failed to switch to a reader; use current connection as a fallback
          final HostInfo currentHost = this.currentConnectionProvider.getCurrentHostInfo();
          this.logger.logWarn(
                  Messages.getString("ReadWriteSplittingPlugin.fallbackToWriter", new Object[]{ currentHost.getHostPortPair() }));
          setReaderConnection(currentConnection, currentHost);
        }
      }
    } else {
      if (!isCurrentConnectionWriter() || currentConnection.isClosed()) {
        try {
          switchToWriterConnection(currentConnection);
        } catch (SQLException e) {
          this.logger.logError(Messages.getString("ReadWriteSplittingPlugin.errorSwitchingToWriter"));
          throw new SQLException(
              Messages.getString("ReadWriteSplittingPlugin.errorSwitchingToWriter"),
              MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e);
        }
      }
    }
  }

  @Override
  public void openInitialConnection(ConnectionUrl connectionUrl) throws SQLException {
    this.nextPlugin.openInitialConnection(connectionUrl);
    final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    if (currentConnection == null) {
      this.logger.logError(Messages.getString("ReadWriteSplittingPlugin.currentConnectionNull"));
      throw new SQLException(Messages.getString("ReadWriteSplittingPlugin.currentConnectionNull"));
    }

    final HostInfo currentHost = this.currentConnectionProvider.getCurrentHostInfo();
    final String hostPattern = this.propertySet.getStringProperty(PropertyKey.clusterInstanceHostPattern).getValue();
    final RdsHost rdsHost;

    if (!StringUtils.isNullOrEmpty(hostPattern)) {
      this.logger.logTrace(Messages.getString("ReadWriteSplittingPlugin.logStringAttribute",
              new Object[]{ "clusterInstanceHostPattern", hostPattern }));
      rdsHost = this.rdsHostUtils.getRdsHostFromHostPattern(this.propertySet, currentHost, hostPattern);
    } else {
      rdsHost = this.rdsHostUtils.getRdsHost(this.propertySet, currentHost);
    }

    final RdsUrlType rdsUrlType = rdsHost.getUrlType();
    this.logger.logTrace(Messages.getString("ReadWriteSplittingPlugin.logAttribute",
            new Object[]{ "Connection URL type", rdsUrlType }));
    final String clusterId = rdsHost.getClusterId();
    if (clusterId != null) {
      this.topologyService.setClusterId(clusterId);
    }
    this.topologyService.setClusterInstanceTemplate(rdsHost.getClusterInstanceTemplate());

    final List<HostInfo> topology = this.topologyService.getTopology(currentConnection, false);
    if (Util.isNullOrEmpty(topology)) {
      this.hosts.addAll(connectionUrl.getHostsList());
      if (Util.isNullOrEmpty(this.hosts)) {
        this.logger.logError(Messages.getString("ReadWriteSplittingPlugin.noHostInformation"));
        throw new SQLException(Messages.getString("ReadWriteSplittingPlugin.noHostInformation"));
      }

      logTopology();
      updateInternalConnectionInfo(currentConnection, currentHost);
      return;
    }
    this.hosts = topology;
    logTopology();

    if (StringUtils.isNullOrEmpty(hostPattern) &&
        (IP_ADDRESS.equals(rdsUrlType) || OTHER.equals(rdsUrlType))) {
      this.logger.logError(Messages.getString("ReadWriteSplittingPlugin.hostPatternRequired"));
      throw new SQLException(Messages.getString("ReadWriteSplittingPlugin.hostPatternRequired"));
    }

    updateInternalConnectionInfo(currentConnection, topologyService.getHostByName(currentConnection));
  }

  private void updateInternalConnectionInfo(JdbcConnection currentConnection, HostInfo currentHost) throws SQLException {
    if (currentConnection == null || currentHost == null) {
      return;
    }

    this.currentHostIndex = getHostIndex(currentConnection, currentHost);
    if (this.currentHostIndex == 0) {
      setWriterConnection(currentConnection);
      if (this.hosts.size() == 1) {
        setReaderConnection(currentConnection, currentHost);
      }
    } else if (this.currentHostIndex != NO_CONNECTION_INDEX) {
      setReaderConnection(currentConnection, currentHost);
    }

    if (!currentConnection.isClosed()) {
      liveConnections.put(currentHost.getHostPortPair(), currentConnection);
    }
  }

  private int getHostIndex(JdbcConnection connection, HostInfo host) throws SQLException {
    return getHostIndex(connection, host.getHostPortPair());
  }

  private int getHostIndex(JdbcConnection connection, String hostPortPair) throws SQLException {
    int currentIndex = this.rdsHostUtils.getHostIndex(this.hosts, hostPortPair);
    if (currentIndex != NO_CONNECTION_INDEX) {
      return currentIndex;
    }

    currentIndex = this.rdsHostUtils.getHostIndex(this.hosts, this.topologyService.getHostByName(connection));
    if (currentIndex != NO_CONNECTION_INDEX) {
      return currentIndex;
    }

    this.logger.logError(Messages.getString("ReadWriteSplittingPlugin.noMatchForHost", new Object[]{ hostPortPair }));
    logTopology();
    throw new SQLException(Messages.getString("ReadWriteSplittingPlugin.noMatchForHost"));
  }

  @Override
  public void releaseResources() {
    closeAllConnections();
    this.nextPlugin.releaseResources();
  }

  private void closeAllConnections() {
    this.logger.logTrace(Messages.getString("ReadWriteSplittingPlugin.closingExtraConnections"));
    final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    closeInternalConnection(this.readerConnection, currentConnection);
    closeInternalConnection(this.writerConnection, currentConnection);

    if (this.readerConnection != currentConnection) {
      this.readerConnection = null;
    }

    if (this.writerConnection != currentConnection) {
      this.readerConnection = null;
    }

    for (JdbcConnection connection : liveConnections.values()) {
      closeInternalConnection(connection, currentConnection);
    }
    liveConnections.clear();
  }

  private void closeInternalConnection(JdbcConnection internalConnection, JdbcConnection currentConnection) {
    try {
      if (internalConnection != null && internalConnection != currentConnection && !internalConnection.isClosed()) {
        internalConnection.close();
      }
    } catch (SQLException e) {
      // ignore
    }
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

  private boolean isInWriterTransaction() {
    return this.inTransaction;
  }

  private boolean isInReaderTransaction() {
    // this.inTransaction is controlled by ConnectionProxyLifeCycleInterceptor. This method of detecting transaction
    // state does not properly detect all transaction scenarios, so we will check the current state instead if
    // setReadOnly(true) has been called.
    return this.stateMachine.isInReaderTransaction();
  }

  List<HostInfo> getHosts() {
    return this.hosts;
  }

  private boolean isCurrentConnectionWriter() {
    return this.currentHostIndex == WRITER_INDEX;
  }

  private boolean isCurrentConnectionReader() {
    // If there is only one host in the list we will consider it to be both a writer and a reader
    return (this.hosts.size() == 1 && this.currentHostIndex == WRITER_INDEX) || this.currentHostIndex > WRITER_INDEX;
  }

  private synchronized void switchToWriterConnection(JdbcConnection currentConnection) throws SQLException {
    if (isCurrentConnectionWriter() && isConnectionUsable(currentConnection)) {
      return;
    }

    if (!isConnectionUsable(this.writerConnection)) {
      getNewWriterConnection();
    }
    switchCurrentConnectionTo(currentConnection, this.writerConnection, WRITER_INDEX);
    this.logger.logDebug(Messages.getString("ReadWriteSplittingPlugin.switchedToWriter",
            new Object[]{ this.hosts.get(WRITER_INDEX).getHostPortPair() }));
  }

  private void switchCurrentConnectionTo(JdbcConnection oldConn, JdbcConnection newConn, int newConnIndex)
          throws SQLException {
    if (oldConn == newConn) {
      return;
    }

    syncSessionStateOnReadWriteSplit(oldConn, newConn);
    final HostInfo hostInfo = this.hosts.get(newConnIndex);
    this.currentConnectionProvider.setCurrentConnection(newConn, hostInfo);
    this.logger.logTrace(
            Messages.getString("ReadWriteSplittingPlugin.setCurrentConnection", new Object[]{ hostInfo.getHostPortPair() }));
    this.currentHostIndex = newConnIndex;
  }

  /**
   * This method should only be called if setReadOnly is currently being invoked through the connection plugin chain,
   * and we have decided to switch connections. It synchronizes session state from the source to the target, except for
   * the readOnly state, which will be set later when setReadOnly continues down the connection plugin chain.
   *
   * @param source
   *            The connection where to get state from.
   * @param target
   *            The connection where to set state.
   */
  void syncSessionStateOnReadWriteSplit(JdbcConnection source, JdbcConnection target) throws SQLException {
    try {
      if (source == null || target == null) {
        return;
      }

      final RuntimeProperty<Boolean> sourceUseLocalSessionState =
          source.getPropertySet().getBooleanProperty(PropertyKey.useLocalSessionState);
      final boolean prevUseLocalSessionState = sourceUseLocalSessionState.getValue();
      sourceUseLocalSessionState.setValue(true);

      target.setAutoCommit(source.getAutoCommit());
      final String db = source.getDatabase();
      if (db != null && !db.isEmpty()) {
        target.setDatabase(db);
      }
      target.setTransactionIsolation(source.getTransactionIsolation());
      target.setSessionMaxRows(source.getSessionMaxRows());

      sourceUseLocalSessionState.setValue(prevUseLocalSessionState);
    } catch (SQLException e) {
      this.logger.logError(Messages.getString("ReadWriteSplittingPlugin.errorSyncingSessionState"));
      throw new SQLException(
          Messages.getString("ReadWriteSplittingPlugin.errorSyncingSessionState"),
          MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e);
    }
  }

  private synchronized void switchToReaderConnection(JdbcConnection currentConnection) throws SQLException {
    if (isCurrentConnectionReader() && isConnectionUsable(currentConnection)) {
      return;
    }

    if (!isConnectionUsable(this.readerConnection)) {
      initializeReaderConnection(currentConnection);
      // The current connection may have changed; update it here in case it did.
      currentConnection = this.currentConnectionProvider.getCurrentConnection();
    }

    final int readerHostIndex = getHostIndex(this.readerConnection, this.readerConnection.getHostPortPair());
    switchCurrentConnectionTo(currentConnection, this.readerConnection, readerHostIndex);
    this.logger.logDebug(Messages.getString("ReadWriteSplittingPlugin.switchedToReader",
            new Object[]{ this.hosts.get(readerHostIndex).getHostPortPair() }));
  }

  private void initializeReaderConnection(JdbcConnection currentConnection) throws SQLException {
    if (this.hosts.size() == 1) {
      if (!isConnectionUsable(this.writerConnection)) {
        getNewWriterConnection();
        switchCurrentConnectionTo(currentConnection, this.writerConnection, WRITER_INDEX);
      }
      setReaderConnection(this.writerConnection, this.hosts.get(WRITER_INDEX));
    } else if (this.hosts.size() == 2) {
      getNewReaderConnection(this.hosts.get(1));
    } else {
      final int randomReaderIndex = getRandomReaderIndex();
      getNewReaderConnection(this.hosts.get(randomReaderIndex));
    }
  }

  private int getRandomReaderIndex() {
    if (this.hosts.size() <= 1) {
      return NO_CONNECTION_INDEX;
    }

    final int minReaderIndex = 1;
    final int maxReaderIndex = this.hosts.size() - 1;
    return (int) (Math.random() * ((maxReaderIndex - minReaderIndex) + 1)) + minReaderIndex;
  }

  private boolean isConnectionUsable(JdbcConnection connection) throws SQLException {
    return connection != null && !connection.isClosed();
  }

  JdbcConnection getWriterConnection() {
    return this.writerConnection;
  }

  JdbcConnection getReaderConnection() {
    return this.readerConnection;
  }

  private void logTopology() {
    if (!this.logger.isTraceEnabled()) {
      return;
    }

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
                    "ReadWriteSplittingPlugin.logTopology",
                    new Object[] { msg.toString() }));
  }
}
