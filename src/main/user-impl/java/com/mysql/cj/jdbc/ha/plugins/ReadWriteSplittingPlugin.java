/*
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Copyright (c) 2015, 2020, 2021 Oracle and/or its affiliates.
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
  public static final String METHOD_SET_READ_ONLY = "setReadOnly";

  private final Map<String, JdbcConnection> liveConnections = new HashMap<>();
  private final ICurrentConnectionProvider currentConnectionProvider;
  private final ITopologyService topologyService;
  private final IConnectionProvider connectionProvider;
  private final RdsHostUtils rdsHostUtils;
  private final PropertySet propertySet;
  private final IConnectionPlugin nextPlugin;
  private final Log logger;
  private final boolean loadBalanceReadOnlyTraffic;

  private boolean inTransaction = false;
  private boolean explicitlyReadOnly = false;
  private boolean isTransactionBoundary = false;
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
        propertySet,
        nextPlugin,
        logger);
  }

  ReadWriteSplittingPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      ITopologyService topologyService,
      IConnectionProvider connectionProvider,
      RdsHostUtils rdsHostUtils,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger) {
    this.currentConnectionProvider = currentConnectionProvider;
    this.topologyService = topologyService;
    this.connectionProvider = connectionProvider;
    this.rdsHostUtils = rdsHostUtils;
    this.propertySet = propertySet;
    this.nextPlugin = nextPlugin;
    this.logger = logger;

    this.loadBalanceReadOnlyTraffic = propertySet.getBooleanProperty(PropertyKey.loadBalanceReadOnlyTraffic).getValue();
  }

  @Override
  public Object execute(
      Class<?> methodInvokeOn,
      String methodName,
      Callable<?> executeSqlFunc,
      Object[] args) throws Exception {

    if (METHOD_SET_READ_ONLY.equals(methodName) && args != null && args.length > 0) {
      switchConnectionIfRequired((Boolean) args[0]);
//      this.isTransactionBoundary = false;
    } else if (this.explicitlyReadOnly && this.loadBalanceReadOnlyTraffic && isTransactionBoundary) {
      pickNewReaderConnection();
//      this.isTransactionBoundary = false;
    }

    try {
      final Object result = this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc, args);

      if (isTransactionClosed(methodName, args)) {
        this.inTransaction = false;
      }
      if (isTransactionStarted(methodName, args)) {
        this.inTransaction = true;
      }

      this.isTransactionBoundary = isTransactionBoundary(methodName, args);
      return result;
    } catch (SQLException e) {
      if (isFailoverException(e)) {
        closeAllConnections();
      }
      throw e;
    } finally {
        // Update connection/topology info in case the connection was changed
        final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
        final HostInfo currentHost = this.currentConnectionProvider.getCurrentHostInfo();
        updateTopology(currentConnection, currentHost);
        updateInternalConnectionInfo(currentConnection, currentHost);
    }
  }

  private boolean isTransactionStarted(String methodName, Object[] args) throws SQLException {
    if (!this.currentConnectionProvider.getCurrentConnection().getAutoCommit()) {
      return isExecuteMethod(methodName) && !isTransactionClosedViaExecute(methodName, args);
    }

    if ("execute".equals(methodName)
            || "executeUpdate".equals(methodName)
            || "executeLargeUpdate".equals(methodName)
            || "executeBatch".equals(methodName)) {
      if (args == null || args.length < 1) {
        return false;
      }

      String sql = (String) args[0];
      return "begin".equalsIgnoreCase(sql)
              || "start transaction".equalsIgnoreCase(sql)
              || "start transaction read only".equalsIgnoreCase(sql)
              || "start transaction read write".equalsIgnoreCase(sql);
    }
    return false;
  }

  private boolean isTransactionClosed(String methodName, Object[] args) throws SQLException {
    if (this.currentConnectionProvider.getCurrentConnection().getAutoCommit() && !this.inTransaction) {
      return true;
    }

    if ("commit".equals(methodName) || "rollback".equals(methodName)) {
      return true;
    }

    return isTransactionClosedViaExecute(methodName, args);
  }

  private boolean isTransactionClosedViaExecute(String methodName, Object[] args) {
    if ("execute".equals(methodName)
            || "executeUpdate".equals(methodName)
            || "executeLargeUpdate".equals(methodName)
            || "executeBatch".equals(methodName)) {
      if (args == null || args.length < 1) {
        return false;
      }

      String sql = (String) args[0];
      return "commit".equalsIgnoreCase(sql) || "rollback".equalsIgnoreCase(sql);
    }
    return false;
  }

  private boolean isTransactionBoundary(String methodName, Object[] args) throws SQLException {
    JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    if (currentConnection == null) {
      return false;
    }

    final boolean isAutoCommit = currentConnection.getAutoCommit();
    if (isAutoCommit) {
      return isExecuteMethod(methodName);
    }

    return isTransactionClosed(methodName, args);
  }

  private boolean isExecuteMethod(String methodName) {
    return "execute".equals(methodName)
            || "executeQuery".equals(methodName)
            || "executeUpdate".equals(methodName)
            || "executeLargeUpdate".equals(methodName)
            || "executeBatch".equals(methodName);
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
        return;
      }
    } catch (SQLException e) {
      // do nothing
    }

    final int oldHostIndex = this.currentHostIndex;
    final int newHostIndex = getHostIndex(
            currentConnection,
            currentHost,
            Messages.getString("ReadWriteSplittingPlugin.5"));

    if (oldHostIndex == WRITER_INDEX && newHostIndex > WRITER_INDEX) {
      // The old host was a writer; we have failed over to a new writer.
      // Update the host list with the new writer at the beginning.
      Collections.swap(this.hosts, oldHostIndex, newHostIndex);
    }
  }

  void pickNewReaderConnection() {
    if (this.hosts.size() <= 2) {
      return;
    }

    final ArrayDeque<Integer> readerIndexes = getRandomReaderIndexes();
    while (!readerIndexes.isEmpty()) {
      final int index = readerIndexes.poll();

      try {
        this.readerConnection = getConnectionToHost(index);
        final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
        switchCurrentConnectionTo(currentConnection, this.readerConnection, index);
        return;
      } catch (SQLException e) {
        // Do nothing
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

  private JdbcConnection getConnectionToHost(int hostIndex) throws SQLException {
    final HostInfo host = this.hosts.get(hostIndex);
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
      return;
    }
    this.explicitlyReadOnly = readOnly;

    final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    if (readOnly) {
      if (!this.inTransaction && (!isCurrentConnectionReader() || currentConnection.isClosed())) {
        try {
          switchToReaderConnection(currentConnection);
        } catch (SQLException e) {
          if (!isConnectionUsable(currentConnection)) {
            throw new SQLException(
                Messages.getString("ReadWriteSplittingPlugin.1"),
                MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e);
          }

          // Failed to switch to a reader; use current connection as a fallback
          this.readerConnection = currentConnection;
        }
      }
    } else {
      if (!isCurrentConnectionWriter() || currentConnection.isClosed()) {
        if (this.inTransaction) {
          throw new SQLException(
              Messages.getString("ReadWriteSplittingPlugin.6"),
              MysqlErrorNumbers.SQL_STATE_ACTIVE_SQL_TRANSACTION
          );
        }

        try {
          switchToWriterConnection(currentConnection);
        } catch (SQLException e) {
          throw new SQLException(
              Messages.getString("ReadWriteSplittingPlugin.2"),
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
      return;
    }

    final HostInfo currentHost = this.currentConnectionProvider.getCurrentHostInfo();
    final String hostPattern = this.propertySet.getStringProperty(PropertyKey.clusterInstanceHostPattern).getValue();
    final RdsHost rdsHost;

    if (!StringUtils.isNullOrEmpty(hostPattern)) {
      rdsHost = this.rdsHostUtils.getRdsHostFromHostPattern(this.propertySet, currentHost, hostPattern);
    } else {
      rdsHost = this.rdsHostUtils.getRdsHost(this.propertySet, currentHost);
    }

    final RdsUrlType rdsUrlType = rdsHost.getUrlType();
    final String clusterId = rdsHost.getClusterId();
    if (clusterId != null) {
      this.topologyService.setClusterId(clusterId);
    }
    this.topologyService.setClusterInstanceTemplate(rdsHost.getClusterInstanceTemplate());

    final List<HostInfo> topology = this.topologyService.getTopology(currentConnection, false);
    if (Util.isNullOrEmpty(topology)) {
      this.hosts.addAll(connectionUrl.getHostsList());
      if (Util.isNullOrEmpty(this.hosts)) {
        throw new SQLException(Messages.getString("ReadWriteSplittingPlugin.3"));
      }

      updateInternalConnectionInfo(currentConnection, currentHost);
      return;
    }
    this.hosts = topology;

    if (StringUtils.isNullOrEmpty(hostPattern) &&
        (IP_ADDRESS.equals(rdsUrlType) || OTHER.equals(rdsUrlType))) {
      throw new SQLException(Messages.getString("ReadWriteSplittingPlugin.8"));
    }

    updateInternalConnectionInfo(currentConnection, topologyService.getHostByName(currentConnection));
  }

  private void updateInternalConnectionInfo(JdbcConnection currentConnection, HostInfo currentHost) throws SQLException {
    if (currentConnection == null || currentHost == null) {
      return;
    }

    this.currentHostIndex = getHostIndex(currentConnection, currentHost, Messages.getString("ReadWriteSplittingPlugin.9"));
    if (this.currentHostIndex == 0) {
      this.writerConnection = currentConnection;
      if (this.hosts.size() == 1) {
        this.readerConnection = currentConnection;
      }
    } else if (this.currentHostIndex != NO_CONNECTION_INDEX) {
      this.readerConnection = currentConnection;
    }

    if (!currentConnection.isClosed()) {
      liveConnections.put(currentHost.getHostPortPair(), currentConnection);
    }
  }

  private int getHostIndex(JdbcConnection connection, HostInfo host, String errorMessage) throws SQLException {
    return getHostIndex(connection, host.getHostPortPair(), errorMessage);
  }

  private int getHostIndex(JdbcConnection connection, String hostPortPair, String errorMessage) throws SQLException {
    int currentIndex = this.rdsHostUtils.getHostIndex(this.hosts, hostPortPair);
    if (currentIndex != NO_CONNECTION_INDEX) {
      return currentIndex;
    }

    currentIndex = this.rdsHostUtils.getHostIndex(this.hosts, this.topologyService.getHostByName(connection));
    if (currentIndex != NO_CONNECTION_INDEX) {
      return currentIndex;
    }

    throw new SQLException(errorMessage);
  }

  @Override
  public void releaseResources() {
    closeAllConnections();

    this.nextPlugin.releaseResources();
  }

  private void closeAllConnections() {
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
      this.writerConnection = getConnectionToHost(WRITER_INDEX);
    }
    switchCurrentConnectionTo(currentConnection, this.writerConnection, WRITER_INDEX);
  }

  private void switchCurrentConnectionTo(JdbcConnection oldConn, JdbcConnection newConn, int newConnIndex) throws SQLException {
    if (oldConn == newConn) {
      return;
    }

    syncSessionStateOnReadWriteSplit(oldConn, newConn);
    final HostInfo hostInfo = this.hosts.get(newConnIndex);
    this.currentConnectionProvider.setCurrentConnection(newConn, hostInfo);
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
      throw new SQLException(
          Messages.getString("ReadWriteSplittingPlugin.7"),
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

    final int readerHostIndex = getHostIndex(
            this.readerConnection,
            this.readerConnection.getHostPortPair(),
            Messages.getString("ReadWriteSplittingPlugin.4"));
    switchCurrentConnectionTo(currentConnection, this.readerConnection, readerHostIndex);
  }

  private void initializeReaderConnection(JdbcConnection currentConnection) throws SQLException {
    if (this.hosts.size() == 1) {
      if (!isConnectionUsable(this.writerConnection)) {
        this.writerConnection = getConnectionToHost(WRITER_INDEX);
        switchCurrentConnectionTo(currentConnection, this.writerConnection, WRITER_INDEX);
      }
      this.readerConnection = this.writerConnection;
    } else if (this.hosts.size() == 2) {
      this.readerConnection = getConnectionToHost(1);
    } else {
      final int randomReaderIndex = getRandomReaderIndex();
      this.readerConnection = getConnectionToHost(randomReaderIndex);
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
}
