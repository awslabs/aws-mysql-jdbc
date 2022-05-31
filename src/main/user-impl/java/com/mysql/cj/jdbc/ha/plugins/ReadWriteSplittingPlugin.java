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

package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class ReadWriteSplittingPlugin implements IConnectionPlugin {
  public static final int WRITER_INDEX = 0;
  public static final String METHOD_SET_READ_ONLY = "setReadOnly";

  private final ICurrentConnectionProvider currentConnectionProvider;
  private final IConnectionProvider connectionProvider;
  private final PropertySet propertySet;
  private final IConnectionPlugin nextPlugin;
  private final Log logger;

  private boolean inTransaction = false;
  private boolean readOnly = false;
  private List<HostInfo> hosts = new ArrayList<>();
  private JdbcConnection writerConnection;
  private JdbcConnection readerConnection;

  public ReadWriteSplittingPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger) {
    this(currentConnectionProvider, new BasicConnectionProvider(), propertySet, nextPlugin, logger);
  }

  ReadWriteSplittingPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      IConnectionProvider connectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger) {
    this.currentConnectionProvider = currentConnectionProvider;
    this.connectionProvider = connectionProvider;
    this.propertySet = propertySet;
    this.nextPlugin = nextPlugin;
    this.logger = logger;
  }

  @Override
  public Object executeOnConnectionBoundObject(Class<?> methodInvokeOn, String methodName, Callable<?> executeSqlFunc, Object[] args) throws Exception {
    return this.nextPlugin.executeOnConnectionBoundObject(methodInvokeOn, methodName, executeSqlFunc, args);
  }

  @Override
  public Object executeOnConnection(Method method, List<Object> args) throws Exception {
    if (METHOD_SET_READ_ONLY.equals(method.getName()) && args != null && args.size() > 0) {
      switchConnectionIfRequired((Boolean) args.get(0));
    }
    return this.nextPlugin.executeOnConnection(method, args);
  }

  void switchConnectionIfRequired(Boolean readOnly) throws SQLException {
    if (readOnly == null) return;
    final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    if (readOnly) {
      if (!this.inTransaction && (!isReaderConnection() || currentConnection.isClosed())) {
        try {
          switchToReaderConnection(currentConnection);
        } catch (SQLException e) {
          if (!isConnectionUsable(currentConnection)) {
            throw new SQLException(
                Messages.getString("ReadWriteSplittingPlugin.1"),
                MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e);
          }

          // Failed to switch to a reader; use current connection as a fallback
          changeReaderConnectionTo(currentConnection);
        }
      }
    } else {
      if (!isWriterConnection() || currentConnection.isClosed()) {
        if(this.inTransaction) {
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
    this.readOnly = readOnly;
  }

  private void changeReaderConnectionTo(JdbcConnection newConnection) throws SQLException {
    if (this.readerConnection != newConnection && this.readerConnection != null) {
      this.readerConnection.close();
    }
    this.readerConnection = newConnection;
  }

  @Override
  public void openInitialConnection(ConnectionUrl connectionUrl) throws SQLException {
    this.hosts.addAll(connectionUrl.getHostsList());
    this.nextPlugin.openInitialConnection(connectionUrl);
    this.writerConnection = this.currentConnectionProvider.getCurrentConnection();
  }

  @Override
  public void releaseResources() {
    JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    if (this.readerConnection != currentConnection) {
      try {
        this.readerConnection.close();
      } catch (SQLException e) {
        // ignore
      }
    }

    if (this.writerConnection != currentConnection) {
      try {
        this.writerConnection.close();
      } catch (SQLException e) {
        // ignore
      }
    }

    this.nextPlugin.releaseResources();
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

  boolean getReadOnly() { return this.readOnly; }

  private boolean isWriterConnection() {
    final JdbcConnection conn = this.currentConnectionProvider.getCurrentConnection();
    return conn != null && conn == this.writerConnection;
  }

  private boolean isReaderConnection() {
    final JdbcConnection conn = this.currentConnectionProvider.getCurrentConnection();
    return conn != null && conn == this.readerConnection;
  }

  private synchronized void switchToWriterConnection(JdbcConnection currentConnection) throws SQLException {
    if (!isConnectionUsable(this.writerConnection)) {
      initializeWriterConnection();
    }
    if (!isWriterConnection() && this.writerConnection != null) {
      syncSessionStateOnReadWriteSplit(currentConnection, this.writerConnection);
      final HostInfo writerHostInfo = this.hosts.get(WRITER_INDEX);
      this.currentConnectionProvider.setCurrentConnection(this.writerConnection, writerHostInfo);
    }
  }

  private void initializeWriterConnection() throws SQLException {
    if (this.hosts.size() == 0) {
      throw new SQLException(
          Messages.getString("ReadWriteSplittingPlugin.3"),
          MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE);
    }
    this.writerConnection = this.connectionProvider.connect(this.hosts.get(WRITER_INDEX));
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
  void syncSessionStateOnReadWriteSplit(JdbcConnection source, JdbcConnection target) {
    try {
      if (source == null || target == null) {
        return;
      }

      final RuntimeProperty<Boolean> sourceUseLocalSessionState = source.getPropertySet().getBooleanProperty(PropertyKey.useLocalSessionState);
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
      // Do nothing. Reader connections must continue to "work" after swapping between writers and readers.
    }
  }

  private synchronized void switchToReaderConnection(JdbcConnection currentConnection) throws SQLException {
    if (!isConnectionUsable(this.readerConnection)) {
      initializeReaderConnection(currentConnection);
    }

    if (isReaderConnection()) {
      return;
    }

    final HostInfo readerHostInfo = getReaderHostInfo();
    if (readerHostInfo == null) {
      if (!isConnectionUsable(currentConnection)) {
        throw new SQLException(
            Messages.getString("ReadWriteSplittingPlugin.5"),
            MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE);
      }

      // Reader connection host wasn't in our host list - stick with current connection
      changeReaderConnectionTo(currentConnection);
      return;
    }

    syncSessionStateOnReadWriteSplit(currentConnection, this.readerConnection);
    this.currentConnectionProvider.setCurrentConnection(this.readerConnection, readerHostInfo);
  }

  private void initializeReaderConnection(JdbcConnection currentConnection) throws SQLException {
    if (this.hosts.size() == 0) {
      if (!isConnectionUsable(currentConnection)) {
        throw new SQLException(
            Messages.getString("ReadWriteSplittingPlugin.4"),
            MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE);
      }

      // No host info available; stick with the current connection
      changeReaderConnectionTo(currentConnection);
    } else if (this.hosts.size() == 1) {
      if (!isConnectionUsable(this.writerConnection)) {
        this.writerConnection = this.connectionProvider.connect(this.hosts.get(WRITER_INDEX));
      }
      this.readerConnection = this.writerConnection;
    } else if (this.hosts.size() == 2) {
      this.readerConnection = this.connectionProvider.connect(this.hosts.get(1));
    } else {
      int maxReaderIndex = this.hosts.size() - 1;
      int minReaderIndex = 1;
      int randomReaderIndex = (int) (Math.random() * ((maxReaderIndex - minReaderIndex) + 1)) + minReaderIndex;
      this.readerConnection = this.connectionProvider.connect(this.hosts.get(randomReaderIndex));
    }
  }

  private HostInfo getReaderHostInfo() {
    final String connAddress = this.readerConnection.getHostPortPair();
    for (HostInfo host : this.hosts) {
      if (host.getHostPortPair().equals(connAddress)) {
        return host;
      }
    }
    return null;
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
