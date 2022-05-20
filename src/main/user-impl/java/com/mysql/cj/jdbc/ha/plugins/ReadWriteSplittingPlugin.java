package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.*;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;

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
  private List<HostInfo> hosts;
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
    this.hosts = new ArrayList<>(currentConnectionProvider.getOriginalUrl().getHostsList());
  }

  @Override
  public Object execute(Class<?> methodInvokeOn, String methodName, Callable<?> executeSqlFunc, Object[] args) throws Exception {
    if(METHOD_SET_READ_ONLY.equals(methodName)) {
      switchConnectionIfRequired((boolean) args[0]);
    }
    return this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc, args);
  }

  void switchConnectionIfRequired(boolean readOnly) throws SQLException {
    JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    if (readOnly) {
      if (!isReaderConnection() || currentConnection.isClosed()) {
        try {
          switchToReaderConnection(currentConnection);
        } catch (SQLException e) {
          if(currentConnection == null || currentConnection.isClosed()) {
            throw new SQLException(
                Messages.getString("ReadWriteSplittingPlugin.1"),
                MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, e);
          }

          // stick with the current connection
          if (this.readerConnection != currentConnection) {
            if (this.readerConnection != null) {
              this.readerConnection.close();
            }
            this.readerConnection = currentConnection;
          }
        }
      }
    } else {
      if (!isWriterConnection() || currentConnection.isClosed()) {
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

  @Override
  public void openInitialConnection(ConnectionUrl connectionUrl) throws SQLException {
    this.nextPlugin.openInitialConnection(connectionUrl);
    this.writerConnection = this.currentConnectionProvider.getCurrentConnection();
  }

  @Override
  public void releaseResources() {
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
    JdbcConnection conn = this.currentConnectionProvider.getCurrentConnection();
    return conn != null && conn == this.writerConnection;
  }

  private boolean isReaderConnection() {
    JdbcConnection conn = this.currentConnectionProvider.getCurrentConnection();
    return conn != null && conn == this.readerConnection;
  }

  private synchronized void switchToWriterConnection(JdbcConnection currentConnection) throws SQLException {
    if (this.writerConnection == null || this.writerConnection.isClosed()) {
      initializeWriterConnection();
    }
    if (!isWriterConnection() && this.writerConnection != null) {
      syncSessionState(currentConnection, this.writerConnection, false);
      HostInfo writerHostInfo = this.hosts.get(WRITER_INDEX);
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
   * Synchronizes session state between two connections, allowing to override the read-only status.
   *
   * @param source
   *            The connection where to get state from.
   * @param target
   *            The connection where to set state.
   * @param readOnly
   *            The new read-only status.
   */
  void syncSessionState(JdbcConnection source, JdbcConnection target, boolean readOnly) {
    try {
      if (target != null) {
        target.setReadOnly(readOnly);
      }

      if (source == null || target == null) {
        return;
      }

      RuntimeProperty<Boolean> sourceUseLocalSessionState = source.getPropertySet().getBooleanProperty(PropertyKey.useLocalSessionState);
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
    } catch (SQLException e) {
      // Do nothing. Reader connections must continue to "work" after swapping between writers and readers.
    }
  }

  private synchronized void switchToReaderConnection(JdbcConnection currentConnection) throws SQLException {
    if (this.readerConnection == null || this.readerConnection.isClosed()) {
      initializeReaderConnection(currentConnection);
    }

    if (isReaderConnection()) {
      return;
    }

    HostInfo readerHostInfo = getReaderHostInfo();
    if (readerHostInfo == null) {
      if (currentConnection == null || currentConnection.isClosed()) {
        throw new SQLException(
            Messages.getString("ReadWriteSplittingPlugin.5"),
            MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE);
      }

      // reader connection host wasn't in our host list - stick with current connection
      if (this.readerConnection != currentConnection) {
        if (this.readerConnection != null) {
          this.readerConnection.close();
        }
        this.readerConnection = currentConnection;
      }
      return;
    }

    syncSessionState(currentConnection, this.readerConnection, true);
    this.currentConnectionProvider.setCurrentConnection(this.readerConnection, readerHostInfo);
  }

  private void initializeReaderConnection(JdbcConnection currentConnection) throws SQLException {
    if (this.hosts.size() == 0) {
      if (currentConnection == null || currentConnection.isClosed()) {
        throw new SQLException(
            Messages.getString("ReadWriteSplittingPlugin.4"),
            MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE);
      }

      // stick with the current connection
      if (this.readerConnection != currentConnection) {
        if (this.readerConnection != null) {
          this.readerConnection.close();
        }
        this.readerConnection = currentConnection;
      }
    } else if (this.hosts.size() == 1) {
      if (this.writerConnection == null || this.writerConnection.isClosed()) {
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
    String connAddress = this.readerConnection.getHostPortPair();
    for (HostInfo host : this.hosts) {
      if (host.getHostPortPair().equals(connAddress)) {
        return host;
      }
    }
    return null;
  }

  JdbcConnection getWriterConnection() {
    return this.writerConnection;
  }

  JdbcConnection getReaderConnection() {
    return this.readerConnection;
  }
}
