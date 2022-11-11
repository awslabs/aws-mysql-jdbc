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

package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.ConnectionProxy;
import com.mysql.cj.log.Log;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * Monitor the server while the connection is executing methods for more sophisticated
 * failure detection.
 */
public class NodeMonitoringConnectionPlugin implements IConnectionPlugin {

  private static final String RETRIEVE_HOST_PORT_SQL =
      "SELECT CONCAT(@@hostname, ':', @@port)";
  private static final List<String> METHODS_STARTING_WITH = Arrays.asList("get", "abort");
  private static final List<String> METHODS_EQUAL_TO = Arrays.asList("close", "next");

  protected IConnectionPlugin nextPlugin;
  protected Log logger;
  protected PropertySet propertySet;
  private IMonitorService monitorService;
  private final Supplier<IMonitorService> monitorServiceSupplier;
  private final Set<String> nodeKeys = new HashSet<>();
  private final ICurrentConnectionProvider currentConnectionProvider;
  private JdbcConnection connection;

  /**
   * Initialize the node monitoring plugin.
   *
   * @param currentConnectionProvider A provider allowing the plugin to retrieve the
   *                                  current active connection and its connection settings.
   * @param propertySet The property set used to initialize the active connection.
   * @param nextPlugin The next connection plugin in the chain.
   * @param logger An implementation of {@link Log}.
   */
  public NodeMonitoringConnectionPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger) {
    this(
        currentConnectionProvider,
        propertySet,
        nextPlugin,
        logger,
        () -> new DefaultMonitorService(logger));
  }

  NodeMonitoringConnectionPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger,
      Supplier<IMonitorService> monitorServiceSupplier) {
    assertArgumentIsNotNull(currentConnectionProvider, "currentConnectionProvider");
    assertArgumentIsNotNull(propertySet, "propertySet");
    assertArgumentIsNotNull(nextPlugin, "nextPlugin");
    assertArgumentIsNotNull(logger, "logger");

    this.currentConnectionProvider = currentConnectionProvider;
    this.connection = currentConnectionProvider.getCurrentConnection();
    this.propertySet = propertySet;
    this.logger = logger;
    this.nextPlugin = nextPlugin;
    this.monitorServiceSupplier = monitorServiceSupplier;

    if (this.connection != null) {
      generateNodeKeys(this.connection);
    }
  }

  /**
   * Executes the given SQL function with {@link Monitor} if connection monitoring is enabled.
   * Otherwise, executes the SQL function directly.
   *
   * @param methodInvokeOn Class of an object that method to monitor to be invoked on.
   * @param methodName     Name of the method to monitor.
   * @param executeSqlFunc {@link Callable} SQL function.
   * @param args Arguments used to execute the given method.
   * @return Results of the {@link Callable} SQL function.
   * @throws Exception if an error occurs.
   */
  @Override
  public Object execute(
      Class<?> methodInvokeOn,
      String methodName,
      Callable<?> executeSqlFunc, Object[] args) throws Exception {
    // update config settings since they may change
    final boolean isEnabled = this.propertySet
        .getBooleanProperty(PropertyKey.failureDetectionEnabled)
        .getValue();

    if (!isEnabled || !this.doesNeedMonitoring(methodInvokeOn, methodName)) {
      // do direct call
      return this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc, args);
    }
    // ... otherwise, use a separate thread to execute method

    final int failureDetectionTimeMillis = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionTime)
        .getValue();
    final int failureDetectionIntervalMillis = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionInterval)
        .getValue();
    final int failureDetectionCount = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionCount)
        .getValue();

    initMonitorService();

    Object result;
    MonitorConnectionContext monitorContext = null;

    try {
      if (this.logger.isTraceEnabled()) {
        this.logger.logTrace(String.format(
                "[NodeMonitoringConnectionPlugin.execute]: method=%s.%s, monitoring is activated",
                methodInvokeOn.getName(),
                methodName));
      }

      this.checkIfChanged(this.currentConnectionProvider.getCurrentConnection());

      monitorContext = this.monitorService.startMonitoring(
          this.connection, //abort current connection if needed
          this.nodeKeys,
          this.currentConnectionProvider.getCurrentHostInfo(),
          this.propertySet,
          failureDetectionTimeMillis,
          failureDetectionIntervalMillis,
          failureDetectionCount);

      result = this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc, args);

    } finally {
      if (monitorContext != null) {
        this.monitorService.stopMonitoring(monitorContext);

        final boolean isConnectionClosed;
        try {
          isConnectionClosed = this.currentConnectionProvider.getCurrentConnection().isClosed();
        } catch (final SQLException e) {
          throw new CJCommunicationsException("Node is unavailable.");
        }

        if (monitorContext.isNodeUnhealthy()) {
          if (!isConnectionClosed) {
            abortConnection();
            throw new CJCommunicationsException("Node is unavailable.");
          }
        }

        if (this.logger.isTraceEnabled()) {
          this.logger.logTrace(String.format(
                  "[NodeMonitoringConnectionPlugin.execute]: method=%s.%s, monitoring is deactivated",
                  methodInvokeOn.getName(),
                  methodName));
        }
      }
    }

    return result;
  }

  void abortConnection() {
    try {
      this.connection.abortInternal();
    } catch (SQLException sqlEx) {
      // ignore
    }
  }

  /**
   * Checks whether the JDBC method passed to this connection plugin requires monitoring.
   *
   * @param methodInvokeOn The class of the JDBC method.
   * @param methodName Name of the JDBC method.
   * @return true if the method requires monitoring; false otherwise.
   */
  protected boolean doesNeedMonitoring(Class<?> methodInvokeOn, String methodName) {
    // It's possible to use the following, or similar, expressions to verify method invocation class
    //
    // boolean isJdbcConnection = JdbcConnection.class.isAssignableFrom(methodInvokeOn) || ClusterAwareConnectionProxy.class.isAssignableFrom(methodInvokeOn);
    // boolean isJdbcStatement = Statement.class.isAssignableFrom(methodInvokeOn);
    // boolean isJdbcResultSet = ResultSet.class.isAssignableFrom(methodInvokeOn);

    for (final String method : METHODS_STARTING_WITH) {
      if (methodName.startsWith(method)) {
        return false;
      }
    }

    for (final String method : METHODS_EQUAL_TO) {
      if (method.equals(methodName)) {
        return false;
      }
    }

    // Monitor all the other methods
    return true;
  }

  private void initMonitorService() {
    if (this.monitorService == null) {
      this.monitorService = this.monitorServiceSupplier.get();
    }
  }

  @Override
  public void transactionBegun() {
    this.nextPlugin.transactionBegun();
  }

  @Override
  public void transactionCompleted() {
    this.nextPlugin.transactionCompleted();
  }

  @Override
  public void openInitialConnection(ConnectionUrl connectionUrl) throws SQLException {
    this.nextPlugin.openInitialConnection(connectionUrl);
  }

  /**
   * Call this plugin's monitor service to release all resources associated with this
   * plugin.
   */
  @Override
  public void releaseResources() {
    if (this.monitorService != null) {
      this.monitorService.releaseResources();
    }

    this.monitorService = null;
    this.nextPlugin.releaseResources();
  }

  private void assertArgumentIsNotNull(Object param, String paramName) {
    if (param == null) {
      throw new IllegalArgumentException(NullArgumentMessage.getMessage(paramName));
    }
  }

  /**
   * Check if the connection has changed.
   * If so, remove monitor's references to that node and
   * regenerate the set of node keys referencing the node that requires monitoring.
   *
   * @param newConnection The connection used by {@link ConnectionProxy}.
   */
  private void checkIfChanged(JdbcConnection newConnection) {
    final boolean isSameConnection = this.connection != null && this.connection.equals(newConnection);
    if (!isSameConnection) {
      if (!this.nodeKeys.isEmpty()) {
        this.monitorService.stopMonitoringForAllConnections(this.nodeKeys);
      }
      this.connection = newConnection;
      generateNodeKeys(this.connection);
    }
  }

  /**
   * Generate a set of node keys representing the node to monitor.
   *
   * @param connection the connection to a specific node.
   */
  private void generateNodeKeys(Connection connection) {
    this.nodeKeys.clear();

    final HostInfo hostInfo = this.currentConnectionProvider.getCurrentHostInfo();
    this.nodeKeys.add(
            String.format(
                    "%s:%d",
                    hostInfo.getHost(),
                    hostInfo.getPort()
            ));

    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(RETRIEVE_HOST_PORT_SQL)) {
        while (rs.next()) {
          this.nodeKeys.add(rs.getString(1));
        }
      }
    } catch (SQLException sqlException) {
      // log and ignore
      this.logger.logTrace(
          "[NodeMonitoringConnectionPlugin.initNodes]: Could not retrieve Host:Port from querying");
    }
  }
}
