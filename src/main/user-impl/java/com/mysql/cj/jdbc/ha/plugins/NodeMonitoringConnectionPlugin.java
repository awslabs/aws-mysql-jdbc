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

package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.ConnectionProxy;
import com.mysql.cj.log.Log;

import java.lang.reflect.Method;
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
   * Executes the given SQL function on the Connection-bound object with a {@link Monitor} if connection
   * monitoring is enabled. Otherwise, executes the SQL function directly.
   *
   * @param methodInvokeOn Class of the object that the method to monitor will be invoked on.
   * @param methodName     Name of the method to monitor.
   * @param executeSqlFunc {@link Callable} SQL function.
   * @param args Arguments used to execute the given method.
   * @return Results of the {@link Callable} SQL function.
   * @throws Exception if an error occurs.
   */
  @Override
  public Object executeOnConnectionBoundObject(
      Class<?> methodInvokeOn,
      String methodName,
      Callable<?> executeSqlFunc, Object[] args) throws Exception {
    // update config settings since they may change
    final boolean isEnabled = this.propertySet
        .getBooleanProperty(PropertyKey.failureDetectionEnabled)
        .getValue();

    if (!isEnabled || !this.doesNeedMonitoring(methodInvokeOn, methodName)) {
      // do direct call
      return this.nextPlugin.executeOnConnectionBoundObject(methodInvokeOn, methodName, executeSqlFunc, args);
    }
    // ... otherwise, use a separate thread to execute method

    Object result;
    MonitorConnectionContext monitorContext = null;

    try {
      monitorContext = startMonitoringService(methodName, methodInvokeOn);
      result = this.nextPlugin.executeOnConnectionBoundObject(methodInvokeOn, methodName, executeSqlFunc, args);
    } finally {
      stopMonitoringService(methodInvokeOn, methodName, monitorContext);
    }

    return result;
  }

  private void stopMonitoringService(Class<?> methodInvokeOn, String methodName, MonitorConnectionContext monitorContext) throws SQLException {
    if (monitorContext != null) {
      this.monitorService.stopMonitoring(monitorContext);
      synchronized (monitorContext) {
        if (monitorContext.isNodeUnhealthy() && !this.connection.isClosed()) {
          abortConnection();
          throw new CJCommunicationsException("Node is unavailable.");
        }
      }
    }
    this.logger.logTrace(String.format(
        "[NodeMonitoringConnectionPlugin.execute]: method=%s.%s, monitoring is deactivated",
        methodInvokeOn.getName(),
        methodName));
  }

  /**
   * Executes the given SQL function on the Connection with a {@link Monitor} if connection
   * monitoring is enabled. Otherwise, executes the SQL function directly.
   *
   * @param method The method to monitor.
   * @param args Arguments used to execute the given method.
   * @return Results of the SQL function invocation.
   * @throws Exception if an error occurs.
   */
  @Override
  public Object executeOnConnection(Method method, List<Object> args)
      throws Exception {
    String methodName = method.getName();
    Class methodInvokeOn = this.currentConnectionProvider.getCurrentConnection().getClass();
    // update config settings since they may change
    final boolean isEnabled = this.propertySet
        .getBooleanProperty(PropertyKey.failureDetectionEnabled)
        .getValue();

    if (!isEnabled || !this.doesNeedMonitoring(methodInvokeOn, methodName)) {
      // do direct call
      return this.nextPlugin.executeOnConnection(method, args);
    }

    // ... otherwise, use a separate thread to execute method
    Object result;
    MonitorConnectionContext monitorContext = null;

    try {
      monitorContext = startMonitoringService(methodName, methodInvokeOn);
      result = this.nextPlugin.executeOnConnection(method, args);
    } finally {
      stopMonitoringService(methodInvokeOn, methodName, monitorContext);
    }

    return result;
  }

  private MonitorConnectionContext startMonitoringService(String methodName, Class methodInvokeOn) {
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
    this.logger.logTrace(String.format(
        "[NodeMonitoringConnectionPlugin.execute]: method=%s.%s, monitoring is activated",
        methodInvokeOn.getName(),
        methodName));

    this.checkIfChanged(this.currentConnectionProvider.getCurrentConnection());

    MonitorConnectionContext monitorContext = this.monitorService.startMonitoring(
        this.connection, //abort current connection if needed
        this.nodeKeys,
        this.currentConnectionProvider.getCurrentHostInfo(),
        this.propertySet,
        failureDetectionTimeMillis,
        failureDetectionIntervalMillis,
        failureDetectionCount);
    return monitorContext;
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
