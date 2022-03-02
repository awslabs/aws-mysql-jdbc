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
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.ha.plugins.failover.FailoverConnectionPluginFactory;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * This class creates and handles a chain of {@link IConnectionPlugin} for each connection.
 */
public class ConnectionPluginManager implements ITransactionContextHandler {

  /* THIS CLASS IS NOT MULTI-THREADING SAFE */
  /* IT'S EXPECTED TO HAVE ONE INSTANCE OF THIS MANAGER PER JDBC CONNECTION */

  protected static final String DEFAULT_PLUGIN_FACTORIES = String.format("%s,%s",
      FailoverConnectionPluginFactory.class.getName(),
      NodeMonitoringConnectionPluginFactory.class.getName());

  protected Log logger;
  protected PropertySet propertySet = null;
  protected IConnectionPlugin headPlugin = null;
  ICurrentConnectionProvider currentConnectionProvider;

  public ConnectionPluginManager(Log logger) {
    if (logger == null) {
      throw new IllegalArgumentException(NullArgumentMessage.getMessage("logger"));
    }

    this.logger = logger;
  }

  /**
   * Initialize a chain of {@link IConnectionPlugin} using their corresponding {@link IConnectionPluginFactory}.
   * If {@code PropertyKey.connectionPluginFactories} is provided by the user, initialize
   * the chain with the given connection plugins in the order they are specified. Otherwise,
   * initialize the {@link NodeMonitoringConnectionPlugin} instead.
   *
   * <p>The {@link DefaultConnectionPlugin} will always be initialized and attached as the
   * last connection plugin in the chain.
   *
   * @param currentConnectionProvider The connection the plugins are associated with.
   * @param propertySet The configuration of the connection.
   */
  public void init(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet) throws SQLException {
    this.currentConnectionProvider = currentConnectionProvider;
    this.propertySet = propertySet;

    String factoryClazzNames = propertySet
        .getStringProperty(PropertyKey.connectionPluginFactories)
        .getValue();

    if (StringUtils.isNullOrEmpty(factoryClazzNames)) {
      factoryClazzNames = DEFAULT_PLUGIN_FACTORIES;
    }

    this.headPlugin = new DefaultConnectionPluginFactory()
        .getInstance(
            this.currentConnectionProvider,
            this.propertySet,
            null,
            this.logger);

    if (!StringUtils.isNullOrEmpty(factoryClazzNames)) {
      IConnectionPluginFactory[] factories =
          Util.<IConnectionPluginFactory>loadClasses(
                  factoryClazzNames,
                  "MysqlIo.BadConnectionPluginFactory",
                  null)
              .toArray(new IConnectionPluginFactory[0]);

      // make a chain of analyzers with default one at the tail

      for (int i = factories.length - 1; i >= 0; i--) {
        this.headPlugin = factories[i]
            .getInstance(
                this.currentConnectionProvider,
                this.propertySet,
                this.headPlugin,
                this.logger);
      }
    }
  }

  /**
   * Execute a JDBC method with the connection plugin chain.
   *
   * @param methodInvokeOn The Java Class invoking the JDBC method.
   * @param methodName The name of the method being invoked.
   * @param executeSqlFunc A lambda executing the method.
   * @return the result from the execution.
   * @throws Exception if errors occurred during the execution.
   */
  public Object execute(
      Class<?> methodInvokeOn,
      String methodName,
      Callable<?> executeSqlFunc,
      Object[] args) throws Exception {
    return this.headPlugin.execute(methodInvokeOn, methodName, executeSqlFunc, args);
  }

  /**
   * Release all dangling resources held by the connection plugins associated with
   * a single connection.
   */
  public void releaseResources() {
    this.logger.logTrace("[ConnectionPluginManager.releaseResources]");
    this.headPlugin.releaseResources();
  }

  public void openInitialConnection(ConnectionUrl connectionUrl) throws SQLException {
    this.headPlugin.openInitialConnection(connectionUrl);
  }

  @Override
  public void transactionBegun() {
    synchronized (this.currentConnectionProvider.getCurrentConnection()) {
      this.headPlugin.transactionBegun();
    }
  }

  @Override
  public void transactionCompleted() {
    synchronized (this.currentConnectionProvider.getCurrentConnection()) {
      this.headPlugin.transactionCompleted();
    }
  }
}
