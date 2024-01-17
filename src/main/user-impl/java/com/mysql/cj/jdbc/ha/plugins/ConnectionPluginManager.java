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
          Util.loadClasses(
                  IConnectionPluginFactory.class,
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
