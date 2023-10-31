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
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.util.ConnectionUtils;
import com.mysql.cj.jdbc.ha.util.RdsUtils;
import com.mysql.cj.util.StringUtils;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

/**
 * This connection plugin is used when connecting through reader cluster endpoints, where all new
 * connections should be directed to the same reader instance rather than a random reader instance
 * for each new connection.
 */
public class ReaderClusterConnectionPlugin implements IConnectionPlugin {
  private static final String GET_INSTANCE_QUERY = "SELECT @@aurora_server_id";
  protected IConnectionProvider connectionProvider;
  private final ICurrentConnectionProvider currentConnectionProvider;
  private final IConnectionPlugin nextPlugin;

  public ReaderClusterConnectionPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      IConnectionPlugin nextPlugin) {

    this(currentConnectionProvider,
         nextPlugin,
         new BasicConnectionProvider());
  }

  public ReaderClusterConnectionPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      IConnectionPlugin nextPlugin,
      IConnectionProvider connectionProvider) {
    if (connectionProvider == null) {
      throw new IllegalArgumentException(NullArgumentMessage.getMessage("connectionProvider"));
    }
    if (currentConnectionProvider == null) {
      throw new IllegalArgumentException(NullArgumentMessage.getMessage("currentConnectionProvider"));
    }

    this.currentConnectionProvider = currentConnectionProvider;
    this.nextPlugin = nextPlugin;
    this.connectionProvider = connectionProvider;
  }

  @Override
  public Object execute(
      Class<?> methodInvokeOn,
      String methodName,
      Callable<?> executeSqlFunc,
      Object[] args)
      throws Exception {
    return this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc, args);
  }

  @Override
  public void openInitialConnection(ConnectionUrl connectionUrl) throws SQLException {
    this.nextPlugin.openInitialConnection(connectionUrl);

    final JdbcConnection currentConnection = this.currentConnectionProvider.getCurrentConnection();
    if (currentConnection != null) {
      final HostInfo mainHostInfo = connectionUrl.getMainHost();
      final RdsUtils rdsUtils = new RdsUtils();

      if (rdsUtils.isReaderClusterDns(mainHostInfo.getHost())) {
        final String connectedHostName = getCurrentlyConnectedInstance(currentConnection);
        if (!StringUtils.isNullOrEmpty(connectedHostName)) {
          final String pattern =
              mainHostInfo.getHostProperties().get(PropertyKey.clusterInstanceHostPattern.getKeyName());
          final String instanceEndpoint = !StringUtils.isNullOrEmpty(pattern) ? pattern :
              rdsUtils.getRdsInstanceHostPattern(mainHostInfo.getHost());
          final HostInfo instanceHostInfo =
              ConnectionUtils.createInstanceHostWithProperties(
                  instanceEndpoint.replace("?", connectedHostName),
                  mainHostInfo);
          final JdbcConnection hostConnection = this.connectionProvider.connect(instanceHostInfo);
          this.currentConnectionProvider.setCurrentConnection(hostConnection, instanceHostInfo);
        }
      }
    }
  }

  @Override
  public void releaseResources() {
    this.nextPlugin.releaseResources();
  }

  @Override
  public void transactionBegun() {
    this.nextPlugin.transactionBegun();
  }

  @Override
  public void transactionCompleted() {
    this.nextPlugin.transactionCompleted();
  }

  private String getCurrentlyConnectedInstance(JdbcConnection connection) throws SQLException {
    try (final Statement statement = connection.createStatement()) {
      final ResultSet rs = statement.executeQuery(GET_INSTANCE_QUERY);
      if (rs.next()) {
        return rs.getString(1);
      }
      return null;
    }
  }
}
