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
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * This connection plugin will always be the last plugin in the connection plugin chain,
 * and will invoke the JDBC method passed down the chain.
 */
public class DefaultConnectionPlugin implements IConnectionPlugin {

  protected Log logger;
  protected IConnectionProvider connectionProvider;
  protected final ICurrentConnectionProvider currentConnectionProvider;

  public DefaultConnectionPlugin(ICurrentConnectionProvider currentConnectionProvider, Log logger) {
    this(currentConnectionProvider, logger, new BasicConnectionProvider());
  }

  public DefaultConnectionPlugin(ICurrentConnectionProvider currentConnectionProvider, Log logger, IConnectionProvider connectionProvider) {
    if (logger == null) {
      throw new IllegalArgumentException(NullArgumentMessage.getMessage("logger"));
    }
    if (connectionProvider == null) {
      throw new IllegalArgumentException(NullArgumentMessage.getMessage("connectionProvider"));
    }
    if (currentConnectionProvider == null) {
      throw new IllegalArgumentException(NullArgumentMessage.getMessage("currentConnectionProvider"));
    }

    this.logger = logger;
    this.connectionProvider = connectionProvider;
    this.currentConnectionProvider = currentConnectionProvider;
  }

  @Override
  public Object execute(
      Class<?> methodInvokeOn,
      String methodName,
      Callable<?> executeSqlFunc,
      Object[] args) throws Exception {
    try {
      return executeSqlFunc.call();
    } catch (InvocationTargetException invocationTargetException) {
      Throwable targetException = invocationTargetException.getTargetException();
      this.logger.logTrace(
          String.format("[DefaultConnectionPlugin.execute]: method=%s.%s, exception: ",
              methodInvokeOn.getName(), methodName), targetException);
      if (targetException instanceof Error) {
        throw (Error) targetException;
      }
      throw (Exception) targetException;
    } catch (Exception ex) {
      this.logger.logTrace(
          String.format("[DefaultConnectionPlugin.execute]: method=%s.%s, exception: ", methodInvokeOn.getName(), methodName), ex);
      throw ex;
    }
  }

  @Override
  public void openInitialConnection(ConnectionUrl connectionUrl) throws SQLException {
    if (this.currentConnectionProvider.getCurrentConnection() != null) {
      return;
    }

    final HostInfo mainHostInfo = connectionUrl.getMainHost();
    JdbcConnection connection = this.connectionProvider.connect(mainHostInfo);

    this.currentConnectionProvider.setCurrentConnection(connection, mainHostInfo);
  }

  @Override
  public void transactionBegun() {
    // do nothing
  }

  @Override
  public void transactionCompleted() {
    // do nothing
  }

  @Override
  public void releaseResources() {
    // do nothing
  }
}
