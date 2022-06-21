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

package com.mysql.cj.jdbc.ha;

import com.mysql.cj.MysqlConnection;
import com.mysql.cj.jdbc.ha.plugins.ITransactionContextHandler;
import com.mysql.cj.log.Log;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Properties;

/**
 * This basic implementation of ConnectionLifecycleInterceptor allows the
 * ClusterAwareConnectionProxy class to be aware of certain events occurring against the database
 * connection. The main goal is to track the transaction state of the connection.
 */
public class ConnectionProxyLifecycleInterceptor
    implements com.mysql.cj.jdbc.interceptors.ConnectionLifecycleInterceptor {

  private final ITransactionContextHandler handler;

  /**
   * Creates a new instance of ClusterAwareConnectionLifecycleInterceptor and initialize it with an
   * instance of connection proxy. This interceptor may change internal state of a connection proxy.
   *
   * @param proxy A connection proxy representing a logical connection. This proxy's internal state
   *     may be changed by this class.
   */
  public ConnectionProxyLifecycleInterceptor(ITransactionContextHandler handler) {
    this.handler = handler;
  }

  @Override
  public com.mysql.cj.jdbc.interceptors.ConnectionLifecycleInterceptor init(MysqlConnection conn, Properties props, Log log) {
    // explicitly do nothing
    return null;
  }

  @Override
  public void destroy() {
    // explicitly do nothing
  }

  @Override
  public void close() throws SQLException {
    // explicitly do nothing
  }

  @Override
  public boolean commit() {
    // explicitly do nothing
    return true;
  }

  @Override
  public boolean rollback() {
    // explicitly do nothing
    return true;
  }

  @Override
  public boolean rollback(Savepoint s) {
    // explicitly do nothing
    return true;
  }

  @Override
  public boolean setAutoCommit(boolean flag) {
    // explicitly do nothing
    return true;
  }

  @Override
  public boolean setDatabase(String db) {
    // explicitly do nothing
    return true;
  }

  /**
   * Called when the driver has been told by the server that a transaction is now in progress (when
   * one has not been currently in progress).
   *
   * @return true if transaction is in progress.
   */
  @Override
  public boolean transactionBegun() {
    handler.transactionBegun();
    return true;
  }

  /**
   * Called when the driver has been told by the server that a transaction has completed, and no
   * transaction is currently in progress.
   *
   * @return true if transaction is completed.
   */
  @Override
  public boolean transactionCompleted() {
    handler.transactionCompleted();
    return true;
  }
}
