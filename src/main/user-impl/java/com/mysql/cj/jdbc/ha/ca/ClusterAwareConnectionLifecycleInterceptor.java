/*
 * AWS JDBC Driver for MySQL
 * Copyright 2020 Amazon.com Inc. or affiliates.
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

package com.mysql.cj.jdbc.ha.ca;

import com.mysql.cj.MysqlConnection;
import com.mysql.cj.jdbc.interceptors.ConnectionLifecycleInterceptor;
import com.mysql.cj.log.Log;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Properties;

/**
 * This basic implementation of ConnectionLifecycleInterceptor allows the
 * ClusterAwareConnectionProxy class to be aware of certain events occurring against the database
 * connection. The main goal is to track the transaction state of the connection.
 */
public class ClusterAwareConnectionLifecycleInterceptor implements ConnectionLifecycleInterceptor {

  private final ClusterAwareConnectionProxy proxy;

  /**
   * Creates a new instance of ClusterAwareConnectionLifecycleInterceptor and initialize it with an
   * instance of connection proxy. This interceptor may change internal state of a connection proxy.
   *
   * @param proxy A connection proxy representing a logical connection. This proxy's internal state
   *     may be changed by this class.
   */
  public ClusterAwareConnectionLifecycleInterceptor(ClusterAwareConnectionProxy proxy) {
    this.proxy = proxy;
  }

  @Override
  public ConnectionLifecycleInterceptor init(MysqlConnection conn, Properties props, Log log) {
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
    synchronized (this.proxy) {
      this.proxy.inTransaction = true;
      return true;
    }
  }

  /**
   * Called when the driver has been told by the server that a transaction has completed, and no
   * transaction is currently in progress.
   *
   * @return true if transaction is completed.
   */
  @Override
  public boolean transactionCompleted() {
    synchronized (this.proxy) {
      this.proxy.inTransaction = false;
      return true;
    }
  }
}
