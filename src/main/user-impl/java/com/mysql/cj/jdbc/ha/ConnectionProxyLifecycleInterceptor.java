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
