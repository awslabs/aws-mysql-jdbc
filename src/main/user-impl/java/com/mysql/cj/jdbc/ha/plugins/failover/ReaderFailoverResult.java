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

package com.mysql.cj.jdbc.ha.plugins.failover;

import com.mysql.cj.jdbc.JdbcConnection;

/** This class holds results of Reader Failover Process. */
public class ReaderFailoverResult {
  private final JdbcConnection newConnection;
  private final int newConnectionIndex;
  private final boolean isConnected;

  /**
   * ConnectionAttemptResult constructor.
   */
  public ReaderFailoverResult(
      JdbcConnection newConnection, int newConnectionIndex, boolean isConnected) {
    this.newConnection = newConnection;
    this.newConnectionIndex = newConnectionIndex;
    this.isConnected = isConnected;
  }

  /**
   * Get new connection to a host.
   *
   * @return {@link JdbcConnection} New connection to a host. Returns null if no connection is
   *     established.
   */
  public JdbcConnection getConnection() {
    return newConnection;
  }

  /**
   * Get index of newly connected host.
   *
   * @return Index of connected host in topology Returns -1 (NO_CONNECTION_INDEX) if no connection
   *     is established.
   */
  public int getConnectionIndex() {
    return newConnectionIndex;
  }

  /**
   * Checks if process result is successful and new connection to host is established.
   *
   * @return True, if process successfully connected to a host.
   */
  public boolean isConnected() {
    return isConnected;
  }
}
