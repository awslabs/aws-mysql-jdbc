/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of this program hereby grant you an additional permission to link the
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
