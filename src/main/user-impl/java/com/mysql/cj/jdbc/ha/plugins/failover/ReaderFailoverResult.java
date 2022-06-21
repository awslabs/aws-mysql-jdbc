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

package com.mysql.cj.jdbc.ha.plugins.failover;

import com.mysql.cj.jdbc.JdbcConnection;

import java.sql.SQLException;

/** This class holds results of Reader Failover Process. */
public class ReaderFailoverResult {
  private final JdbcConnection newConnection;
  private final int newConnectionIndex;
  private final boolean isConnected;
  private final SQLException exception;

  /**
   * ReaderFailoverResult constructor.
   */
  public ReaderFailoverResult(
      JdbcConnection newConnection, int newConnectionIndex, boolean isConnected) {
    this(newConnection, newConnectionIndex, isConnected, null);
  }

  public ReaderFailoverResult(
      JdbcConnection newConnection,
      int newConnectionIndex,
      boolean isConnected,
      SQLException exception) {
    this.newConnection = newConnection;
    this.newConnectionIndex = newConnectionIndex;
    this.isConnected = isConnected;
    this.exception = exception;
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

  /**
   * Get the exception raised during failover.
   *
   * @return a {@link SQLException}.
   */
  public SQLException getException() {
    return exception;
  }
}
