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

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.jdbc.JdbcConnection;

import java.sql.SQLException;
import java.util.List;

/** This class holds results of Writer Failover Process. */
public class WriterFailoverResult {
  private final boolean isConnected;
  private final boolean isNewHost;
  private final List<HostInfo> topology;
  private final JdbcConnection newConnection;
  private final String taskName;
  private final SQLException exception;

  /**
   * WriterFailoverResult constructor.
   * */
  public WriterFailoverResult(
      boolean isConnected,
      boolean isNewHost,
      List<HostInfo> topology,
      JdbcConnection newConnection,
      String taskName) {
    this(isConnected, isNewHost, topology, newConnection, taskName, null);
  }

   public WriterFailoverResult(
      boolean isConnected,
      boolean isNewHost,
      List<HostInfo> topology,
      JdbcConnection newConnection,
      String taskName,
      SQLException exception) {
    this.isConnected = isConnected;
    this.isNewHost = isNewHost;
    this.topology = topology;
    this.newConnection = newConnection;
    this.taskName = taskName;
    this.exception = exception;
  }

  /**
   * Checks if process result is successful and new connection to host is established.
   *
   * @return True, if process successfully connected to a host.
   */
  public boolean isConnected() {
    return this.isConnected;
  }

  /**
   * Checks if process successfully connected to a new host.
   *
   * @return True, if process successfully connected to a new host. False, if process successfully
   *     re-connected to the same host.
   */
  public boolean isNewHost() {
    return this.isNewHost;
  }

  /**
   * Get the latest topology.
   *
   * @return List of hosts that represent the latest topology. Returns null if no connection is
   *     established.
   */
  public List<HostInfo> getTopology() {
    return this.topology;
  }

  /**
   * Get the new connection established by the failover procedure if successful.
   *
   * @return {@link JdbcConnection} New connection to a host. Returns null if the failover procedure
   *     was unsuccessful.
   */
  public JdbcConnection getNewConnection() {
    return this.newConnection;
  }

  /**
   * Get the name of the writer failover task that created this result.
   *
   * @return The name of the writer failover task that created this result.
   */
  public String getTaskName() {
    return this.taskName;
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
