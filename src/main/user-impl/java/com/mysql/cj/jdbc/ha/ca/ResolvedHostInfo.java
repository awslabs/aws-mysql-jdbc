/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
 * Modifications Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
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

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.jdbc.JdbcConnection;

import java.util.List;

/** This class holds results of Writer Failover Process. */
public class ResolvedHostInfo {
  private final boolean isConnected;
  private final boolean isNewHost;
  private final List<HostInfo> topology;
  private final JdbcConnection newConnection;

  /**
   * ResolvedHostInfo constructor.
   * */
  public ResolvedHostInfo(
      boolean isConnected,
      boolean isNewHost,
      List<HostInfo> topology,
      JdbcConnection newConnection) {
    this.isConnected = isConnected;
    this.isNewHost = isNewHost;
    this.topology = topology;
    this.newConnection = newConnection;
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
   * Get latest topology.
   *
   * @return List of hosts that represent the latest topology. Returns null if no connection is
   *     established.
   */
  public List<HostInfo> getTopology() {
    return this.topology;
  }

  /**
   * Get new connection to a host.
   *
   * @return {@link JdbcConnection} New connection to a host. Returns null if no connection is
   *     established.
   */
  public JdbcConnection getNewConnection() {
    return this.newConnection;
  }
}
