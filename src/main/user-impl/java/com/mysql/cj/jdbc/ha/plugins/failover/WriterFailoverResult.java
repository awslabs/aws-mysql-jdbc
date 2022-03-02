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

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.jdbc.JdbcConnection;

import java.util.List;

/** This class holds results of Writer Failover Process. */
public class WriterFailoverResult {
  private final boolean isConnected;
  private final boolean isNewHost;
  private final List<HostInfo> topology;
  private final JdbcConnection newConnection;
  private final String taskName;

  /**
   * ResolvedHostInfo constructor.
   * */
  public WriterFailoverResult(
      boolean isConnected,
      boolean isNewHost,
      List<HostInfo> topology,
      JdbcConnection newConnection,
      String taskName) {
    this.isConnected = isConnected;
    this.isNewHost = isNewHost;
    this.topology = topology;
    this.newConnection = newConnection;
    this.taskName = taskName;
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
   * Get the new connection established by the failover procedure if successful.
   *
   * @return {@link JdbcConnection} New connection to a host. Returns null if the failover pocedure
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
}
