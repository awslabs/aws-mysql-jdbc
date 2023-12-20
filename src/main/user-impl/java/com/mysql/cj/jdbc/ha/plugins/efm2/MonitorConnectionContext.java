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

package com.mysql.cj.jdbc.ha.plugins.efm2;

import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Monitoring context for each connection. This contains each connection's criteria for whether a server should be
 * considered unhealthy. The context is shared between the main thread and the monitor thread.
 */
public class MonitorConnectionContext {
  private final AtomicReference<WeakReference<Connection>> connectionToAbortRef;
  private final AtomicBoolean nodeUnhealthy = new AtomicBoolean(false);

  /**
   * Constructor.
   *
   * @param connectionToAbort A reference to the connection associated with this context that will be aborted.
   */
  public MonitorConnectionContext(final Connection connectionToAbort) {
    this.connectionToAbortRef = new AtomicReference<>(new WeakReference<>(connectionToAbort));
  }

  public boolean isNodeUnhealthy() {
    return this.nodeUnhealthy.get();
  }

  void setNodeUnhealthy(final boolean nodeUnhealthy) {
    this.nodeUnhealthy.set(nodeUnhealthy);
  }

  public boolean shouldAbort() {
    return this.nodeUnhealthy.get() && this.connectionToAbortRef.get() != null;
  }

  public void setInactive() {
    this.connectionToAbortRef.set(null);
  }

  public Connection getConnection() {
    WeakReference<Connection> copy = this.connectionToAbortRef.get();
    return copy == null ? null : copy.get();
  }

  public boolean isActive() {
    WeakReference<Connection> copy = this.connectionToAbortRef.get();
    return copy != null && copy.get() != null;
  }
}
