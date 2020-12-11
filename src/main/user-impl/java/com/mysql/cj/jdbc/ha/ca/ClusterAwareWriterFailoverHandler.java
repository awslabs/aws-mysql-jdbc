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

import com.mysql.cj.Messages;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An implementation of WriterFailoverHandler.
 *
 * <p>Writer Failover Process goal is to re-establish connection to a writer. Connection to a writer
 * may be disrupted either by temporary network issue, or due to writer host unavailability during
 * cluster failover. This handler tries both approaches in parallel: 1) try to re-connect to the
 * same writer host, 2) try to update cluster topology and connect to a newly elected writer.
 */
public class ClusterAwareWriterFailoverHandler implements WriterFailoverHandler {

  static final int WRITER_CONNECTION_INDEX = 0;

  /** Null logger shared by all connections at startup. */
  protected static final Log NULL_LOGGER = new NullLogger(Log.LOGGER_INSTANCE_NAME);

  /** The logger we're going to use. */
  protected transient Log log = NULL_LOGGER;

  protected int maxFailoverTimeoutMs = 60000; // 60 sec
  protected int readTopologyIntervalMs = 5000; // 5 sec
  protected int reconnectWriterIntervalMs = 5000; // 5 sec
  protected TopologyService topologyService;
  protected ConnectionProvider connectionProvider;
  protected ReaderFailoverHandler readerFailoverHandler;

  /**
   * ClusterAwareWriterFailoverHandler constructor.
   * */
  public ClusterAwareWriterFailoverHandler(
      TopologyService topologyService,
      ConnectionProvider connectionProvider,
      ReaderFailoverHandler readerFailoverHandler,
      Log log) {
    this.topologyService = topologyService;
    this.connectionProvider = connectionProvider;
    this.readerFailoverHandler = readerFailoverHandler;

    if (log != null) {
      this.log = log;
    }
  }

  /**
   * ClusterAwareWriterFailoverHandler constructor.
   * */
  public ClusterAwareWriterFailoverHandler(
      TopologyService topologyService,
      ConnectionProvider connectionProvider,
      ReaderFailoverHandler readerFailoverHandler,
      int failoverTimeoutMs,
      int readTopologyIntervalMs,
      int reconnectWriterIntervalMs,
      Log log) {
    this(topologyService, connectionProvider, readerFailoverHandler, log);
    this.maxFailoverTimeoutMs = failoverTimeoutMs;
    this.readTopologyIntervalMs = readTopologyIntervalMs;
    this.reconnectWriterIntervalMs = reconnectWriterIntervalMs;
  }

  /**
   * Called to start Writer Failover Process.
   *
   * @param currentTopology Cluster current topology
   * @return {@link ResolvedHostInfo} The results of this process. May return null, which is
   *     considered an unsuccessful result.
   */
  @Override
  public ResolvedHostInfo failover(List<HostInfo> currentTopology) throws SQLException {

    ExecutorService executorService = null;
    try {
      executorService = Executors.newFixedThreadPool(2);

      CountDownLatch taskCompletedLatch = new CountDownLatch(1);

      HostInfo writerHost = currentTopology.get(WRITER_CONNECTION_INDEX);
      String writerHostPortPair = writerHost.getHostPortPair();
      this.topologyService.addDownHost(writerHostPortPair);
      ReconnectToWriterHandler taskA =
          new ReconnectToWriterHandler(
              taskCompletedLatch,
              writerHost,
              this.topologyService,
              this.connectionProvider,
              this.reconnectWriterIntervalMs,
              this.log);
      Future<?> futureA = executorService.submit(taskA);
      WaitForNewWriterHandler taskB =
          new WaitForNewWriterHandler(
              taskCompletedLatch,
              currentTopology,
              this.topologyService,
              writerHost,
              this.connectionProvider,
              this.readerFailoverHandler,
              this.readTopologyIntervalMs,
              this.log);
      Future<?> futureB = executorService.submit(taskB);

      executorService
          .shutdown(); // stop accepting new tasks but continue with tasks already in the the pool

      boolean isLatchZero;
      try {
        // wait for any task to complete
        isLatchZero = taskCompletedLatch.await(this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw createInterruptedException(e);
      }

      if (!isLatchZero) {
        // latch isn't 0 and that means that tasks are not yet finished;
        // time is out; cancel them
        futureA.cancel(true);
        futureB.cancel(true);
        return new ResolvedHostInfo(false, false, null, null);
      }

      // latch has passed and that means that either of tasks is almost finished
      // wait till a task is "officially" done
      while (!futureA.isDone() && !futureB.isDone()) {
        try {
          TimeUnit.MILLISECONDS.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw createInterruptedException(e);
        }
      }

      if (futureA.isDone()) {
        if (taskA.isConnected()) {
          // taskA is completed and connected to writer node
          // use this connection

          futureB.cancel(true);
          this.log.logDebug(
              Messages.getString(
                  "ClusterAwareWriterFailoverHandler.2", new Object[] {writerHostPortPair}));

          return new ResolvedHostInfo(
              true, false, taskA.getTopology(), taskA.getCurrentConnection());
        } else {
          // taskA is completed but hasn't connected
          // wait for taskB to complete

          try {
            futureB.get(this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
          } catch (TimeoutException | ExecutionException e) {
            // time is out; taskB is not connected either
            this.log.logDebug(Messages.getString("ClusterAwareWriterFailoverHandler.3"));
            return new ResolvedHostInfo(false, false, null, null);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw createInterruptedException(e);
          }

          // taskB is completed; check its results
          if (taskB.isConnected()) {
            HostInfo newWriterHost = taskB.getTopology().get(WRITER_CONNECTION_INDEX);
            String newWriterHostPair =
                newWriterHost == null ? "<null>" : newWriterHost.getHostPortPair();
            this.log.logDebug(
                Messages.getString(
                    "ClusterAwareWriterFailoverHandler.4", new Object[] {newWriterHostPair}));

            return new ResolvedHostInfo(
                true, true, taskB.getTopology(), taskB.getCurrentConnection());
          }
          this.log.logDebug(Messages.getString("ClusterAwareWriterFailoverHandler.3"));
          return new ResolvedHostInfo(false, false, null, null);
        }
      } else if (futureB.isDone()) {
        if (taskB.isConnected()) {
          // taskB is done and it's connected to writer node
          // use this connection
          futureA.cancel(true);

          HostInfo newWriterHost = taskB.getTopology().get(WRITER_CONNECTION_INDEX);
          String newWriterHostPair =
              newWriterHost == null ? "<null>" : newWriterHost.getHostPortPair();
          this.log.logDebug(
              Messages.getString(
                  "ClusterAwareWriterFailoverHandler.4", new Object[] {newWriterHostPair}));

          return new ResolvedHostInfo(
              true, true, taskB.getTopology(), taskB.getCurrentConnection());
        } else {
          // taskB is completed but it's failed to connect to writer node
          // wait for taskA completes

          try {
            futureA.get(this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
          } catch (TimeoutException | ExecutionException e) {
            // time is out; taskA is not connected either
            this.log.logDebug(Messages.getString("ClusterAwareWriterFailoverHandler.3"));
            return new ResolvedHostInfo(false, false, null, null);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw createInterruptedException(e);
          }

          // taskA is completed; check its results
          if (taskA.isConnected()) {
            this.log.logDebug(
                Messages.getString(
                    "ClusterAwareWriterFailoverHandler.2", new Object[] {writerHostPortPair}));
            return new ResolvedHostInfo(
                true, false, taskA.getTopology(), taskA.getCurrentConnection());
          }

          this.log.logDebug(Messages.getString("ClusterAwareWriterFailoverHandler.3"));
          return new ResolvedHostInfo(false, false, null, null);
        }
      }

      // shouldn't reach this point but anyway...
      futureA.cancel(true);
      futureB.cancel(true);

      // both taskA and taskB are unsuccessful
      this.log.logDebug(Messages.getString("ClusterAwareWriterFailoverHandler.3"));
      return new ResolvedHostInfo(false, false, null, null);
    } finally {
      if (executorService != null && !executorService.isTerminated()) {
        executorService.shutdownNow(); // terminate all remaining tasks
      }
    }
  }

  private SQLException createInterruptedException(InterruptedException e) {
    // "Thread was interrupted"
    return new SQLException(Messages.getString("ClusterAwareWriterFailoverHandler.1"), "70100", e);
  }

  /** Internal class responsible for re-connecting to the current writer (aka TaskA). */
  private static class ReconnectToWriterHandler implements Runnable {
    private List<HostInfo> latestTopology = null;
    private final HostInfo currentWriterHost;
    private final CountDownLatch taskCompletedLatch;
    private boolean isConnected = false;
    private JdbcConnection currentConnection = null;
    private final ConnectionProvider connectionProvider;
    private final TopologyService topologyService;
    private final int reconnectWriterIntervalMs;
    private final transient Log log;

    public ReconnectToWriterHandler(
        CountDownLatch taskCompletedLatch,
        HostInfo host,
        TopologyService topologyService,
        ConnectionProvider connectionProvider,
        int reconnectWriterIntervalMs,
        Log log) {
      this.taskCompletedLatch = taskCompletedLatch;
      this.currentWriterHost = host;
      this.topologyService = topologyService;
      this.connectionProvider = connectionProvider;
      this.reconnectWriterIntervalMs = reconnectWriterIntervalMs;
      this.log = log;
    }

    public boolean isConnected() {
      return this.isConnected;
    }

    public JdbcConnection getCurrentConnection() {
      return this.currentConnection;
    }

    public List<HostInfo> getTopology() {
      return this.latestTopology;
    }

    public void run() {
      try {
        this.log.logTrace(Messages.getString("ClusterAwareWriterFailoverHandler.6"));
        while (true) {
          try {
            this.log.logDebug(
                Messages.getString(
                    "ClusterAwareWriterFailoverHandler.5",
                    new Object[] {this.currentWriterHost.getHostPortPair()}));

            JdbcConnection conn = this.connectionProvider.connect(this.currentWriterHost);

            this.latestTopology = this.topologyService.getTopology(conn, true);
            if (this.latestTopology != null
                && !this.latestTopology.isEmpty()
                && isCurrentHostWriter()) {
              this.isConnected = true;
              this.currentConnection = conn;
              this.topologyService.removeDownHost(this.currentWriterHost.getHostPortPair());
              return;
            }
          } catch (SQLException exception) {
            // eat
          }

          TimeUnit.MILLISECONDS.sleep(this.reconnectWriterIntervalMs);
        }
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        this.isConnected = false;
        this.currentConnection = null;
        this.latestTopology = null;
      } catch (Exception ex) {
        this.log.logError(ex);
        throw ex;
      } finally {
        this.log.logTrace(Messages.getString("ClusterAwareWriterFailoverHandler.7"));
        // notify that this task is done
        this.taskCompletedLatch.countDown();
      }
    }

    private boolean isCurrentHostWriter() {
      String currentInstanceName =
          this.currentWriterHost.getHostProperties().get(TopologyServicePropertyKeys.INSTANCE_NAME);
      HostInfo latestWriter = this.latestTopology.get(WRITER_CONNECTION_INDEX);
      if (latestWriter == null) {
        return false;
      }
      String latestWriterInstanceName =
          latestWriter.getHostProperties().get(TopologyServicePropertyKeys.INSTANCE_NAME);
      return currentInstanceName.equals(latestWriterInstanceName);
    }
  }

  /**
   * Internal class responsible for getting latest cluster topology and connecting to a newly
   * elected writer (aka TaskB).
   */
  private static class WaitForNewWriterHandler implements Runnable {

    private final CountDownLatch taskCompletedLatch;
    private List<HostInfo> latestTopology;
    private JdbcConnection currentConnection = null;
    private boolean isConnected;
    private final int readTopologyIntervalMs;
    private final TopologyService topologyService;
    private final HostInfo originalWriterHost;
    private final ConnectionProvider connectionProvider;
    private final List<HostInfo> currentTopology;
    private final ReaderFailoverHandler readerFailoverHandler;
    private final transient Log log;

    public WaitForNewWriterHandler(
        CountDownLatch taskCompletedLatch,
        List<HostInfo> currentTopology,
        TopologyService topologyService,
        HostInfo currentHost,
        ConnectionProvider connectionProvider,
        ReaderFailoverHandler readerFailoverHandler,
        int readTopologyIntervalMs,
        Log log) {
      this.taskCompletedLatch = taskCompletedLatch;
      this.currentTopology = currentTopology;
      this.topologyService = topologyService;
      this.originalWriterHost = currentHost;
      this.connectionProvider = connectionProvider;
      this.readerFailoverHandler = readerFailoverHandler;
      this.readTopologyIntervalMs = readTopologyIntervalMs;
      this.log = log;
    }

    public boolean isConnected() {
      return this.isConnected;
    }

    public List<HostInfo> getTopology() {
      return this.latestTopology;
    }

    public JdbcConnection getCurrentConnection() {
      return this.currentConnection;
    }

    public void run() {
      this.log.logTrace(Messages.getString("ClusterAwareWriterFailoverHandler.8"));

      JdbcConnection conn = null;

      try {
        if (this.currentTopology == null) {
          // topology isn't available
          this.isConnected = false;
          return;
        }

        int connIndex = -1;
        HostInfo currentReaderHost = null;

        while (true) {
          try {
            ConnectionAttemptResult connResult =
                this.readerFailoverHandler.getReaderConnection(this.currentTopology);
            conn = connResult != null && connResult.isSuccess() ? connResult.getConnection() : null;
            connIndex =
                connResult != null && connResult.isSuccess() ? connResult.getConnectionIndex() : -1;
          } catch (SQLException e) {
            // eat
          }

          if (conn == null) {
            // can't connect to any reader
            this.log.logDebug(Messages.getString("ClusterAwareWriterFailoverHandler.11"));
          } else {
            currentReaderHost = this.currentTopology.get(connIndex);
            if(currentReaderHost != null) {
              this.log.logDebug(
                      Messages.getString(
                              "ClusterAwareWriterFailoverHandler.10",
                              new Object[]{connIndex, currentReaderHost.getHostPortPair()}));
              break;
            }
          }

          TimeUnit.MILLISECONDS.sleep(1);
        }

        // re-read topology and wait for a new writer
        while (true) {
          this.latestTopology = this.topologyService.getTopology(conn, true);

          if (this.latestTopology != null && !this.latestTopology.isEmpty()) {

            logTopology();
            HostInfo writerCandidate = this.latestTopology.get(WRITER_CONNECTION_INDEX);

            if (writerCandidate != null && !isSame(writerCandidate, this.originalWriterHost)) {
              // new writer is available and it's different from the previous writer
              try {
                this.log.logDebug(
                    Messages.getString(
                        "ClusterAwareWriterFailoverHandler.13",
                        new Object[] {writerCandidate.getHostPortPair()}));

                if(isSame(writerCandidate, currentReaderHost)) {
                  this.currentConnection = conn;
                } else {
                  // connected to a new writer
                  this.currentConnection = this.connectionProvider.connect(writerCandidate);
                }
                this.isConnected = true;

                this.topologyService.removeDownHost(writerCandidate.getHostPortPair());
                return;
              } catch (SQLException exception) {
                this.topologyService.addDownHost(writerCandidate.getHostPortPair());
              }
            }
          }

          TimeUnit.MILLISECONDS.sleep(this.readTopologyIntervalMs);
        }

      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        this.isConnected = false;
        this.currentConnection = null;
        this.latestTopology = null;
      } catch (Exception ex) {
        this.log.logError(ex);
        throw ex;
      } finally {
        if(conn != null && this.currentConnection != conn) {
          try {
            conn.close();
            conn = null;
          } catch(SQLException e) {
            // eat
          }
        }
        this.log.logTrace(Messages.getString("ClusterAwareWriterFailoverHandler.9"));
        this.taskCompletedLatch.countDown();
      }
    }

    private boolean isSame(HostInfo writerCandidate, HostInfo originalWriter) {
      if (writerCandidate == null) {
        return false;
      }
      return writerCandidate
          .getHostProperties()
          .get(TopologyServicePropertyKeys.INSTANCE_NAME)
          .equals(
              originalWriter.getHostProperties().get(TopologyServicePropertyKeys.INSTANCE_NAME));
    }

    private void logTopology() {
      StringBuilder msg = new StringBuilder();
      for (int i = 0; i < this.latestTopology.size(); i++) {
        HostInfo hostInfo = this.latestTopology.get(i);
        msg.append("\n   [")
            .append(i)
            .append("]: ")
            .append(hostInfo == null ? "<null>" : hostInfo.getHost());
      }
      this.log.logTrace(
          Messages.getString(
              "ClusterAwareWriterFailoverHandler.12", new Object[] {msg.toString()}));
    }
  }
}
