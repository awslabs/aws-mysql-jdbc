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

import com.mysql.cj.Messages;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.util.ConnectionUtils;
import com.mysql.cj.jdbc.ha.plugins.IConnectionProvider;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;
import com.mysql.cj.util.Util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of WriterFailoverHandler.
 *
 * <p>Writer Failover Process goal is to re-establish connection to a writer. Connection to a writer
 * may be disrupted either by temporary network issue, or due to writer host unavailability during
 * cluster failover. This handler tries both approaches in parallel: 1) try to re-connect to the
 * same writer host, 2) try to update cluster topology and connect to a newly elected writer.
 */
public class ClusterAwareWriterFailoverHandler implements IWriterFailoverHandler {

  static final int WRITER_CONNECTION_INDEX = 0;

  /** Null logger shared by all connections at startup. */
  protected static final Log NULL_LOGGER = new NullLogger(Log.LOGGER_INSTANCE_NAME);

  /** The logger we're going to use. */
  protected transient Log log = NULL_LOGGER;

  protected int maxFailoverTimeoutMs = 60000; // 60 sec
  protected int readTopologyIntervalMs = 5000; // 5 sec
  protected int reconnectWriterIntervalMs = 5000; // 5 sec
  protected Map<String, String> initialConnectionProps;
  protected ITopologyService topologyService;
  protected IConnectionProvider connectionProvider;
  protected IReaderFailoverHandler readerFailoverHandler;

  /**
   * ClusterAwareWriterFailoverHandler constructor.
   */
  public ClusterAwareWriterFailoverHandler(
      ITopologyService topologyService,
      IConnectionProvider connectionProvider,
      IReaderFailoverHandler readerFailoverHandler,
      Map<String, String> initialConnectionProps,
      Log log) {
    this.topologyService = topologyService;
    this.connectionProvider = connectionProvider;
    this.readerFailoverHandler = readerFailoverHandler;
    this.initialConnectionProps = initialConnectionProps;

    if (log != null) {
      this.log = log;
    }
  }

  /**
   * ClusterAwareWriterFailoverHandler constructor.
   */
  public ClusterAwareWriterFailoverHandler(
      ITopologyService topologyService,
      IConnectionProvider connectionProvider,
      IReaderFailoverHandler readerFailoverHandler,
      Map<String, String> initialConnectionProps,
      int failoverTimeoutMs,
      int readTopologyIntervalMs,
      int reconnectWriterIntervalMs,
      Log log) {
    this(
        topologyService,
        connectionProvider,
        readerFailoverHandler,
        initialConnectionProps,
        log);
    this.maxFailoverTimeoutMs = failoverTimeoutMs;
    this.readTopologyIntervalMs = readTopologyIntervalMs;
    this.reconnectWriterIntervalMs = reconnectWriterIntervalMs;
  }

  /**
   * Called to start Writer Failover Process.
   *
   * @param currentTopology Cluster current topology
   * @return {@link WriterFailoverResult} The results of this process.
   */
  @Override
  public WriterFailoverResult failover(List<HostInfo> currentTopology)
      throws SQLException {
    if (Util.isNullOrEmpty(currentTopology)) {
      this.log.logError(Messages.getString("ClusterAwareWriterFailoverHandler.7"));
      return new WriterFailoverResult(false, false, null, null, "None");
    }

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    CompletionService<WriterFailoverResult> completionService =
        new ExecutorCompletionService<>(executorService);
    submitTasks(currentTopology, executorService, completionService);

    try {
      long startTimeNano = System.nanoTime();
      WriterFailoverResult result = getNextResult(executorService, completionService, this.maxFailoverTimeoutMs);
      if (result.isConnected() || result.getException() != null) {
        return result;
      }

      long endTimeNano = System.nanoTime();
      int durationMs = (int) TimeUnit.NANOSECONDS.toMillis(endTimeNano - startTimeNano);
      int remainingTimeMs = this.maxFailoverTimeoutMs - durationMs;

      if (remainingTimeMs > 0) {
        result = getNextResult(executorService, completionService, remainingTimeMs);
        if (result.isConnected() || result.getException() != null) {
          return result;
        }
      }

      this.log.logDebug(Messages.getString("ClusterAwareWriterFailoverHandler.3"));
      return new WriterFailoverResult(false, false, null, null, "None");
    } finally {
      if (!executorService.isTerminated()) {
        executorService.shutdownNow(); // terminate all remaining tasks
      }
    }
  }

  private void submitTasks(
      List<HostInfo> currentTopology, ExecutorService executorService,
      CompletionService<WriterFailoverResult> completionService) {
    HostInfo writerHost = currentTopology.get(WRITER_CONNECTION_INDEX);
    this.topologyService.addToDownHostList(writerHost);
    completionService.submit(new ReconnectToWriterHandler(writerHost));
    completionService.submit(new WaitForNewWriterHandler(
        currentTopology,
        writerHost));
    executorService.shutdown();
  }

  private WriterFailoverResult getNextResult(
      ExecutorService executorService,
      CompletionService<WriterFailoverResult> completionService,
      int timeoutMs) throws SQLException {
    try {
      Future<WriterFailoverResult> firstCompleted = completionService.poll(
          timeoutMs, TimeUnit.MILLISECONDS);
      if (firstCompleted == null) {
        // The task was unsuccessful and we have timed out
        return new WriterFailoverResult(false, false, new ArrayList<>(), null, "None");
      }
      WriterFailoverResult result = firstCompleted.get();
      if (result.isConnected()) {
        executorService.shutdownNow();
        logTaskSuccess(result);
        return result;
      }

      if (result.getException() != null) {
        executorService.shutdownNow();
        return result;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw createInterruptedException(e);
    } catch (ExecutionException e) {
      // return failure below
    }
    return new WriterFailoverResult(false, false, new ArrayList<>(), null, "None");
  }

  private void logTaskSuccess(WriterFailoverResult result) {
    List<HostInfo> topology = result.getTopology();
    if (Util.isNullOrEmpty(topology)) {
      String taskName = result.getTaskName() == null ? "None" : result.getTaskName();
      this.log.logError(Messages.getString(
          "ClusterAwareWriterFailoverHandler.5",
          new Object[] {taskName}));
      return;
    }

    String newWriterHost = topology.get(WRITER_CONNECTION_INDEX).getHostPortPair();
    if (result.isNewHost()) {
      this.log.logDebug(Messages.getString(
          "ClusterAwareWriterFailoverHandler.4",
          new Object[] {newWriterHost}));
    } else {
      this.log.logDebug(Messages.getString(
          "ClusterAwareWriterFailoverHandler.2",
          new Object[] {newWriterHost}));
    }
  }

  private SQLException createInterruptedException(InterruptedException e) {
    // "Thread was interrupted"
    return new SQLException(
        Messages.getString("ClusterAwareWriterFailoverHandler.1"),
        "70100",
        e);
  }

  /** Internal class responsible for re-connecting to the current writer (aka TaskA). */
  private class ReconnectToWriterHandler implements Callable<WriterFailoverResult> {
    private final HostInfo originalWriterHost;

    public ReconnectToWriterHandler(HostInfo originalWriterHost) {
      this.originalWriterHost = originalWriterHost;
    }

    public WriterFailoverResult call() {
      log.logDebug(
          Messages.getString(
              "ClusterAwareWriterFailoverHandler.6",
              new Object[] {this.originalWriterHost.getHostPortPair()}));

      JdbcConnection conn = null;
      List<HostInfo> latestTopology = null;
      boolean success = false;

      try {
        while (latestTopology == null || Util.isNullOrEmpty(latestTopology)) {
          try {

            if (conn != null && !conn.isClosed()) {
              conn.close();
            }

            HostInfo originalHost = ConnectionUtils.createHostWithProperties(this.originalWriterHost, initialConnectionProps);
            conn = connectionProvider.connect(originalHost);
            latestTopology = topologyService.getTopology(conn, true);

          } catch (CJCommunicationsException exception) {
            // do nothing
          } catch (SQLException exception) {
            // Propagate exceptions that are not caused by network errors.
            if (!ConnectionUtils.isNetworkException(exception)) {
              log.logTrace(Messages.getString("ClusterAwareWriterFailoverHandler.16"), exception);
              return new WriterFailoverResult(false, false, null, null, "TaskA", exception);
            }
          }

          TimeUnit.MILLISECONDS.sleep(reconnectWriterIntervalMs);
        }

        success = isCurrentHostWriter(latestTopology);
        topologyService.removeFromDownHostList(this.originalWriterHost);
        return new WriterFailoverResult(success, false, latestTopology, success ? conn : null, "TaskA");

      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return new WriterFailoverResult(success, false, latestTopology, success ? conn : null, "TaskA");
      } catch (Exception ex) {
        log.logError(ex);
        return new WriterFailoverResult(false, false, null, null, "TaskA");
      } finally {
        try {
          if (conn != null && !success && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // ignore
        }
        log.logTrace(Messages.getString("ClusterAwareWriterFailoverHandler.8"));
      }
    }

    private boolean isCurrentHostWriter(List<HostInfo> latestTopology) {
      String currentInstanceName =
          this.originalWriterHost
              .getHostProperties()
              .get(TopologyServicePropertyKeys.INSTANCE_NAME);
      HostInfo latestWriter = latestTopology.get(WRITER_CONNECTION_INDEX);
      if (currentInstanceName == null) {
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
  private class WaitForNewWriterHandler implements Callable<WriterFailoverResult> {
    private JdbcConnection currentConnection = null;
    private final HostInfo originalWriterHost;
    private List<HostInfo> currentTopology;
    private HostInfo currentReaderHost;
    private JdbcConnection currentReaderConnection;

    public WaitForNewWriterHandler(
        List<HostInfo> currentTopology,
        HostInfo currentHost) {
      this.currentTopology = currentTopology;
      this.originalWriterHost = currentHost;
    }

    public WriterFailoverResult call() {
      log.logTrace(Messages.getString("ClusterAwareWriterFailoverHandler.9"));

      try {
        boolean success = false;
        while (!success) {
          connectToReader();
          success = refreshTopologyAndConnectToNewWriter();
          if (!success) {
            closeReaderConnection();
          }
        }
        return new WriterFailoverResult(
            true,
            true,
            this.currentTopology,
            this.currentConnection,
            "TaskB");
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return new WriterFailoverResult(false, false, null, null, "TaskB");
      } catch (Exception ex) {
        log.logTrace(Messages.getString("ClusterAwareWriterFailoverHandler.15"), ex);
        return new WriterFailoverResult(false, false, null, null, "TaskB");
      } finally {
        performFinalCleanup();
        log.logTrace(Messages.getString("ClusterAwareWriterFailoverHandler.10"));
      }
    }

    private void connectToReader() throws InterruptedException {
      while (true) {
        try {
          ReaderFailoverResult connResult =
              readerFailoverHandler.getReaderConnection(this.currentTopology);
          if (isValidReaderConnection(connResult)) {
            this.currentReaderConnection = connResult.getConnection();
            this.currentReaderHost = this.currentTopology.get(connResult.getConnectionIndex());
            log.logDebug(
                Messages.getString(
                    "ClusterAwareWriterFailoverHandler.11",
                    new Object[] {connResult.getConnectionIndex(),
                        this.currentReaderHost.getHostPortPair()}));
            break;
          }
        } catch (SQLException e) {
          // ignore
        }
        log.logDebug(Messages.getString("ClusterAwareWriterFailoverHandler.12"));
        TimeUnit.MILLISECONDS.sleep(1);
      }
    }

    private boolean isValidReaderConnection(ReaderFailoverResult result) {
      if (!result.isConnected() || result.getConnection() == null) {
        return false;
      }
      int connIndex = result.getConnectionIndex();
      return connIndex != FailoverConnectionPlugin.NO_CONNECTION_INDEX
          && connIndex < this.currentTopology.size()
          && this.currentTopology.get(connIndex) != null;
    }

    /**
     * Re-read topology and wait for a new writer.
     *
     * @return Returns true if successful.
     */
    private boolean refreshTopologyAndConnectToNewWriter() throws InterruptedException {
      while (true) {
        try {
          List<HostInfo> topology = topologyService.getTopology(this.currentReaderConnection, true);

          if (!topology.isEmpty()) {
            if (topology.size() == 1) {
              // The currently connected reader is in the middle of failover. It's not yet connected
              // to a new writer and works as "standalone" node. The handler needs to
              // wait till the reader gets connected to the entire cluster and fetch a proper
              // cluster topology.

              // do nothing
              log.logTrace(
                  Messages.getString("ClusterAwareWriterFailoverHandler.17",
                      new Object[] {this.currentReaderHost.getHost()}));
            } else {
              this.currentTopology = topology;
              HostInfo writerCandidate = this.currentTopology.get(WRITER_CONNECTION_INDEX);

              if (!isSame(writerCandidate, this.originalWriterHost)) {
                // new writer is available, and it's different from the previous writer
                logTopology();
                if (connectToWriter(writerCandidate)) {
                  return true;
                }
              }
            }
          }
        } catch (CJCommunicationsException | SQLException ex) {
          log.logTrace(Messages.getString("ClusterAwareWriterFailoverHandler.15"), ex);
          return false;
        }

        TimeUnit.MILLISECONDS.sleep(readTopologyIntervalMs);
      }
    }

    private boolean isSame(HostInfo writerCandidate, HostInfo originalWriter) {
      if (writerCandidate == null || originalWriter == null) {
        return false;
      }

      return writerCandidate
          .getHostProperties()
          .get(TopologyServicePropertyKeys.INSTANCE_NAME)
          .equals(originalWriter
              .getHostProperties()
              .get(TopologyServicePropertyKeys.INSTANCE_NAME));
    }

    private boolean connectToWriter(HostInfo writerCandidate) {
      log.logDebug(
          Messages.getString(
              "ClusterAwareWriterFailoverHandler.14",
              new Object[] {writerCandidate.getHostPortPair()}));

      if (isSame(writerCandidate, this.currentReaderHost)) {
        log.logDebug(Messages.getString("ClusterAwareWriterFailoverHandler.18"));
        this.currentConnection = this.currentReaderConnection;
      } else {
        // connect to the new writer
        try {
          HostInfo writerCandidateWithProps = ConnectionUtils.createHostWithProperties(writerCandidate, initialConnectionProps);
          this.currentConnection = connectionProvider.connect(writerCandidateWithProps);
        } catch (SQLException exception) {
          topologyService.addToDownHostList(writerCandidate);
          return false;
        }
      }
      topologyService.removeFromDownHostList(writerCandidate);
      return true;
    }

    /**
     * Close the reader connection if not done so already, and mark the relevant fields as null.
     */
    private void closeReaderConnection() {
      try {
        if (this.currentReaderConnection != null
            && !this.currentReaderConnection.isClosed()) {
          this.currentReaderConnection.close();
        }
      } catch (SQLException e) {
        // ignore
      }
      this.currentReaderConnection = null;
      this.currentReaderHost = null;
    }

    private void performFinalCleanup() {
      // Close the reader connection if it's not needed.
      if (this.currentReaderConnection != null
          && this.currentConnection != this.currentReaderConnection) {
        try {
          this.currentReaderConnection.close();
        } catch (SQLException e) {
          // ignore
        }
      }
    }

    private void logTopology() {
      StringBuilder msg = new StringBuilder();
      for (int i = 0; i < this.currentTopology.size(); i++) {
        HostInfo hostInfo = this.currentTopology.get(i);
        msg.append("\n   [")
            .append(i)
            .append("]: ")
            .append(hostInfo == null ? "<null>" : hostInfo.getHost());
      }
      log.logTrace(
          Messages.getString(
              "ClusterAwareWriterFailoverHandler.13", new Object[] {msg.toString()}));
    }
  }
}
