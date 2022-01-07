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

import com.mysql.cj.Messages;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.ConnectionUtils;
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
      WriterFailoverResult result = getNextResult(executorService, completionService);
      if (result.isConnected()) {
        return result;
      }
      result = getNextResult(executorService, completionService);
      if (result.isConnected()) {
        return result;
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
    HostInfo writerHostWithInitialProps = ConnectionUtils.copyWithAdditionalProps(
        writerHost,
        this.initialConnectionProps);
    this.topologyService.addToDownHostList(writerHost);
    completionService.submit(new ReconnectToWriterHandler(writerHostWithInitialProps));
    completionService.submit(new WaitForNewWriterHandler(
        currentTopology,
        writerHostWithInitialProps));
    executorService.shutdown();
  }

  private WriterFailoverResult getNextResult(
      ExecutorService executorService,
      CompletionService<WriterFailoverResult> completionService) throws SQLException {
    try {
      Future<WriterFailoverResult> firstCompleted = completionService.poll(
          this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
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
      try {
        while (true) {
          try {
            JdbcConnection conn = connectionProvider.connect(this.originalWriterHost);

            List<HostInfo> latestTopology = topologyService.getTopology(conn, true);
            if (!Util.isNullOrEmpty(latestTopology)
                && isCurrentHostWriter(latestTopology)) {
              topologyService.removeFromDownHostList(this.originalWriterHost);
              return new WriterFailoverResult(true, false, latestTopology, conn, "TaskA");
            }
          } catch (SQLException exception) {
            // ignore
          }

          TimeUnit.MILLISECONDS.sleep(reconnectWriterIntervalMs);
        }
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return new WriterFailoverResult(false, false, null, null, "TaskA");
      } catch (Exception ex) {
        log.logError(ex);
        throw ex;
      } finally {
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
        log.logError(Messages.getString(
            "ClusterAwareWriterFailoverHandler.15",
            new Object[] {ex.getMessage()}));
        throw ex;
      } finally {
        performFinalCleanup();
      }
    }

    private void connectToReader() throws InterruptedException {
      while (true) {
        try {
          ReaderFailoverResult connResult =
              readerFailoverHandler.getReaderConnection(this.currentTopology);
          if (isValidReaderConnection(connResult)) {
            this.currentReaderConnection = connResult.getConnection();
            this.currentReaderHost =
                this.currentTopology.get(connResult.getConnectionIndex());
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
          List<HostInfo> topology =
              topologyService.getTopology(this.currentReaderConnection, true);

          if (!topology.isEmpty()) {
            this.currentTopology = topology;
            HostInfo writerCandidate = this.currentTopology.get(WRITER_CONNECTION_INDEX);
            logTopology();

            if (!isSame(writerCandidate, this.originalWriterHost)) {
              // new writer is available and it's different from the previous writer
              if (connectToWriter(writerCandidate)) {
                return true;
              }
            }
          }
        } catch (SQLException e) {
          // ignore
        }

        TimeUnit.MILLISECONDS.sleep(readTopologyIntervalMs);
      }
    }

    private boolean isSame(HostInfo writerCandidate, HostInfo originalWriter) {
      if (writerCandidate == null) {
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
      try {
        log.logDebug(
            Messages.getString(
                "ClusterAwareWriterFailoverHandler.14",
                new Object[] {writerCandidate.getHostPortPair()}));

        if (isSame(writerCandidate, this.currentReaderHost)) {
          this.currentConnection = this.currentReaderConnection;
        } else {
          // connect to the new writer
          HostInfo writerCandidateWithProps =
              ConnectionUtils.copyWithAdditionalProps(
                  writerCandidate,
                  initialConnectionProps);
          this.currentConnection = connectionProvider.connect(writerCandidateWithProps);
        }

        topologyService.removeFromDownHostList(writerCandidate);
        return true;
      } catch (SQLException exception) {
        topologyService.addToDownHostList(writerCandidate);
        return false;
      }
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
      log.logTrace(Messages.getString("ClusterAwareWriterFailoverHandler.10"));
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
