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

import com.mysql.cj.Messages;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;

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
public class ClusterAwareWriterFailoverHandler implements WriterFailoverHandler {

  static final int WRITER_CONNECTION_INDEX = 0;

  /** Null logger shared by all connections at startup. */
  protected static final Log NULL_LOGGER = new NullLogger(Log.LOGGER_INSTANCE_NAME);

  /** The logger we're going to use. */
  protected transient Log log = NULL_LOGGER;

  protected int maxFailoverTimeoutMs = 60000; // 60 sec
  protected int readTopologyIntervalMs = 5000; // 5 sec
  protected int reconnectWriterIntervalMs = 5000; // 5 sec
  protected Map<String, String> initialConnectionProps;
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
   * */
  public ClusterAwareWriterFailoverHandler(
      TopologyService topologyService,
      ConnectionProvider connectionProvider,
      ReaderFailoverHandler readerFailoverHandler,
      Map<String, String> initialConnectionProps,
      int failoverTimeoutMs,
      int readTopologyIntervalMs,
      int reconnectWriterIntervalMs,
      Log log) {
    this(topologyService, connectionProvider, readerFailoverHandler, initialConnectionProps, log);
    this.maxFailoverTimeoutMs = failoverTimeoutMs;
    this.readTopologyIntervalMs = readTopologyIntervalMs;
    this.reconnectWriterIntervalMs = reconnectWriterIntervalMs;
  }

  /**
   * Called to start Writer Failover Process.
   *
   * @param currentTopology Cluster current topology
   * @return {@link WriterFailoverResult} The results of this process. May return null, which is
   *     considered an unsuccessful result.
   */
  @Override
  public WriterFailoverResult failover(List<HostInfo> currentTopology) throws SQLException {
    if(currentTopology == null) {
      this.log.logError(Messages.getString("ClusterAwareWriterFailoverHandler.7"));
      return new WriterFailoverResult(false, false, null, null);
    }
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    CompletionService<WriterFailoverResult> completionService =
            new ExecutorCompletionService<>(executorService);
    try {
      for(int numTasks = submitTasks(currentTopology, executorService, completionService); numTasks > 0; numTasks--) {
        WriterFailoverResult result = getNextResult(executorService, completionService);
        if(result.isConnected()) return result;
      }

      this.log.logDebug(Messages.getString("ClusterAwareWriterFailoverHandler.3"));
      return new WriterFailoverResult(false, false, null, null);
    } finally {
      if (!executorService.isTerminated()) {
        executorService.shutdownNow(); // terminate all remaining tasks
      }
    }
  }

  private int submitTasks(List<HostInfo> currentTopology, ExecutorService executorService,
                          CompletionService<WriterFailoverResult> completionService) {
    int numTasks = 0;
    HostInfo writerHost = currentTopology.isEmpty() ? null : currentTopology.get(WRITER_CONNECTION_INDEX);
    HostInfo writerHostWithInitialProps = writerHost == null ?
            null : ClusterAwareUtils.copyWithAdditionalProps(writerHost, this.initialConnectionProps);
    this.topologyService.addToDownHostList(writerHost);
    if (writerHostWithInitialProps != null) {
      completionService.submit(new ReconnectToWriterHandler(writerHostWithInitialProps));
      numTasks++;
    }
    if(!currentTopology.isEmpty()) {
      completionService.submit(new WaitForNewWriterHandler(currentTopology, writerHostWithInitialProps));
      numTasks++;
    }
    executorService.shutdown();
    return numTasks;
  }

  private WriterFailoverResult getNextResult(ExecutorService executorService,
      CompletionService<WriterFailoverResult> completionService) throws SQLException{
    try {
      Future<WriterFailoverResult> firstCompleted = completionService.poll(
              this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
      if(firstCompleted == null) {
        // The task was unsuccessful and we have timed out
        return new WriterFailoverResult(false, false, new ArrayList<>(), null);
      }
      WriterFailoverResult result = firstCompleted.get();
      if(result.isConnected()) {
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
    return new WriterFailoverResult(false, false, new ArrayList<>(), null);
  }

  private void logTaskSuccess(WriterFailoverResult result) {
    List<HostInfo> topology = result.getTopology();
    if(topology == null || topology.isEmpty() || topology.get(WRITER_CONNECTION_INDEX) == null) {
      String taskId = result.isNewHost() ? "TaskB" : "TaskA";
      this.log.logError(Messages.getString("ClusterAwareWriterFailoverHandler.5", new Object[] { taskId }));
      return;
    }
    String newWriterHost = topology.get(WRITER_CONNECTION_INDEX).getHostPortPair();
    if(result.isNewHost()) {
      this.log.logDebug(Messages.getString("ClusterAwareWriterFailoverHandler.4", new Object[] { newWriterHost }));
    } else  {
      this.log.logDebug(Messages.getString("ClusterAwareWriterFailoverHandler.2", new Object[] { newWriterHost }));
    }
  }

  private SQLException createInterruptedException(InterruptedException e) {
    // "Thread was interrupted"
    return new SQLException(Messages.getString("ClusterAwareWriterFailoverHandler.1"), "70100", e);
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
            if (latestTopology != null && !latestTopology.isEmpty() && isCurrentHostWriter(latestTopology)) {
              topologyService.removeFromDownHostList(this.originalWriterHost);
              return new WriterFailoverResult(true, false, latestTopology, conn);
            }
          } catch (SQLException exception) {
            // ignore
          }

          TimeUnit.MILLISECONDS.sleep(reconnectWriterIntervalMs);
        }
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return new WriterFailoverResult(false, false, null, null);
      } catch (Exception ex) {
        log.logError(ex);
        throw ex;
      } finally {
        log.logTrace(Messages.getString("ClusterAwareWriterFailoverHandler.8"));
      }
    }

    private boolean isCurrentHostWriter(List<HostInfo> latestTopology) {
      String currentInstanceName =
          this.originalWriterHost.getHostProperties().get(TopologyServicePropertyKeys.INSTANCE_NAME);
      HostInfo latestWriter = latestTopology.get(WRITER_CONNECTION_INDEX);
      if (currentInstanceName == null || latestWriter == null) {
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
          connectoToReader();
          success = refreshTopologyAndConnectToNewWriter();
          if(!success) {
            closeReaderConnection();
          }
        }
        return new WriterFailoverResult(true, true, this.currentTopology, this.currentConnection);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return new WriterFailoverResult(false, false, null, null);
      } catch (Exception ex) {
        log.logError(Messages.getString("ClusterAwareWriterFailoverHandler.15", new Object[] { ex.getMessage() }));
        throw ex;
      } finally {
        performFinalCleanup();
      }
    }

    private void connectoToReader() throws InterruptedException {
      while (true) {
        try {
          ReaderFailoverResult connResult =
                  readerFailoverHandler.getReaderConnection(this.currentTopology);
          if(isValidReaderConnection(connResult)) {
            this.currentReaderConnection = connResult.getConnection();
            this.currentReaderHost = this.currentTopology.get(connResult.getConnectionIndex());
            log.logDebug(
                    Messages.getString(
                            "ClusterAwareWriterFailoverHandler.11",
                            new Object[]{connResult.getConnectionIndex(), this.currentReaderHost.getHostPortPair()}));
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
      if(!result.isConnected() || result.getConnection() == null) {
        return false;
      }
      int connIndex = result.getConnectionIndex();
      return connIndex != ClusterAwareConnectionProxy.NO_CONNECTION_INDEX
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
        this.currentTopology = topologyService.getTopology(this.currentReaderConnection, true);
        if (this.currentTopology == null) {
          // topology couldn't be obtained; it might be issues with reader connection
          return false;
        }

        if (!this.currentTopology.isEmpty()) {
          logTopology();
          HostInfo writerCandidate = this.currentTopology.get(WRITER_CONNECTION_INDEX);

          if (writerCandidate != null && (this.originalWriterHost == null
                  || !isSame(writerCandidate, this.originalWriterHost))) {
            // new writer is available and it's different from the previous writer
            if (connectToWriter(writerCandidate)) {
              return true;
            }
          }
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
          .equals(originalWriter.getHostProperties().get(TopologyServicePropertyKeys.INSTANCE_NAME));
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
                  ClusterAwareUtils.copyWithAdditionalProps(writerCandidate, initialConnectionProps);
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
     * Close the reader connection if not done so already, and mark the relevant fields as null
     */
    private void closeReaderConnection() {
      try {
        if (this.currentReaderConnection != null && !this.currentReaderConnection.isClosed()) {
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
      if (this.currentReaderConnection != null && this.currentConnection != this.currentReaderConnection) {
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
