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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An implementation of ReaderFailoverHandler.
 *
 * <p>Reader Failover Process goal is to connect to any available reader. In order to connect
 * faster, this implementation tries to connect to two readers at the same time. The first
 * successfully connected reader is returned as the process result. If both readers are unavailable
 * (i.e. could not be connected to), the process picks up another pair of readers and repeat. If no
 * reader has been connected to, the process may consider a writer host, and other hosts marked
 * down, to connect to.
 */
public class ClusterAwareReaderFailoverHandler implements IReaderFailoverHandler {
  protected static final int DEFAULT_FAILOVER_TIMEOUT = 60000; // 60 sec
  protected static final int DEFAULT_READER_CONNECT_TIMEOUT = 30000; // 30 sec

  /** Null logger shared by all connections at startup. */
  protected static final Log NULL_LOGGER = new NullLogger(Log.LOGGER_INSTANCE_NAME);

  /** The logger we're going to use. */
  protected transient Log log = NULL_LOGGER;
  protected Map<String, String> initialConnectionProps;
  protected int maxFailoverTimeoutMs;
  protected int timeoutMs;
  protected final IConnectionProvider connProvider;
  protected final ITopologyService topologyService;

  /**
   * ClusterAwareReaderFailoverHandler constructor.
   *
   * @param topologyService An implementation of {@link ITopologyService} that obtains and
   *                        caches a cluster's topology.
   * @param connProvider A provider for creating new connections.
   * @param initialConnectionProps The initial connection properties to copy over to the
   *                               new reader.
   */
  public ClusterAwareReaderFailoverHandler(
      ITopologyService topologyService,
      IConnectionProvider connProvider,
      Map<String, String> initialConnectionProps,
      Log log) {
    this(
        topologyService,
        connProvider,
        initialConnectionProps,
        DEFAULT_FAILOVER_TIMEOUT,
        DEFAULT_READER_CONNECT_TIMEOUT,
        log);
  }

  /**
   * ClusterAwareReaderFailoverHandler constructor.
   *
   * @param topologyService An implementation of {@link ITopologyService} that obtains and
   *                        caches a cluster's topology.
   * @param connProvider A provider for creating new connections.
   * @param initialConnectionProps The initial connection properties to copy over to the
   *                               new reader.
   * @param failoverTimeoutMs Maximum allowed time in milliseconds to attempt reconnecting
   *                         to a new reader instance after a cluster failover is initiated.
   * @param timeoutMs Maximum allowed time for the entire reader failover process.
   * @param log An implementation of {@link Log}.
   */
  public ClusterAwareReaderFailoverHandler(
      ITopologyService topologyService,
      IConnectionProvider connProvider,
      Map<String, String> initialConnectionProps,
      int failoverTimeoutMs,
      int timeoutMs,
      Log log) {
    this.topologyService = topologyService;
    this.connProvider = connProvider;
    this.initialConnectionProps = initialConnectionProps;
    this.maxFailoverTimeoutMs = failoverTimeoutMs;
    this.timeoutMs = timeoutMs;

    if (log != null) {
      this.log = log;
    }
  }

  /**
   * Set process timeout in millis. Entire process of connecting to a reader will be limited by this
   * time duration.
   *
   * @param timeoutMs Process timeout in millis
   */
  protected void setTimeoutMs(int timeoutMs) {
    this.timeoutMs = timeoutMs;
  }

  /**
   * Called to start Reader Failover Process. This process tries to connect to any reader. If no
   * reader is available then driver may also try to connect to a writer host, down hosts, and the
   * current reader host.
   *
   * @param hosts Cluster current topology
   * @param currentHost The currently connected host that has failed.
   * @return {@link ReaderFailoverResult} The results of this process.
   */
  @Override
  public ReaderFailoverResult failover(List<HostInfo> hosts, HostInfo currentHost)
      throws SQLException {
    if (Util.isNullOrEmpty(hosts)) {
      this.log.logDebug(Messages.getString("ClusterAwareReaderFailover.6", new Object[] {"failover"}));
      return new ReaderFailoverResult(
          null,
          FailoverConnectionPlugin.NO_CONNECTION_INDEX,
          false);
    }

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<ReaderFailoverResult> future =
        submitInternalFailoverTask(hosts, currentHost, executor);
    return getInternalFailoverResult(executor, future);
  }

  private Future<ReaderFailoverResult> submitInternalFailoverTask(
      List<HostInfo> hosts,
      HostInfo currentHost,
      ExecutorService executor) {
    Future<ReaderFailoverResult> future = executor.submit(() -> {
      ReaderFailoverResult result = null;
      while (result == null || !result.isConnected()) {
        result = failoverInternal(hosts, currentHost);
        TimeUnit.SECONDS.sleep(1);
      }
      return result;
    });
    executor.shutdown();
    return future;
  }

  private ReaderFailoverResult getInternalFailoverResult(
      ExecutorService executor,
      Future<ReaderFailoverResult> future) throws SQLException {
    ReaderFailoverResult defaultResult = new ReaderFailoverResult(
        null, FailoverConnectionPlugin.NO_CONNECTION_INDEX, false);
    try {
      ReaderFailoverResult result =
          future.get(this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
      return result == null ? defaultResult : result;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException(
          Messages.getString("ClusterAwareReaderFailoverHandler.1"), "70100", e);

    } catch (ExecutionException e) {
      return defaultResult;

    } catch (TimeoutException e) {
      future.cancel(true);
      return defaultResult;

    } finally {
      if (!executor.isTerminated()) {
        executor.shutdownNow(); // terminate all remaining tasks
      }
    }
  }

  protected ReaderFailoverResult failoverInternal(
      List<HostInfo> hosts,
      HostInfo currentHost)
      throws SQLException {
    this.topologyService.addToDownHostList(currentHost);
    Set<String> downHosts = topologyService.getDownHosts();
    List<HostTuple> hostGroup = getHostTuplesByPriority(hosts, downHosts);
    return getConnectionFromHostGroup(hostGroup);
  }

  public List<HostTuple> getHostTuplesByPriority(List<HostInfo> hosts, Set<String> downHosts) {
    List<HostTuple> hostGroup = new ArrayList<>();
    addActiveReaders(hostGroup, hosts, downHosts);
    HostInfo writerHost = hosts.get(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX);
    hostGroup.add(
        new HostTuple(
            writerHost,
            FailoverConnectionPlugin.WRITER_CONNECTION_INDEX));
    addDownHosts(hostGroup, hosts, downHosts);
    return hostGroup;
  }

  private void addActiveReaders(
      List<HostTuple> list,
      List<HostInfo> hosts,
      Set<String> downHosts) {
    List<HostTuple> activeReaders = new ArrayList<>();
    for (int i = FailoverConnectionPlugin.WRITER_CONNECTION_INDEX + 1;
         i < hosts.size(); i++) {
      HostInfo host = hosts.get(i);
      if (!downHosts.contains(host.getHostPortPair())) {
        activeReaders.add(new HostTuple(host, i));
      }
    }
    Collections.shuffle(activeReaders);
    list.addAll(activeReaders);
  }

  private void addDownHosts(
      List<HostTuple> list,
      List<HostInfo> hosts,
      Set<String> downHosts) {
    List<HostTuple> downHostList = new ArrayList<>();
    for (int i = 0; i < hosts.size(); i++) {
      HostInfo host = hosts.get(i);
      if (downHosts.contains(host.getHostPortPair())) {
        downHostList.add(new HostTuple(host, i));
      }
    }
    Collections.shuffle(downHostList);
    list.addAll(downHostList);
  }

  /**
   * Called to get any available reader connection. If no reader is available then result of process
   * is unsuccessful. This process will not attempt to connect to the writer.
   *
   * @param hostList Cluster current topology
   * @return {@link ReaderFailoverResult} The results of this process.
   */
  @Override
  public ReaderFailoverResult getReaderConnection(List<HostInfo> hostList)
      throws SQLException {
    if (Util.isNullOrEmpty(hostList)) {
      this.log.logDebug(Messages.getString("ClusterAwareReaderFailover.6", new Object[] {"getReaderConnection"}));
      return new ReaderFailoverResult(
          null,
          FailoverConnectionPlugin.NO_CONNECTION_INDEX,
          false);
    }

    Set<String> downHosts = topologyService.getDownHosts();
    List<HostTuple> tuples = getReaderTuplesByPriority(hostList, downHosts);
    return getConnectionFromHostGroup(tuples);
  }

  public List<HostTuple> getReaderTuplesByPriority(
      List<HostInfo> hostList,
      Set<String> downHosts) {
    List<HostTuple> tuples = new ArrayList<>();
    addActiveReaders(tuples, hostList, downHosts);
    addDownReaders(tuples, hostList, downHosts);
    return tuples;
  }

  private void addDownReaders(
      List<HostTuple> list,
      List<HostInfo> hosts,
      Set<String> downHosts) {
    List<HostTuple> downReaders = new ArrayList<>();
    for (int i = FailoverConnectionPlugin.WRITER_CONNECTION_INDEX + 1; i < hosts.size(); i++) {
      HostInfo host = hosts.get(i);
      if (downHosts.contains(host.getHostPortPair())) {
        downReaders.add(new HostTuple(host, i));
      }
    }
    Collections.shuffle(downReaders);
    list.addAll(downReaders);
  }

  private ReaderFailoverResult getConnectionFromHostGroup(List<HostTuple> hostGroup)
      throws SQLException {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CompletionService<ReaderFailoverResult> completionService =
        new ExecutorCompletionService<>(executor);

    try {
      for (int i = 0; i < hostGroup.size(); i += 2) {
        // submit connection attempt tasks in batches of 2
        ReaderFailoverResult result =
            getResultFromNextTaskBatch(hostGroup, executor, completionService, i);
        if (result.isConnected()) {
          return result;
        }

        try {
          TimeUnit.MILLISECONDS.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SQLException(
              Messages.getString("ClusterAwareReaderFailoverHandler.1"), "70100", e);
        }
      }

      return new ReaderFailoverResult(
          null,
          FailoverConnectionPlugin.NO_CONNECTION_INDEX,
          false);
    } finally {
      executor.shutdownNow();
    }
  }

  private ReaderFailoverResult getResultFromNextTaskBatch(
      List<HostTuple> hostGroup,
      ExecutorService executor,
      CompletionService<ReaderFailoverResult> completionService,
      int i) throws SQLException {
    ReaderFailoverResult result;
    int numTasks = i + 1 < hostGroup.size() ? 2 : 1;
    completionService.submit(new ConnectionAttemptTask(hostGroup.get(i)));
    if (numTasks == 2) {
      completionService.submit(new ConnectionAttemptTask(hostGroup.get(i + 1)));
    }
    for (int taskNum = 0; taskNum < numTasks; taskNum++) {
      result = getNextResult(completionService);
      if (result.isConnected()) {
        executor.shutdownNow();
        this.log.logDebug(
            Messages.getString(
                "ClusterAwareReaderFailoverHandler.2",
                new Object[] {result.getConnectionIndex()}));
        return result;
      }
    }
    return new ReaderFailoverResult(
        null,
        FailoverConnectionPlugin.NO_CONNECTION_INDEX,
        false);
  }

  private ReaderFailoverResult getNextResult(CompletionService<ReaderFailoverResult> service)
      throws SQLException {
    ReaderFailoverResult defaultResult = new ReaderFailoverResult(
        null, FailoverConnectionPlugin.NO_CONNECTION_INDEX, false);
    try {
      Future<ReaderFailoverResult> future =
          service.poll(this.timeoutMs, TimeUnit.MILLISECONDS);
      if (future == null) {
        return defaultResult;
      }
      ReaderFailoverResult result = future.get();
      return result == null ? defaultResult : result;
    } catch (ExecutionException e) {
      return defaultResult;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // "Thread was interrupted"
      throw new SQLException(
          Messages.getString("ClusterAwareReaderFailoverHandler.1"),
          "70100",
          e);
    }
  }

  private class ConnectionAttemptTask implements Callable<ReaderFailoverResult> {
    private final HostTuple newHostTuple;

    private ConnectionAttemptTask(HostTuple newHostTuple) {
      this.newHostTuple = newHostTuple;
    }

    /**
     * Call ConnectionAttemptResult.
     * */
    @Override
    public ReaderFailoverResult call() {
      HostInfo newHost = this.newHostTuple.getHost();
      log.logDebug(
          Messages.getString(
              "ClusterAwareReaderFailoverHandler.3",
              new Object[] {this.newHostTuple.getIndex(), newHost.getHostPortPair()}));

      try {
        HostInfo newHostWithProps =
            ConnectionUtils.copyWithAdditionalProps(newHost, initialConnectionProps);
        JdbcConnection conn = connProvider.connect(newHostWithProps);
        topologyService.removeFromDownHostList(newHost);
        log.logDebug(
            Messages.getString(
                "ClusterAwareReaderFailoverHandler.4",
                new Object[] {this.newHostTuple.getIndex(), newHost.getHostPortPair()}));
        return new ReaderFailoverResult(conn, this.newHostTuple.getIndex(), true);
      } catch (SQLException e) {
        topologyService.addToDownHostList(newHost);
        log.logDebug(
            Messages.getString(
                "ClusterAwareReaderFailoverHandler.5",
                new Object[] {this.newHostTuple.getIndex(), newHost.getHostPortPair()}));
        return new ReaderFailoverResult(
            null,
            FailoverConnectionPlugin.NO_CONNECTION_INDEX,
            false);
      }
    }
  }

  /**
   * HostTuple class.
   * */
  public static class HostTuple {
    private final HostInfo host;
    private final int index;

    public HostTuple(HostInfo host, int index) {
      this.host = host;
      this.index = index;
    }

    public HostInfo getHost() {
      return host;
    }

    public int getIndex() {
      return index;
    }
  }
}
