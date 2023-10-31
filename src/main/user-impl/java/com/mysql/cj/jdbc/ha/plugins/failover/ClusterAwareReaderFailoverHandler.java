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
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.util.ConnectionUtils;
import com.mysql.cj.jdbc.ha.plugins.IConnectionProvider;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;
import com.mysql.cj.util.Util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  protected boolean enableFailoverStrictReader;
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
        false,
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
      boolean enableFailoverStrictReader,
      Log log) {
    this.topologyService = topologyService;
    this.connProvider = connProvider;
    this.initialConnectionProps = initialConnectionProps;
    this.maxFailoverTimeoutMs = failoverTimeoutMs;
    this.timeoutMs = timeoutMs;
    this.enableFailoverStrictReader = enableFailoverStrictReader;
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
      this.log.logDebug(Messages.getString("ClusterAwareReaderFailoverHandler.6", new Object[] {"failover"}));
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
      ReaderFailoverResult result;
      try {
        while (true) {
          result = failoverInternal(hosts, currentHost);
          if (result != null && result.isConnected()) {

            if (!this.enableFailoverStrictReader) {
              // connection to any node works for us
              return result;
            }

            // need to ensure that the new connection is a connection to a reader node
            final HostInfo newHost = hosts.get(result.getConnectionIndex());
            final List<HostInfo> topology = this.topologyService.getTopology(result.getConnection(), true);
            for (int i = FailoverConnectionPlugin.WRITER_CONNECTION_INDEX + 1; i < topology.size(); i++) {
              if (topology.get(i).equalHostPortPair(newHost)) {
                return result;
              }
            }

            // New node is not found in the latest topology.There are few possible reasons for that:
            // - Node is not yet presented in the topology due to the failover process in progress
            // - Node is in the topology but it isn't a
            //   READER (that is not an acceptable option due to this.strictReader setting)
            // Need to continue this loop and to make another try to connect to a reader.
            try {
              result.getConnection().close();
            } catch (SQLException ex) {
              // ignore
            }
          }

          TimeUnit.SECONDS.sleep(1);
        }
      } catch (SQLException ex) {
        return new ReaderFailoverResult(null, -1, false, ex);
      } catch (Exception ex) {
        return new ReaderFailoverResult(null, -1, false, new SQLException(ex));
      }
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
    if (writerHost != null && (!this.enableFailoverStrictReader || hosts.size() == 1)) {
      hostGroup.add(
          new HostTuple(
              writerHost,
              FailoverConnectionPlugin.WRITER_CONNECTION_INDEX));
    }
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
      this.log.logDebug(Messages.getString("ClusterAwareReaderFailoverHandler.6", new Object[] {"getReaderConnection"}));
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
        if (result.isConnected() || result.getException() != null) {
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
      if (result.getException() != null) {
        executor.shutdownNow();
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
        HostInfo newHostWithProps = ConnectionUtils.createHostWithProperties(newHost, initialConnectionProps);
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

        // Propagate exceptions that are not caused by network errors.
        if (!ConnectionUtils.isNetworkException(e)) {
          return new ReaderFailoverResult(
              null,
              FailoverConnectionPlugin.NO_CONNECTION_INDEX,
              false,
              e);
        }

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

    @Override
    public int hashCode() {
      return Objects.hash(host, index);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }

      if (obj.getClass() != this.getClass()) {
        return false;
      }

      final HostTuple other = (HostTuple) obj;

      return (other.host != null && this.host.equalHostPortPair(other.host) && (this.index == other.index));
    }
  }
}
