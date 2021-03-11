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
import com.mysql.cj.util.ClusterAwareUtils;

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
public class ClusterAwareReaderFailoverHandler implements ReaderFailoverHandler {
  protected static final int DEFAULT_FAILOVER_TIMEOUT = 60000; // 60 sec
  protected static final int DEFAULT_READER_CONNECT_TIMEOUT = 30000; // 30 sec

  /** Null logger shared by all connections at startup. */
  protected static final Log NULL_LOGGER = new NullLogger(Log.LOGGER_INSTANCE_NAME);

  /** The logger we're going to use. */
  protected transient Log log = NULL_LOGGER;
  protected Map<String, String> initialConnectionProps;
  protected int maxFailoverTimeoutMs;
  protected int timeoutMs;
  protected final ConnectionProvider connProvider;
  protected final TopologyService topologyService;

  public ClusterAwareReaderFailoverHandler(
          TopologyService topologyService, ConnectionProvider connProvider, Map<String, String> initialConnectionProps, Log log) {
    this(topologyService, connProvider, initialConnectionProps, DEFAULT_FAILOVER_TIMEOUT, DEFAULT_READER_CONNECT_TIMEOUT, log);
  }


  /**
   * ClusterAwareReaderFailoverHandler constructor.
   * */
  public ClusterAwareReaderFailoverHandler(
      TopologyService topologyService, ConnectionProvider connProvider, Map<String, String> initialConnectionProps, int failoverTimeoutMs, int timeoutMs, Log log) {
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
   * @return {@link ConnectionAttemptResult} The results of this process. May return null, which is
   *     considered an unsuccessful result.
   */
  @Override
  public ConnectionAttemptResult failover(List<HostInfo> hosts, HostInfo currentHost)
      throws SQLException {

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<ConnectionAttemptResult> future = executor.submit(() -> {
      ConnectionAttemptResult result = null;
      while(result == null) {
        result = failoverInternal(hosts, currentHost);
        TimeUnit.MILLISECONDS.sleep(1);
      }
      return result;
    });
    executor.shutdown();

    ConnectionAttemptResult defaultResult = new ConnectionAttemptResult(
            null, ClusterAwareConnectionProxy.NO_CONNECTION_INDEX, false);


    try {
      return future.get(this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);

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

  protected ConnectionAttemptResult failoverInternal(List<HostInfo> hosts, HostInfo currentHost)
          throws SQLException {
    this.topologyService.addToDownHostList(currentHost);
    if (hosts == null || hosts.isEmpty()) {
      return new ConnectionAttemptResult(
              null, ClusterAwareConnectionProxy.NO_CONNECTION_INDEX, false);
    }
    Set<String> downHosts = topologyService.getDownHosts();
    List<HostTuple> hostGroup = getHostTuplesByPriority(hosts, downHosts);
    return getConnectionFromHostGroup(hostGroup);
  }

  List<HostTuple> getHostTuplesByPriority(List<HostInfo> hosts, Set<String> downHosts) {
    List<HostTuple> hostGroup = new ArrayList<>();
    addActiveReaders(hostGroup, hosts, downHosts);
    HostInfo writerHost = hosts.get(ClusterAwareConnectionProxy.WRITER_CONNECTION_INDEX);
    if (writerHost != null) {
      hostGroup.add(
          new HostTuple(
              writerHost,
              ClusterAwareConnectionProxy.WRITER_CONNECTION_INDEX));
    }
    addDownHosts(hostGroup, hosts, downHosts);
    return hostGroup;
  }

  private void addActiveReaders(List<HostTuple> list, List<HostInfo> hosts, Set<String> downHosts) {
    List<HostTuple> activeReaders = new ArrayList<>();
    for (int i = ClusterAwareConnectionProxy.WRITER_CONNECTION_INDEX + 1; i < hosts.size(); i++) {
      HostInfo host = hosts.get(i);
      if (!downHosts.contains(host.getHostPortPair())) {
        activeReaders.add(new HostTuple(host, i));
      }
    }
    Collections.shuffle(activeReaders);
    list.addAll(activeReaders);
  }

  private void addDownHosts(List<HostTuple> list, List<HostInfo> hosts, Set<String> downHosts) {
    List<HostTuple> downHostList = new ArrayList<>();
    for (int i = 0; i < hosts.size(); i++) {
      HostInfo host = hosts.get(i);
      if (host != null && downHosts.contains(host.getHostPortPair())) {
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
   * @return {@link ConnectionAttemptResult} The results of this process. May return null, which is
   *     considered an unsuccessful result.
   */
  @Override
  public ConnectionAttemptResult getReaderConnection(List<HostInfo> hostList) throws SQLException {
    Set<String> downHosts = topologyService.getDownHosts();
    List<HostTuple> tuples = getReaderTuplesByPriority(hostList, downHosts);
    return getConnectionFromHostGroup(tuples);
  }

  List<HostTuple> getReaderTuplesByPriority(List<HostInfo> hostList, Set<String> downHosts) {
    List<HostTuple> tuples = new ArrayList<>();
    addActiveReaders(tuples, hostList, downHosts);
    addDownReaders(tuples, hostList, downHosts);
    return tuples;
  }

  private void addDownReaders(List<HostTuple> list, List<HostInfo> hosts, Set<String> downHosts) {
    List<HostTuple> downReaders = new ArrayList<>();
    for (int i = ClusterAwareConnectionProxy.WRITER_CONNECTION_INDEX + 1; i < hosts.size(); i++) {
      HostInfo host = hosts.get(i);
      if (downHosts.contains(host.getHostPortPair())) {
        downReaders.add(new HostTuple(host, i));
      }
    }
    Collections.shuffle(downReaders);
    list.addAll(downReaders);
  }

  private ConnectionAttemptResult getConnectionFromHostGroup(List<HostTuple> hostGroup)
      throws SQLException {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CompletionService<ConnectionAttemptResult> completionService =
        new ExecutorCompletionService<>(executor);

    ConnectionAttemptResult result;

    try {
      for (int i = 0; i < hostGroup.size(); i += 2) {
        boolean secondAttemptPresent = i + 1 < hostGroup.size();
        Future<ConnectionAttemptResult> attempt1 =
                completionService.submit(
                        new ConnectionAttemptTask(
                                this.connProvider, hostGroup.get(i), this.topologyService, this.initialConnectionProps, this.log));
        if (secondAttemptPresent) {
          Future<ConnectionAttemptResult> attempt2 =
                  completionService.submit(
                          new ConnectionAttemptTask(
                                  this.connProvider, hostGroup.get(i + 1), this.topologyService, this.initialConnectionProps, this.log));
          result = getResultFromAttemptPair(attempt1, attempt2, completionService);
        } else {
          result = getNextResult(completionService);
        }

        if (result.isSuccess()) {
          this.log.logDebug(
                  Messages.getString(
                          "ClusterAwareReaderFailoverHandler.2", new Object[]{result.getConnectionIndex()}));
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

      return new ConnectionAttemptResult(
              null, ClusterAwareConnectionProxy.NO_CONNECTION_INDEX, false);

    } finally {
      executor.shutdownNow();
    }
  }

  private ConnectionAttemptResult getResultFromAttemptPair(
      Future<ConnectionAttemptResult> attempt1,
      Future<ConnectionAttemptResult> attempt2,
      CompletionService<ConnectionAttemptResult> service)
      throws SQLException {
    try {
      Future<ConnectionAttemptResult> firstCompleted =
          service.poll(this.timeoutMs, TimeUnit.MILLISECONDS);
      if (firstCompleted != null) {
        ConnectionAttemptResult result = firstCompleted.get();
        if (result.isSuccess()) {
          if (firstCompleted.equals(attempt1)) {
            attempt2.cancel(true);
          } else {
            attempt1.cancel(true);
          }
          return result;
        }
      }
    } catch (ExecutionException e) {
      return getNextResult(service);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // "Thread was interrupted"
      throw new SQLException(Messages.getString("ClusterAwareReaderFailoverHandler.1"), "70100", e);
    }
    return getNextResult(service);
  }

  private ConnectionAttemptResult getNextResult(CompletionService<ConnectionAttemptResult> service)
      throws SQLException {
    try {
      Future<ConnectionAttemptResult> result = service.poll(this.timeoutMs, TimeUnit.MILLISECONDS);
      if (result == null) {
        return new ConnectionAttemptResult(
            null, ClusterAwareConnectionProxy.NO_CONNECTION_INDEX, false);
      }
      return result.get();
    } catch (ExecutionException e) {
      return new ConnectionAttemptResult(
          null, ClusterAwareConnectionProxy.NO_CONNECTION_INDEX, false);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // "Thread was interrupted"
      throw new SQLException(Messages.getString("ClusterAwareReaderFailoverHandler.1"), "70100", e);
    }
  }

  private static class ConnectionAttemptTask implements Callable<ConnectionAttemptResult> {
    private final ConnectionProvider connProvider;
    private final HostTuple newHostTuple;
    private final TopologyService topologyService;
    private final Map<String, String> initialConnectionProps;
    private final transient Log log;

    private ConnectionAttemptTask(
        ConnectionProvider connProvider,
        HostTuple newHostTuple,
        TopologyService topologyService,
        Map<String, String> initialConnectionProps,
        Log log) {
      this.connProvider = connProvider;
      this.newHostTuple = newHostTuple;
      this.topologyService = topologyService;
      this.initialConnectionProps = initialConnectionProps;
      this.log = log;
    }

    /**
     * Call ConnectionAttemptResult.
     * */
    @Override
    public ConnectionAttemptResult call() {
      HostInfo newHost = this.newHostTuple.getHost();
      this.log.logDebug(
          Messages.getString(
              "ClusterAwareReaderFailoverHandler.3",
              new Object[] {this.newHostTuple.getIndex(), newHost.getHostPortPair()}));

      try {
        HostInfo newHostWithProps = ClusterAwareUtils.copyHostInfoAndAddProps(newHost, this.initialConnectionProps);
        JdbcConnection conn = this.connProvider.connect(newHostWithProps);
        topologyService.removeFromDownHostList(newHost);
        this.log.logDebug(
            Messages.getString(
                "ClusterAwareReaderFailoverHandler.4",
                new Object[] {this.newHostTuple.getIndex(), newHost.getHostPortPair()}));
        return new ConnectionAttemptResult(conn, this.newHostTuple.getIndex(), true);
      } catch (SQLException e) {
        topologyService.addToDownHostList(newHost);
        this.log.logDebug(
            Messages.getString(
                "ClusterAwareReaderFailoverHandler.5",
                new Object[] {this.newHostTuple.getIndex(), newHost.getHostPortPair()}));
        return new ConnectionAttemptResult(
            null, ClusterAwareConnectionProxy.NO_CONNECTION_INDEX, false);
      }
    }
  }

  /**
   * HostTuple class.
   * */
  protected static class HostTuple {
    private final HostInfo host;
    private final int index;

    HostTuple(HostInfo host, int index) {
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
