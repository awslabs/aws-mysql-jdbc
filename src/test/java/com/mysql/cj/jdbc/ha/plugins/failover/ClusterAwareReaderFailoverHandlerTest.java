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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.ha.plugins.IConnectionProvider;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ClusterAwareReaderFailoverHandlerTest class.
 */
public class ClusterAwareReaderFailoverHandlerTest {
  static final List<HostInfo> testHosts;

  static {
    testHosts = new ArrayList<>();
    List<String> instances = Arrays.asList(
        "writer-1",
        "reader-1",
        "reader-2",
        "reader-3",
        "reader-4",
        "reader-5"
    );

    for (String instance : instances) {
      HostInfo host = ClusterAwareTestUtils.createBasicHostInfo(instance, "test");
      testHosts.add(host);
    }
  }

  static final Map<String, String> testConnectionProps;

  static {
    testConnectionProps = new HashMap<>();
    testConnectionProps.put(PropertyKey.DBNAME.getKeyName(), "test");
  }

  final Log mockLog = Mockito.mock(Log.class);

  @Test
  public void testFailover() throws SQLException {
    // original host list: [active writer, active reader, current connection (reader), active
    // reader, down reader, active reader]
    // priority order by index (the subsets will be shuffled): [[1, 3, 5], 0, [2, 4]]
    // connection attempts are made in pairs using the above list
    // expected test result: successful connection for host at index 4
    final ITopologyService mockTopologyService = Mockito.mock(ITopologyService.class);
    final IConnectionProvider mockConnProvider = Mockito.mock(IConnectionProvider.class);
    final ConnectionImpl mockConnection = Mockito.mock(ConnectionImpl.class);
    final List<HostInfo> hosts = getHostsFromTestUrls(6);
    final int currentHostIndex = 2;
    final int successHostIndex = 4;
    for (int i = 0; i < hosts.size(); i++) {
      if (i != successHostIndex) {
        when(mockConnProvider.connect(refEq(hosts.get(i)))).thenThrow(new SQLException());
      } else {
        when(mockConnProvider.connect(refEq(hosts.get(i)))).thenReturn(mockConnection);
      }
    }

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(hosts.get(hostIndex).getHostPortPair());
    }
    when(mockTopologyService.getDownHosts()).thenReturn(downHosts);

    final IReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockTopologyService,
            mockConnProvider,
            testConnectionProps,
            mockLog);
    final ReaderFailoverResult result =
        target.failover(hosts, hosts.get(currentHostIndex));

    assertTrue(result.isConnected());
    assertSame(mockConnection, result.getConnection());
    assertEquals(successHostIndex, result.getConnectionIndex());

    final HostInfo successHost = hosts.get(successHostIndex);
    verify(mockTopologyService, atLeast(4)).addToDownHostList(any());
    verify(mockTopologyService, never()).addToDownHostList(eq(successHost));
    verify(mockTopologyService, times(1)).removeFromDownHostList(eq(successHost));
  }

  private List<HostInfo> getHostsFromTestUrls(int numHosts) {
    final List<HostInfo> hosts = new ArrayList<>();
    if (numHosts < 0 || numHosts > testHosts.size()) {
      numHosts = testHosts.size();
    }
    for (int i = 0; i < numHosts; i++) {
      hosts.add(testHosts.get(i));
    }
    return hosts;
  }

  @Test
  public void testFailover_timeout() throws SQLException {
    // original host list: [active writer, active reader, current connection (reader), active
    // reader, down reader, active reader]
    // priority order by index (the subsets will be shuffled): [[1, 3, 5], 0, [2, 4]]
    // connection attempts are made in pairs using the above list
    // expected test result: failure to get reader since process is limited to 5s and each attempt to connect takes 20s
    final ITopologyService mockTopologyService = Mockito.mock(ITopologyService.class);
    final IConnectionProvider mockConnProvider = Mockito.mock(IConnectionProvider.class);
    final ConnectionImpl mockConnection = Mockito.mock(ConnectionImpl.class);
    final List<HostInfo> hosts = getHostsFromTestUrls(6);
    final int currentHostIndex = 2;
    for (HostInfo host : hosts) {
      when(mockConnProvider.connect(refEq(host)))
          .thenAnswer((Answer<ConnectionImpl>) invocation -> {
            Thread.sleep(20000);
            return mockConnection;
          });
    }

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(hosts.get(hostIndex).getHostPortPair());
    }
    when(mockTopologyService.getDownHosts()).thenReturn(downHosts);

    final IReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockTopologyService,
            mockConnProvider,
            testConnectionProps,
            5000,
            30000,
            mockLog);
    final ReaderFailoverResult result =
        target.failover(hosts, hosts.get(currentHostIndex));

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertEquals(
        FailoverConnectionPlugin.NO_CONNECTION_INDEX,
        result.getConnectionIndex());
  }

  @Test
  public void testFailover_nullOrEmptyHostList() throws SQLException {
    final ITopologyService mockTopologyService = Mockito.mock(ITopologyService.class);
    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockTopologyService,
            Mockito.mock(IConnectionProvider.class),
            testConnectionProps,
            mockLog);
    final HostInfo currentHost = new HostInfo(null, "writer", 1234, null, null);

    ReaderFailoverResult result = target.failover(null, currentHost);
    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertEquals(
        FailoverConnectionPlugin.NO_CONNECTION_INDEX,
        result.getConnectionIndex());

    final List<HostInfo> hosts = new ArrayList<>();
    result = target.failover(hosts, currentHost);
    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertEquals(
        FailoverConnectionPlugin.NO_CONNECTION_INDEX,
        result.getConnectionIndex());
  }

  @Test
  public void testGetReader_connectionSuccess() throws SQLException {
    // even number of connection attempts
    // first connection attempt to return succeeds, second attempt cancelled
    // expected test result: successful connection for host at index 2
    final ITopologyService mockTopologyService = Mockito.mock(ITopologyService.class);
    when(mockTopologyService.getDownHosts()).thenReturn(new HashSet<>());
    final ConnectionImpl mockConnection = Mockito.mock(ConnectionImpl.class);
    final List<HostInfo> hosts =
        getHostsFromTestUrls(3); // 2 connection attempts (writer not attempted)
    final HostInfo slowHost = hosts.get(1);
    final HostInfo fastHost = hosts.get(2);
    final IConnectionProvider mockConnProvider = Mockito.mock(IConnectionProvider.class);
    when(mockConnProvider.connect(refEq(slowHost)))
        .thenAnswer(
            (Answer<ConnectionImpl>)
                invocation -> {
                  Thread.sleep(20000);
                  return mockConnection;
                });
    when(mockConnProvider.connect(refEq(fastHost))).thenReturn(mockConnection);

    final IReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockTopologyService,
            mockConnProvider,
            testConnectionProps,
            mockLog);
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertTrue(result.isConnected());
    assertSame(mockConnection, result.getConnection());
    assertEquals(2, result.getConnectionIndex());

    verify(mockTopologyService, never()).addToDownHostList(any());
    verify(mockTopologyService, times(1)).removeFromDownHostList(eq(fastHost));
  }

  @Test
  public void testGetReader_connectionFailure() throws SQLException {
    // odd number of connection attempts
    // first connection attempt to return fails
    // expected test result: failure to get reader
    final ITopologyService mockTopologyService = Mockito.mock(ITopologyService.class);
    when(mockTopologyService.getDownHosts()).thenReturn(new HashSet<>());
    final IConnectionProvider mockConnProvider = Mockito.mock(IConnectionProvider.class);
    final List<HostInfo> hosts =
        getHostsFromTestUrls(4); // 3 connection attempts (writer not attempted)
    when(mockConnProvider.connect(any())).thenThrow(new SQLException());

    final int currentHostIndex = 2;

    final IReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockTopologyService,
            mockConnProvider,
            testConnectionProps,
            mockLog);
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertEquals(
        FailoverConnectionPlugin.NO_CONNECTION_INDEX,
        result.getConnectionIndex());

    final HostInfo currentHost = hosts.get(currentHostIndex);
    verify(mockTopologyService, atLeastOnce()).addToDownHostList(eq(currentHost));
    verify(mockTopologyService, never())
        .addToDownHostList(
            eq(hosts.get(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX)));
  }

  @Test
  public void testGetReader_connectionAttemptsTimeout() throws SQLException {
    // connection attempts time out before they can succeed
    // first connection attempt to return times out
    // expected test result: failure to get reader
    final ITopologyService mockTopologyService = Mockito.mock(ITopologyService.class);
    when(mockTopologyService.getDownHosts()).thenReturn(new HashSet<>());
    final IConnectionProvider mockProvider = Mockito.mock(IConnectionProvider.class);
    final ConnectionImpl mockConnection = Mockito.mock(ConnectionImpl.class);
    final List<HostInfo> hosts =
        getHostsFromTestUrls(3); // 2 connection attempts (writer not attempted)
    when(mockProvider.connect(any()))
        .thenAnswer(
            (Answer<ConnectionImpl>)
                invocation -> {
                  try {
                    Thread.sleep(5000);
                  } catch (InterruptedException exception) {
                    // ignore
                  }
                  return mockConnection;
                });

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockTopologyService,
            mockProvider,
            testConnectionProps,
            60000,
            1000,
            mockLog);
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertEquals(
        FailoverConnectionPlugin.NO_CONNECTION_INDEX,
        result.getConnectionIndex());

    verify(mockTopologyService, never()).addToDownHostList(any());
  }

  @Test
  public void testGetHostTuplesByPriority() {
    final List<HostInfo> originalHosts = getHostsFromTestUrls(6);

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4, 5);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(originalHosts.get(hostIndex).getHostPortPair());
    }

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            Mockito.mock(ITopologyService.class),
            Mockito.mock(IConnectionProvider.class),
            testConnectionProps,
            mockLog);
    final List<ClusterAwareReaderFailoverHandler.HostTuple> tuplesByPriority =
        target.getHostTuplesByPriority(originalHosts, downHosts);

    final int activeReaderOriginalIndex = 1;
    final int downReaderOriginalIndex = 5;

    // get new positions of active reader, writer, down reader in tuplesByPriority
    final int activeReaderTupleIndex =
        getHostTupleIndexFromOriginalIndex(activeReaderOriginalIndex, tuplesByPriority);
    final int writerTupleIndex =
        getHostTupleIndexFromOriginalIndex(
            FailoverConnectionPlugin.WRITER_CONNECTION_INDEX, tuplesByPriority);
    final int downReaderTupleIndex =
        getHostTupleIndexFromOriginalIndex(downReaderOriginalIndex, tuplesByPriority);

    // assert the following priority ordering: active readers, writer, down readers
    final int numActiveReaders = 2;
    assertTrue(writerTupleIndex > activeReaderTupleIndex);
    assertEquals(numActiveReaders, writerTupleIndex);
    assertTrue(downReaderTupleIndex > writerTupleIndex);
    assertEquals(6, tuplesByPriority.size());
  }

  private int getHostTupleIndexFromOriginalIndex(
      int originalIndex, List<ClusterAwareReaderFailoverHandler.HostTuple> tuples) {
    for (int i = 0; i < tuples.size(); i++) {
      ClusterAwareReaderFailoverHandler.HostTuple tuple = tuples.get(i);
      if (tuple.getIndex() == originalIndex) {
        return i;
      }
    }
    return -1;
  }

  @Test
  public void testGetReaderTuplesByPriority() {
    final List<HostInfo> originalHosts = getHostsFromTestUrls(6);

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4, 5);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(originalHosts.get(hostIndex).getHostPortPair());
    }

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            Mockito.mock(ITopologyService.class),
            Mockito.mock(IConnectionProvider.class),
            testConnectionProps,
            mockLog);
    final List<ClusterAwareReaderFailoverHandler.HostTuple> readerTuples =
        target.getReaderTuplesByPriority(originalHosts, downHosts);

    final int activeReaderOriginalIndex = 1;
    final int downReaderOriginalIndex = 5;

    // get new positions of active reader, down reader in readerTuples
    final int activeReaderTupleIndex =
        getHostTupleIndexFromOriginalIndex(activeReaderOriginalIndex, readerTuples);
    final int downReaderTupleIndex =
        getHostTupleIndexFromOriginalIndex(downReaderOriginalIndex, readerTuples);

    // assert the following priority ordering: active readers, down readers
    final int numActiveReaders = 2;
    final ClusterAwareReaderFailoverHandler.HostTuple writerTuple =
        new ClusterAwareReaderFailoverHandler.HostTuple(
            originalHosts.get(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX),
            FailoverConnectionPlugin.WRITER_CONNECTION_INDEX);
    assertTrue(downReaderTupleIndex > activeReaderTupleIndex);
    assertTrue(downReaderTupleIndex >= numActiveReaders);
    assertFalse(readerTuples.contains(writerTuple));
    assertEquals(5, readerTuples.size());
  }
}
