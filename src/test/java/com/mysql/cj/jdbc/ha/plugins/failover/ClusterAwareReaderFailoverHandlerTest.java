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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.ha.plugins.IConnectionProvider;
import com.mysql.cj.jdbc.ha.plugins.failover.ClusterAwareReaderFailoverHandler.HostTuple;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    final List<ClusterAwareTestUtils.HostInfoMatcher> hostMatchers = new ArrayList<>();
    final int currentHostIndex = 2;
    final int successHostIndex = 4;
    for (int i = 0; i < hosts.size(); i++) {
      ClusterAwareTestUtils.HostInfoMatcher currentMatcher = new ClusterAwareTestUtils.HostInfoMatcher(hosts.get(i));
      hostMatchers.add(currentMatcher);
      if (i != successHostIndex) {
        doThrow(new SQLException("exception", "08S01", null)).when(mockConnProvider).connect(argThat(currentMatcher));
      } else {
        doReturn(mockConnection).when(mockConnProvider).connect(argThat(currentMatcher));
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
    ClusterAwareTestUtils.HostInfoMatcher successHostMatcher = new ClusterAwareTestUtils.HostInfoMatcher(successHost);
    verify(mockTopologyService, atLeast(4)).addToDownHostList(any());
    verify(mockTopologyService, never()).addToDownHostList(eq(successHost));
    verify(mockTopologyService, times(1)).removeFromDownHostList(argThat(successHostMatcher));
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
            false,
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
    ClusterAwareTestUtils.HostInfoMatcher fastHostMatcher = new ClusterAwareTestUtils.HostInfoMatcher(fastHost);
    verify(mockTopologyService, times(1)).removeFromDownHostList(argThat(fastHostMatcher));
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
    when(mockConnProvider.connect(any())).thenThrow(new SQLException("exception", "08S01", null));

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
    ClusterAwareTestUtils.HostInfoMatcher currentHostMatcher = new ClusterAwareTestUtils.HostInfoMatcher(currentHost);
    verify(mockTopologyService, atLeastOnce()).addToDownHostList(argThat(currentHostMatcher));
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
            false,
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

  @Test
  public void testHostFailoverStrictReaderEnabled() {
    final ITopologyService mockTopologyService = Mockito.mock(ITopologyService.class);
    final IConnectionProvider mockConnProvider = Mockito.mock(IConnectionProvider.class);
    final List<HostInfo> hosts = getHostsFromTestUrls(2);
    final HostInfo writer = hosts.get(0);
    final HostInfo reader = hosts.get(1);

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockTopologyService,
            mockConnProvider,
            testConnectionProps,
            5000,
            30000,
            true,
            mockLog);

    // We expect only reader nodes to be chosen.
    List<HostTuple> expectedReaderHost = Collections.singletonList(
        new ClusterAwareReaderFailoverHandler.HostTuple(
            reader,
            1));

    List<HostTuple> hostsByPriority = target.getHostTuplesByPriority(hosts, Collections.emptySet());
    assertEquals(expectedReaderHost, hostsByPriority);

    // Should pick reader even if unavailable.
    hostsByPriority = target.getHostTuplesByPriority(hosts, new HashSet<>(Collections.singletonList(reader.getHost())));
    assertEquals(expectedReaderHost, hostsByPriority);

    // Writer node will only be picked if it is the only node in topology;
    List<HostTuple> expectedWriterHost = Collections.singletonList(
        new ClusterAwareReaderFailoverHandler.HostTuple(
            writer,
            0));
    hostsByPriority = target.getHostTuplesByPriority(Collections.singletonList(writer), Collections.emptySet());
    assertEquals(expectedWriterHost, hostsByPriority);
  }
}
