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

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * ClusterAwareWriterFailoverHandlerTest class.
 * */
public class ClusterAwareWriterFailoverHandlerTest {
  final Log mockLog = Mockito.mock(Log.class);

  /**
   * Verify that writer failover handler can re-connect to a current writer node.
   *
   * <p>topology: no changes taskA: successfully re-connect to writer; return new connection taskB:
   * fail to connect to any reader due to exception expected test result: new connection by taskA
   */
  @Test
  public void testReconnectToWriter_taskBReaderException() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final ConnectionImpl mockConnection = Mockito.mock(ConnectionImpl.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo writerHost = createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(refEq(writerHost))).thenReturn(mockConnection);
    when(mockConnectionProvider.connect(refEq(readerA_Host))).thenThrow(SQLException.class);
    when(mockConnectionProvider.connect(refEq(readerB_Host))).thenThrow(SQLException.class);

    when(mockTopologyService.getTopology(any(JdbcConnection.class), eq(true)))
        .thenReturn(currentTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenThrow(SQLException.class);

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            mockReaderFailover,
            new HashMap<>(),
            5000,
            2000,
            2000,
            mockLog);
    final ResolvedHostInfo result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertFalse(result.isNewHost());
    assertSame(result.getNewConnection(), mockConnection);

    final InOrder inOrder = Mockito.inOrder(mockTopologyService);
    inOrder.verify(mockTopologyService).addToDownHostList(refEq(writerHost));
    inOrder.verify(mockTopologyService).removeFromDownHostList(refEq(writerHost));
  }

  private static HostInfo createBasicHostInfo(String instanceName, String db) {
    final Map<String, String> properties = new HashMap<>();
    properties.put(TopologyServicePropertyKeys.INSTANCE_NAME, instanceName);
    String url = "jdbc:mysql:aws://" + instanceName + ".com:1234/";
    db = db == null ? "" : db;
    properties.put(PropertyKey.DBNAME.getKeyName(), db);
    url += db;
    final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    return new HostInfo(conStr, instanceName, 1234, null, null, properties);
  }

  /**
   * Verify that writer failover handler can re-connect to a current writer node.
   *
   * <p>topology: no changes seen by task A, changes to [new-writer, reader-A, reader-B] for taskB
   * taskA: successfully re-connect to initial writer; return new connection taskB: successfully
   * connect to readerA and then new writer but it takes more time than taskA expected test result:
   * new connection by taskA
   */
  @Test
  public void testReconnectToWriter_SlowReaderA() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final ConnectionImpl mockWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderA_Connection = Mockito.mock(ConnectionImpl.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo writerHost = createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = createBasicHostInfo("new-writer-host", "test");
    final List<HostInfo> newTopology = new ArrayList<>();
    newTopology.add(newWriterHost);
    newTopology.add(readerA_Host);
    newTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(refEq(writerHost))).thenReturn(mockWriterConnection);
    when(mockConnectionProvider.connect(refEq(readerB_Host))).thenThrow(SQLException.class);

    when(mockTopologyService.getTopology(eq(mockWriterConnection), eq(true)))
        .thenReturn(currentTopology);
    when(mockTopologyService.getTopology(eq(mockReaderA_Connection), eq(true)))
        .thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenAnswer(
            (Answer<ConnectionAttemptResult>)
                invocation -> {
                  Thread.sleep(5000);
                  return new ConnectionAttemptResult(mockReaderA_Connection, 1, true);
                });

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            mockReaderFailover,
            new HashMap<>(),
            60000,
            5000,
            5000,
            mockLog);
    final ResolvedHostInfo result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertFalse(result.isNewHost());
    assertSame(result.getNewConnection(), mockWriterConnection);

    final InOrder inOrder = Mockito.inOrder(mockTopologyService);
    inOrder.verify(mockTopologyService).addToDownHostList(refEq(writerHost));
    inOrder.verify(mockTopologyService).removeFromDownHostList(refEq(writerHost));
  }

  /**
   * Verify that writer failover handler can re-connect to a current writer node.
   *
   * <p>topology: no changes taskA: successfully re-connect to writer; return new connection taskB:
   * successfully connect to readerA and retrieve topology, but latest writer is not new (defer to
   * taskA) expected test result: new connection by taskA
   */
  @Test
  public void testReconnectToWriter_taskBDefers() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final ConnectionImpl mockWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderA_Connection = Mockito.mock(ConnectionImpl.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo writerHost = createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(refEq(writerHost)))
        .thenAnswer(
            (Answer<ConnectionImpl>)
                invocation -> {
                  Thread.sleep(5000);
                  return mockWriterConnection;
                });
    when(mockConnectionProvider.connect(refEq(readerB_Host))).thenThrow(SQLException.class);

    when(mockTopologyService.getTopology(any(JdbcConnection.class), eq(true)))
        .thenReturn(currentTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ConnectionAttemptResult(mockReaderA_Connection, 1, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            mockReaderFailover,
            new HashMap<>(),
            60000,
            2000,
            2000,
            mockLog);
    final ResolvedHostInfo result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertFalse(result.isNewHost());
    assertSame(result.getNewConnection(), mockWriterConnection);

    final InOrder inOrder = Mockito.inOrder(mockTopologyService);
    inOrder.verify(mockTopologyService).addToDownHostList(refEq(writerHost));
    inOrder.verify(mockTopologyService).removeFromDownHostList(refEq(writerHost));
  }

  /**
   * Verify that writer failover handler can re-connect to a new writer node.
   *
   * <p>topology: changes to [new-writer, reader-A, reader-B] for taskB, taskA sees no changes
   * taskA: successfully re-connect to writer; return connection to initial writer but it takes more
   * time than taskB taskB: successfully connect to readerA and then to new-writer expected test
   * result: new connection to writer by taskB
   */
  @Test
  public void testConnectToReaderA_SlowWriter() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final ConnectionImpl mockWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockNewWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderA_Connection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderB_Connection = Mockito.mock(ConnectionImpl.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo writerHost = createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = createBasicHostInfo("new-writer-host", "test");
    final List<HostInfo> newTopology = new ArrayList<>();
    newTopology.add(newWriterHost);
    newTopology.add(readerA_Host);
    newTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(refEq(writerHost)))
        .thenAnswer(
            (Answer<ConnectionImpl>)
                invocation -> {
                  Thread.sleep(5000);
                  return mockWriterConnection;
                });
    when(mockConnectionProvider.connect(refEq(readerA_Host))).thenReturn(mockReaderA_Connection);
    when(mockConnectionProvider.connect(refEq(readerB_Host))).thenReturn(mockReaderB_Connection);
    when(mockConnectionProvider.connect(refEq(newWriterHost))).thenReturn(mockNewWriterConnection);

    when(mockTopologyService.getTopology(eq(mockWriterConnection), eq(true)))
        .thenReturn(currentTopology);
    when(mockTopologyService.getTopology(eq(mockReaderA_Connection), eq(true)))
        .thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ConnectionAttemptResult(mockReaderA_Connection, 1, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            mockReaderFailover,
            new HashMap<>(),
            60000,
            5000,
            5000,
            mockLog);
    final ResolvedHostInfo result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertTrue(result.isNewHost());
    assertSame(result.getNewConnection(), mockNewWriterConnection);
    assertEquals(3, result.getTopology().size());
    assertEquals("new-writer-host", result.getTopology().get(0).getHost());

    verify(mockTopologyService, times(1)).addToDownHostList(refEq(writerHost));
    verify(mockTopologyService, times(1)).removeFromDownHostList(refEq(newWriterHost));
  }

  /**
   * Verify that writer failover handler can re-connect to a new writer node.
   *
   * <p>topology: changes to [new-writer, initial-writer, reader-A, reader-B] taskA: successfully
   * reconnect, but initial-writer is now a reader (defer to taskB) taskB: successfully connect to
   * readerA and then to new-writer expected test result: new connection to writer by taskB
   */
  @Test
  public void testConnectToReaderA_taskADefers() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final ConnectionImpl mockNewWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderA_Connection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderB_Connection = Mockito.mock(ConnectionImpl.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo initialWriterHost = createBasicHostInfo("initial-writer-host", "test");
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(initialWriterHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = createBasicHostInfo("new-writer-host", "test");
    final List<HostInfo> newTopology = new ArrayList<>();
    newTopology.add(newWriterHost);
    newTopology.add(initialWriterHost);
    newTopology.add(readerA_Host);
    newTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(initialWriterHost))
        .thenReturn(Mockito.mock(ConnectionImpl.class));
    when(mockConnectionProvider.connect(refEq(readerA_Host))).thenReturn(mockReaderA_Connection);
    when(mockConnectionProvider.connect(refEq(readerB_Host))).thenReturn(mockReaderB_Connection);
    when(mockConnectionProvider.connect(refEq(newWriterHost)))
        .thenAnswer(
            (Answer<ConnectionImpl>)
                invocation -> {
                  Thread.sleep(5000);
                  return mockNewWriterConnection;
                });

    when(mockTopologyService.getTopology(any(JdbcConnection.class), eq(true)))
        .thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ConnectionAttemptResult(mockReaderA_Connection, 1, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            mockReaderFailover,
            new HashMap<>(),
            60000,
            5000,
            5000,
            mockLog);
    final ResolvedHostInfo result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertTrue(result.isNewHost());
    assertSame(result.getNewConnection(), mockNewWriterConnection);
    assertEquals(4, result.getTopology().size());
    assertEquals("new-writer-host", result.getTopology().get(0).getHost());

    verify(mockTopologyService, times(1)).addToDownHostList(refEq(initialWriterHost));
    verify(mockTopologyService, times(1)).removeFromDownHostList(refEq(newWriterHost));
  }

  /**
   * Verify that writer failover handler fails to re-connect to any writer node.
   *
   * <p>topology: no changes seen by task A, changes to [new-writer, reader-A, reader-B] for taskB
   * taskA: fail to re-connect to writer due to failover timeout taskB: successfully connect to
   * readerA and then fail to connect to writer due to failover timeout expected test result: no
   * connection
   */
  @Test
  public void testFailedToConnect_failoverTimeout() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final ConnectionImpl mockWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockNewWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderA_Connection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderB_Connection = Mockito.mock(ConnectionImpl.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo writerHost = createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = createBasicHostInfo("new-writer-host", "test");
    final List<HostInfo> newTopology = new ArrayList<>();
    newTopology.add(newWriterHost);
    newTopology.add(readerA_Host);
    newTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(refEq(writerHost)))
        .thenAnswer(
            (Answer<ConnectionImpl>)
                invocation -> {
                  Thread.sleep(30000);
                  return mockWriterConnection;
                });
    when(mockConnectionProvider.connect(refEq(readerA_Host))).thenReturn(mockReaderA_Connection);
    when(mockConnectionProvider.connect(refEq(readerB_Host))).thenReturn(mockReaderB_Connection);
    when(mockConnectionProvider.connect(refEq(newWriterHost)))
        .thenAnswer(
            (Answer<ConnectionImpl>)
                invocation -> {
                  Thread.sleep(30000);
                  return mockNewWriterConnection;
                });

    when(mockTopologyService.getTopology(eq(mockWriterConnection), any(Boolean.class)))
        .thenReturn(currentTopology);
    when(mockTopologyService.getTopology(eq(mockNewWriterConnection), any(Boolean.class)))
        .thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ConnectionAttemptResult(mockReaderA_Connection, 1, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            mockReaderFailover,
            new HashMap<>(),
            5000,
            2000,
            2000,
            mockLog);
    final ResolvedHostInfo result = target.failover(currentTopology);

    assertFalse(result.isConnected());
    assertFalse(result.isNewHost());

    verify(mockTopologyService, times(1)).addToDownHostList(refEq(writerHost));
  }

  /**
   * Verify that writer failover handler fails to re-connect to any writer node.
   *
   * <p>topology: changes to [new-writer, reader-A, reader-B] for taskB taskA: fail to re-connect to
   * writer due to exception taskB: successfully connect to readerA and then fail to connect to
   * writer due to exception expected test result: no connection
   */
  @Test
  public void testFailedToConnect_taskAException_taskBWriterException() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final ConnectionImpl mockReaderA_Connection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderB_Connection = Mockito.mock(ConnectionImpl.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo writerHost = createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = createBasicHostInfo("new-writer-host", "test");
    final List<HostInfo> newTopology = new ArrayList<>();
    newTopology.add(newWriterHost);
    newTopology.add(readerA_Host);
    newTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(refEq(writerHost))).thenThrow(SQLException.class);
    when(mockConnectionProvider.connect(refEq(readerA_Host))).thenReturn(mockReaderA_Connection);
    when(mockConnectionProvider.connect(refEq(readerB_Host))).thenReturn(mockReaderB_Connection);
    when(mockConnectionProvider.connect(refEq(newWriterHost))).thenThrow(SQLException.class);

    when(mockTopologyService.getTopology(any(JdbcConnection.class), any(Boolean.class)))
        .thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ConnectionAttemptResult(mockReaderA_Connection, 1, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            mockReaderFailover,
            new HashMap<>(),
            5000,
            2000,
            2000,
            mockLog);
    final ResolvedHostInfo result = target.failover(currentTopology);

    assertFalse(result.isConnected());
    assertFalse(result.isNewHost());

    verify(mockTopologyService, times(1)).addToDownHostList(refEq(writerHost));
    verify(mockTopologyService, atLeastOnce()).addToDownHostList(refEq(newWriterHost));
  }
}
