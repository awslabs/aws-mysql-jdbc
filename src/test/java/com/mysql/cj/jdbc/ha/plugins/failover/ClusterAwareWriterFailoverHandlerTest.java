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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.plugins.IConnectionProvider;
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
    final IConnectionProvider mockConnectionProvider = Mockito.mock(IConnectionProvider.class);
    final ConnectionImpl mockConnection = Mockito.mock(ConnectionImpl.class);
    final IReaderFailoverHandler mockReaderFailover = Mockito.mock(IReaderFailoverHandler.class);

    final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
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
    final WriterFailoverResult result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertFalse(result.isNewHost());
    assertSame(result.getNewConnection(), mockConnection);

    final InOrder inOrder = Mockito.inOrder(mockTopologyService);
    inOrder.verify(mockTopologyService).addToDownHostList(refEq(writerHost));
    inOrder.verify(mockTopologyService).removeFromDownHostList(refEq(writerHost));
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
    final IConnectionProvider mockConnectionProvider = Mockito.mock(IConnectionProvider.class);
    final ConnectionImpl mockWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderA_Connection = Mockito.mock(ConnectionImpl.class);
    final IReaderFailoverHandler mockReaderFailover = Mockito.mock(IReaderFailoverHandler.class);

    final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = ClusterAwareTestUtils.createBasicHostInfo("new-writer-host", "test");
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
            (Answer<ReaderFailoverResult>)
                invocation -> {
                  Thread.sleep(5000);
                  return new ReaderFailoverResult(mockReaderA_Connection, 1, true);
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
    final WriterFailoverResult result = target.failover(currentTopology);

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
    final IConnectionProvider mockConnectionProvider = Mockito.mock(IConnectionProvider.class);
    final ConnectionImpl mockWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderA_Connection = Mockito.mock(ConnectionImpl.class);
    final IReaderFailoverHandler mockReaderFailover = Mockito.mock(IReaderFailoverHandler.class);

    final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
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
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, 1, true));

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
    final WriterFailoverResult result = target.failover(currentTopology);

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
    final IConnectionProvider mockConnectionProvider = Mockito.mock(IConnectionProvider.class);
    final ConnectionImpl mockWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockNewWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderA_Connection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderB_Connection = Mockito.mock(ConnectionImpl.class);
    final IReaderFailoverHandler mockReaderFailover = Mockito.mock(IReaderFailoverHandler.class);

    final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = ClusterAwareTestUtils.createBasicHostInfo("new-writer-host", "test");
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
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, 1, true));

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
    final WriterFailoverResult result = target.failover(currentTopology);

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
    final IConnectionProvider mockConnectionProvider = Mockito.mock(IConnectionProvider.class);
    final ConnectionImpl mockNewWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderA_Connection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderB_Connection = Mockito.mock(ConnectionImpl.class);
    final IReaderFailoverHandler mockReaderFailover = Mockito.mock(IReaderFailoverHandler.class);

    final HostInfo initialWriterHost = ClusterAwareTestUtils.createBasicHostInfo("initial-writer-host", "test");
    final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(initialWriterHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = ClusterAwareTestUtils.createBasicHostInfo("new-writer-host", "test");
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
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, 1, true));

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
    final WriterFailoverResult result = target.failover(currentTopology);

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
    final IConnectionProvider mockConnectionProvider = Mockito.mock(IConnectionProvider.class);
    final ConnectionImpl mockWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockNewWriterConnection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderA_Connection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderB_Connection = Mockito.mock(ConnectionImpl.class);
    final IReaderFailoverHandler mockReaderFailover = Mockito.mock(IReaderFailoverHandler.class);

    final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = ClusterAwareTestUtils.createBasicHostInfo("new-writer-host", "test");
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
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, 1, true));

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
    final WriterFailoverResult result = target.failover(currentTopology);

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
    final IConnectionProvider mockConnectionProvider = Mockito.mock(IConnectionProvider.class);
    final ConnectionImpl mockReaderA_Connection = Mockito.mock(ConnectionImpl.class);
    final ConnectionImpl mockReaderB_Connection = Mockito.mock(ConnectionImpl.class);
    final IReaderFailoverHandler mockReaderFailover = Mockito.mock(IReaderFailoverHandler.class);

    final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
    final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
    final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = ClusterAwareTestUtils.createBasicHostInfo("new-writer-host", "test");
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
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, 1, true));

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
    final WriterFailoverResult result = target.failover(currentTopology);

    assertFalse(result.isConnected());
    assertFalse(result.isNewHost());

    verify(mockTopologyService, times(1)).addToDownHostList(refEq(writerHost));
    verify(mockTopologyService, atLeastOnce()).addToDownHostList(refEq(newWriterHost));
  }
}
