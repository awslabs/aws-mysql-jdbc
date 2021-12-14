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

package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.IntegerProperty;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Multi-threaded tests for {@link MultiThreadedDefaultMonitorServiceTest}.
 * Repeats each testcase multiple times.
 * Use a cyclic barrier to ensure threads start at the same time.
 */
class MultiThreadedDefaultMonitorServiceTest {
  @Mock IMonitorInitializer monitorInitializer;
  @Mock IExecutorServiceInitializer executorServiceInitializer;
  @Mock ExecutorService service;
  @Mock Future<?> taskA;
  @Mock HostInfo info;
  @Mock IMonitor monitor;
  @Mock PropertySet propertySet;
  @Mock IntegerProperty integerProperty;
  @Mock JdbcConnection connection;

  private final Log logger = new NullLogger("MultiThreadedDefaultMonitorServiceTest");
  private final AtomicInteger counter = new AtomicInteger(0);
  private final AtomicInteger concurrentCounter = new AtomicInteger(0);

  private static final Map<String, AtomicBoolean> CONCURRENT_TEST_MAP = new ConcurrentHashMap<>();
  private static final int FAILURE_DETECTION_TIME = 10;
  private static final int FAILURE_DETECTION_INTERVAL = 100;
  private static final int FAILURE_DETECTION_COUNT = 3;
  private static final int MONITOR_DISPOSE_TIME = 60000;
  private static final String UNEXPECTED_EXCEPTION = "Test thread interrupted due to an unexpected exception.";

  private AutoCloseable closeable;
  private ArgumentCaptor<MonitorConnectionContext> startMonitoringCaptor;
  private ArgumentCaptor<MonitorConnectionContext> stopMonitoringCaptor;
  private MonitorThreadContainer monitorThreadContainer;

  @BeforeEach
  void init(TestInfo testInfo) {
    closeable = MockitoAnnotations.openMocks(this);
    startMonitoringCaptor = ArgumentCaptor.forClass(MonitorConnectionContext.class);
    stopMonitoringCaptor = ArgumentCaptor.forClass(MonitorConnectionContext.class);
    monitorThreadContainer = MonitorThreadContainer.getInstance();

    CONCURRENT_TEST_MAP.computeIfAbsent(
        testInfo.getDisplayName(),
        k -> new AtomicBoolean(false));

    when(monitorInitializer.createMonitor(
        any(HostInfo.class),
        any(PropertySet.class),
        any(IMonitorService.class)))
        .thenReturn(monitor);
    when(executorServiceInitializer.createExecutorService()).thenReturn(service);
    doReturn(taskA).when(service).submit(any(Monitor.class));
    doNothing().when(monitor).startMonitoring(startMonitoringCaptor.capture());
    doNothing().when(monitor).stopMonitoring(stopMonitoringCaptor.capture());
    when(propertySet.getIntegerProperty(any(String.class))).thenReturn(integerProperty);
    when(integerProperty.getValue()).thenReturn(MONITOR_DISPOSE_TIME);
  }

  @AfterEach
  void cleanUp(TestInfo testInfo) throws Exception {
    counter.set(0);

    if (concurrentCounter.get() > 0) {
      CONCURRENT_TEST_MAP.get(testInfo.getDisplayName()).getAndSet(true);
    }

    concurrentCounter.set(0);
    closeable.close();
    MonitorThreadContainer.releaseInstance();
  }

  /**
   * Ensure each test case was executed concurrently at least once.
   */
  @AfterAll
  static void assertConcurrency() {
    CONCURRENT_TEST_MAP.forEach((key, value) -> assertTrue(
        value.get(),
        String.format("Test '%s' was executed sequentially.", key)));
  }

  @RepeatedTest(value = 1000, name = "start monitoring with multiple connections to different nodes")
  void test_1_startMonitoring_multipleConnectionsToDifferentNodes()
      throws ExecutionException, InterruptedException {
    final int numConnections = 10;
    final List<Set<String>> nodeKeyList = generateNodeKeys(numConnections, true);
    final List<DefaultMonitorService> services = generateServices(numConnections);

    try {
      final List<MonitorConnectionContext> contexts = runStartMonitor(
          numConnections,
          services,
          nodeKeyList);

      final List<MonitorConnectionContext> capturedContexts = startMonitoringCaptor.getAllValues();

      assertEquals(numConnections, services.get(0).threadContainer.getMonitorMap().size());
      assertTrue((contexts.size() == capturedContexts.size())
          && contexts.containsAll(capturedContexts)
          && capturedContexts.containsAll(contexts));
      verify(monitorInitializer, times(numConnections)).createMonitor(eq(info), eq(propertySet), any(IMonitorService.class));
    } finally {
      releaseResources(services);
    }
  }

  @RepeatedTest(value = 1000, name = "start monitoring with multiple connections to the same node")
  void test_2_startMonitoring_multipleConnectionsToOneNode() throws InterruptedException, ExecutionException {
    final int numConnections = 10;
    final List<Set<String>> nodeKeyList = generateNodeKeys(numConnections, false);
    final List<DefaultMonitorService> services = generateServices(numConnections);

    try {
      final List<MonitorConnectionContext> contexts = runStartMonitor(
          numConnections,
          services,
          nodeKeyList);

      final List<MonitorConnectionContext> capturedContexts = startMonitoringCaptor.getAllValues();

      assertEquals(1, services.get(0).threadContainer.getMonitorMap().size());
      assertTrue((contexts.size() == capturedContexts.size())
          && contexts.containsAll(capturedContexts)
          && capturedContexts.containsAll(contexts));

      verify(monitorInitializer).createMonitor(eq(info), eq(propertySet), any(IMonitorService.class));
    } finally {
      releaseResources(services);
    }
  }

  @RepeatedTest(value = 1000, name = "stop monitoring with multiple connections to different nodes")
  void test_3_stopMonitoring_multipleConnectionsToDifferentNodes()
      throws ExecutionException, InterruptedException {
    final int numConnections = 10;
    final List<MonitorConnectionContext> contexts = generateContexts(numConnections, true);
    final List<DefaultMonitorService> services = generateServices(numConnections);

    try {
      runStopMonitor(numConnections, services, contexts);

      final List<MonitorConnectionContext> capturedContexts = stopMonitoringCaptor.getAllValues();
      assertTrue((contexts.size() == capturedContexts.size())
          && contexts.containsAll(capturedContexts)
          && capturedContexts.containsAll(contexts));
    } finally {
      releaseResources(services);
    }
  }

  @RepeatedTest(value = 1000, name = "stop monitoring with multiple connections to the same node")
  void test_4_stopMonitoring_multipleConnectionsToTheSameNode()
      throws ExecutionException, InterruptedException {
    final int numConnections = 10;
    final List<MonitorConnectionContext> contexts = generateContexts(numConnections, false);
    final List<DefaultMonitorService> services = generateServices(numConnections);

    try {
      runStopMonitor(numConnections, services, contexts);

      final List<MonitorConnectionContext> capturedContexts = stopMonitoringCaptor.getAllValues();
      assertTrue((contexts.size() == capturedContexts.size())
          && contexts.containsAll(capturedContexts)
          && capturedContexts.containsAll(contexts));
    } finally {
      releaseResources(services);
    }
  }

  /**
   * Run {@link DefaultMonitorService#startMonitoring(Set, HostInfo, PropertySet, int, int, int)}
   * concurrently in multiple threads.
   * A {@link CountDownLatch} is used to ensure all threads start at the same time.
   *
   * @param numThreads   The number of threads to create.
   * @param services     The services to run in each thread.
   * @param nodeKeysList The set of nodes assigned to each service.
   * @return the results from executing the method.
   * @throws InterruptedException if a thread has been interrupted.
   * @throws ExecutionException   if an exception occurred within a thread.
   */
  private List<MonitorConnectionContext> runStartMonitor(
      final int numThreads,
      final List<DefaultMonitorService> services,
      final List<Set<String>> nodeKeysList
  ) throws InterruptedException, ExecutionException {
    final CountDownLatch latch = new CountDownLatch(1);
    final List<CompletableFuture<MonitorConnectionContext>> threads = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      final DefaultMonitorService service = services.get(i);
      final Set<String> nodeKeys = nodeKeysList.get(i);

      threads.add(CompletableFuture.supplyAsync(() -> {
        try {
          // Wait until each thread is ready to start running.
          latch.await();
        } catch (final InterruptedException e) {
          fail(UNEXPECTED_EXCEPTION, e);
        }

        // Execute the method.
        final int val = counter.getAndIncrement();
        if (val != 0) {
          concurrentCounter.getAndIncrement();
        }

        final MonitorConnectionContext context = service.startMonitoring(
                connection,
                nodeKeys,
                info,
                propertySet,
                FAILURE_DETECTION_TIME,
                FAILURE_DETECTION_INTERVAL,
                FAILURE_DETECTION_COUNT);

        counter.getAndDecrement();
        return context;
      }));
    }

    // Start all threads.
    latch.countDown();

    final List<MonitorConnectionContext> contexts = new ArrayList<>();
    for (final CompletableFuture<MonitorConnectionContext> thread : threads) {
      contexts.add(thread.get());
    }

    return contexts;
  }

  /**
   * Run {@link DefaultMonitorService#stopMonitoring(MonitorConnectionContext)}
   * concurrently in multiple threads.
   * A {@link CountDownLatch} is used to ensure all threads start at the same time.
   *
   * @param numThreads The number of threads to create.
   * @param services   The services to run in each thread.
   * @param contexts   The context for each connection wanting to stop monitoring.
   * @throws InterruptedException if a thread has been interrupted.
   * @throws ExecutionException   if an exception occurred within a thread.
   */
  private void runStopMonitor(
      final int numThreads,
      final List<DefaultMonitorService> services,
      final List<MonitorConnectionContext> contexts)
      throws ExecutionException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    final List<CompletableFuture<Void>> threads = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      final DefaultMonitorService service = services.get(i);
      final MonitorConnectionContext context = contexts.get(i);

      threads.add(CompletableFuture.runAsync(() -> {
        try {
          // Wait until each thread is ready to start running.
          latch.await();
        } catch (final InterruptedException e) {
          fail(UNEXPECTED_EXCEPTION, e);
        }

        // Execute the method.
        final int val = counter.getAndIncrement();
        if (val != 0) {
          concurrentCounter.getAndIncrement();
        }

        service.stopMonitoring(context);
        counter.getAndDecrement();
      }));
    }

    // Start all threads.
    latch.countDown();

    for (final CompletableFuture<Void> thread : threads) {
      thread.get();
    }
  }

  /**
   * Generate multiple sets of node keys pointing to either different nodes or the same node.
   *
   * @param numNodeKeys The amount of sets to create.
   * @param diffNode    Whether the node keys refer to different Aurora cluster nodes.
   * @return the sets of node keys.
   */
  private List<Set<String>> generateNodeKeys(final int numNodeKeys, final boolean diffNode) {
    final Set<String> singleNode = new HashSet<>(Collections.singletonList("node"));

    final List<Set<String>> nodeKeysList = new ArrayList<>();
    final Function<Integer, Set<String>> generateNodeKeysFunc = diffNode
        ? (i) -> new HashSet<>(Collections.singletonList(String.format("node%d", i)))
        : (i) -> singleNode;

    for (int i = 0; i < numNodeKeys; i++) {
      nodeKeysList.add(generateNodeKeysFunc.apply(i));
    }

    return nodeKeysList;
  }

  /**
   * Generate multiple contexts with either different node keys or the same node keys,
   * and add the contexts to the monitor thread container.
   *
   * @param numContexts The amount of contexts to create.
   * @param diffContext Whether the contexts have the same set of node keys or different sets of node keys.
   * @return the generated contexts.
   */
  private List<MonitorConnectionContext> generateContexts(final int numContexts, final boolean diffContext) {
    final List<Set<String>> nodeKeysList = generateNodeKeys(numContexts, diffContext);
    final List<MonitorConnectionContext> contexts = new ArrayList<>();

    nodeKeysList.forEach(nodeKeys -> {
      monitorThreadContainer.getOrCreateMonitor(nodeKeys, () -> monitor);
      contexts.add(new MonitorConnectionContext(
          null,
          nodeKeys,
          logger,
          FAILURE_DETECTION_TIME,
          FAILURE_DETECTION_INTERVAL,
          FAILURE_DETECTION_COUNT));
    });

    return contexts;
  }

  /**
   * Create multiple {@link DefaultMonitorService} objects.
   *
   * @param numServices The number of monitor services to create.
   * @return a list of monitor services.
   */
  private List<DefaultMonitorService> generateServices(final int numServices) {
    final List<DefaultMonitorService> services = new ArrayList<>();
    for (int i = 0; i < numServices; i++) {
      services.add(new DefaultMonitorService(
          monitorInitializer,
          executorServiceInitializer,
          logger));
    }
    return services;
  }

  /**
   * Release any resources used by the given services.
   *
   * @param services The {@link DefaultMonitorService} services to clean.
   */
  private void releaseResources(final List<DefaultMonitorService> services) {
    for (final DefaultMonitorService defaultMonitorService : services) {
      defaultMonitorService.releaseResources();
    }
  }
}
