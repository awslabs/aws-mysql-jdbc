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
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.log.Log;
import org.jboss.util.NullArgumentException;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class NodeMonitoringFailoverPlugin implements IFailoverPlugin {

  protected static int CHECK_INTERVAL_MILLIS = 1000;
  protected static String METHODS_TO_MONITOR = "executeQuery,";

  protected IFailoverPlugin next;
  protected Log log;
  protected PropertySet propertySet;
  protected HostInfo hostInfo;
  protected boolean isEnabled = true;
  protected int failureDetectionTimeMillis;
  protected int failureDetectionIntervalMillis;
  protected int failureDetectionCount;
  private IMonitorService monitorService;
  private MonitorConnectionContext monitorContext;
  private String node;

  @FunctionalInterface
  interface MonitorServiceInitializer {
    IMonitorService start(HostInfo hostInfo, PropertySet propertySet, Log log);
  }

  public NodeMonitoringFailoverPlugin() {
  }

  @Override
  public void init(
      PropertySet propertySet,
      HostInfo hostInfo,
      IFailoverPlugin next,
      Log log) {
    this.init(
        propertySet,
        hostInfo,
        next,
        log,
        DefaultMonitorService::new);
  }

  public void init(
      PropertySet propertySet,
      HostInfo hostInfo,
      IFailoverPlugin next,
      Log log,
      MonitorServiceInitializer monitorServiceInitializer) {
    if (next == null) {
      throw new NullArgumentException("next");
    }

    if (log == null) {
      throw new NullArgumentException("log");
    }

    if (propertySet == null) {
      throw new NullArgumentException("propertySet");
    }

    if (hostInfo == null) {
      throw new NullArgumentException("hostInfo");
    }

    this.hostInfo = hostInfo;
    this.node = hostInfo.getHost();
    this.propertySet = propertySet;
    this.log = log;
    this.next = next;

    this.isEnabled = this.propertySet
        .getBooleanProperty(PropertyKey.nativeFailureDetectionEnabled)
        .getValue();
    this.failureDetectionTimeMillis = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionTime)
        .getValue();
    this.failureDetectionIntervalMillis = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionInterval)
        .getValue();
    this.failureDetectionCount = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionCount)
        .getValue();

    if (this.isEnabled) {
      this.monitorService = monitorServiceInitializer.start(
          this.hostInfo,
          this.propertySet,
          this.log);
    }
  }

  @Override
  public Object execute(String methodName, Callable executeSqlFunc) throws Exception {
    boolean needMonitoring = METHODS_TO_MONITOR.contains(methodName + ",");

    if (!this.isEnabled
        || !needMonitoring
        || this.monitorService == null) {
      // do direct call
      return this.next.execute(methodName, executeSqlFunc);
    }

    // update config settings since they may change
    this.isEnabled = this.propertySet
        .getBooleanProperty(PropertyKey.nativeFailureDetectionEnabled)
        .getValue();
    this.failureDetectionTimeMillis = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionTime)
        .getValue();
    this.failureDetectionIntervalMillis = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionInterval)
        .getValue();
    this.failureDetectionCount = this.propertySet
        .getIntegerProperty(PropertyKey.failureDetectionCount)
        .getValue();

    // use a separate thread to execute method

    Object result;
    ExecutorService executor = null;
    try {
      this.log.logTrace(String.format(
          "[NodeMonitoringFailoverPlugin.execute]: method=%s, monitoring is activated",
          methodName));

      this.monitorContext = this.monitorService.startMonitoring(node,
          this.failureDetectionTimeMillis,
          this.failureDetectionIntervalMillis,
          this.failureDetectionCount);

      Future<Object> executeFuncFuture;
      executor = Executors.newSingleThreadExecutor();
      executeFuncFuture = executor.submit(() -> this.next.execute(methodName, executeSqlFunc));
      executor.shutdown(); // stop executor to accept new tasks

      boolean isDone = executeFuncFuture.isDone();
      while (!isDone) {
        TimeUnit.MILLISECONDS.sleep(CHECK_INTERVAL_MILLIS);
        isDone = executeFuncFuture.isDone();

        if (this.monitorContext.isNodeUnhealthy()) {
          //throw new SocketTimeoutException("Read time out");
          throw new CJCommunicationsException("Node is unavailable.");
        }
      }

      result = executeFuncFuture.get();
    } catch (Exception ex) {
      throw ex;
    } finally {
      // TODO: double check this
      this.monitorService.stopMonitoring(node, this.monitorContext);
      if (executor != null) {
        executor.shutdownNow();
      }
      this.log.logTrace(String.format(
          "[NodeMonitoringFailoverPlugin.execute]: method=%s, monitoring is deactivated",
          methodName));
    }

    return result;
  }

  @Override
  public void releaseResources() {
    this.next.releaseResources();
  }
}
