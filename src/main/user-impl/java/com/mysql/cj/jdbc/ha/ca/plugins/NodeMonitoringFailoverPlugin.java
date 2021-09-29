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

import com.mysql.cj.conf.*;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.log.Log;
import org.jboss.util.NullArgumentException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class NodeMonitoringFailoverPlugin implements IFailoverPlugin {

  protected static int THREAD_SLEEP_WHEN_INACTIVE_MILLIS = 100;
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
  protected Thread monitoringThread = null;
  protected Monitor monitor = null;

  public NodeMonitoringFailoverPlugin() {}

  @Override
  public void init(PropertySet propertySet, HostInfo hostInfo, IFailoverPlugin next, Log log) {
    if(next == null) {
      throw new NullArgumentException("next");
    }

    if(log == null) {
      throw new NullArgumentException("log");
    }

    if(propertySet == null) {
      throw new NullArgumentException("propertySet");
    }

    if(hostInfo == null) {
      throw new NullArgumentException("hostInfo");
    }

    this.hostInfo = hostInfo;
    this.propertySet = propertySet;
    this.log = log;
    this.next = next;

    this.isEnabled = this.propertySet.getBooleanProperty(PropertyKey.nativeFailureDetectionEnabled).getValue();
    this.failureDetectionTimeMillis = this.propertySet.getIntegerProperty(PropertyKey.failureDetectionTime).getValue();
    this.failureDetectionIntervalMillis = this.propertySet.getIntegerProperty(PropertyKey.failureDetectionInterval).getValue();
    this.failureDetectionCount = this.propertySet.getIntegerProperty(PropertyKey.failureDetectionCount).getValue();

    if(!this.isEnabled) {
      return;
    }

    this.monitor = new Monitor(this.hostInfo, this.propertySet, this.log);
    this.monitoringThread = new Thread(this.monitor);
    this.monitoringThread.start();
  }

  @Override
  public Object execute(String methodName, Callable executeSqlFunc) throws Exception {
    boolean needMonitoring = METHODS_TO_MONITOR.contains(methodName + ",");

    if(!this.isEnabled || !needMonitoring || this.monitor == null || this.monitoringThread == null) {
      // do direct call
      return this.next.execute(methodName, executeSqlFunc);
    }

    // update config settings since they may change
    this.isEnabled = this.propertySet.getBooleanProperty(PropertyKey.nativeFailureDetectionEnabled).getValue();
    this.failureDetectionTimeMillis = this.propertySet.getIntegerProperty(PropertyKey.failureDetectionTime).getValue();
    this.failureDetectionIntervalMillis = this.propertySet.getIntegerProperty(PropertyKey.failureDetectionInterval).getValue();
    this.failureDetectionCount = this.propertySet.getIntegerProperty(PropertyKey.failureDetectionCount).getValue();

    // use a separate thread to execute method

    Object result = null;
    ExecutorService executor = null;
    try {

      this.log.logTrace(String.format("[NodeMonitoringFailoverPlugin.execute]: method=%s, monitoring is activated", methodName));
      this.monitor.startMonitoring(this.failureDetectionTimeMillis, this.failureDetectionIntervalMillis, this.failureDetectionCount);

      Future<Object> executeFuncFuture = null;
      executor = Executors.newSingleThreadExecutor();
      executeFuncFuture = executor.submit(() -> this.next.execute(methodName, executeSqlFunc));
      executor.shutdown(); // stop executor to accept new tasks

      boolean isDone = executeFuncFuture.isDone();
      while(!isDone) {
        TimeUnit.MILLISECONDS.sleep(CHECK_INTERVAL_MILLIS);
        isDone = executeFuncFuture.isDone();

        if(this.monitor.isNodeUnhealthy()) {
          //throw new SocketTimeoutException("Read time out");
          throw new CJCommunicationsException("Node is unavailable.");
        }
      }

      result = executeFuncFuture.get();
    }
    catch(Exception ex) {
      throw ex;
    }
    finally {
      this.monitor.stopMonitoring();
      if(executor != null) {
        executor.shutdownNow();
      }
      this.log.logTrace(String.format("[NodeMonitoringFailoverPlugin.execute]: method=%s, monitoring is deactivated", methodName));
    }

    return result;
  }

  @Override
  public void releaseResources() {
    if (this.monitor != null) {
      this.monitor.stopMonitoring();
      this.monitor = null;
    }
    if(this.monitoringThread != null && !this.monitoringThread.isInterrupted()) {
      this.monitoringThread.interrupt();
      this.monitoringThread = null;
    }

    this.next.releaseResources();
  }

  protected HostInfo copy(HostInfo src, Map<String, String> props) {
    return new HostInfo(
            null,
            src.getHost(),
            src.getPort(),
            src.getUser(),
            src.getPassword(),
            src.isPasswordless(),
            props);
  }

  private class Monitor implements Runnable {

    protected Log log;
    protected PropertySet propertySet;
    protected HostInfo hostInfo;
    protected boolean isMonitoring;
    protected long monitoringStartTime;
    protected boolean isNodeUnhealthy;
    protected int failureDetectionTimeMillis;
    protected int failureDetectionIntervalMillis;
    protected int failureDetectionCount;
    protected int failureCount;
    protected Connection monitoringConn = null;

    public Monitor(HostInfo hostInfo, PropertySet propertySet, Log log) {
      this.hostInfo = hostInfo;
      this.propertySet = propertySet;
      this.log = log;

      this.isMonitoring = false;
      this.isNodeUnhealthy = false;
      this.failureCount = 0;
    }

    public void startMonitoring(int failureDetectionTimeMillis, int failureDetectionIntervalMillis, int failureDetectionCount) {
      this.failureDetectionTimeMillis = failureDetectionTimeMillis;
      this.failureDetectionIntervalMillis = failureDetectionIntervalMillis;
      this.failureDetectionCount = failureDetectionCount;

      this.monitoringStartTime = System.currentTimeMillis();
      this.isMonitoring = true;
      this.failureCount = 0;
    }

    public void stopMonitoring() {
      this.isMonitoring = false;
    }

    public boolean isNodeUnhealthy() {
      return this.isNodeUnhealthy;
    }

    protected void updateFlags(boolean isValid) {
      if(!isValid) {
        this.failureCount++;
        if(failureCount >= this.failureDetectionCount) {
          this.isNodeUnhealthy = true;
          this.log.logTrace(String.format("[NodeMonitoringFailoverPlugin::Monitor] node '%s' is *dead*.", this.hostInfo.getHost()));
        }
        else {
          this.log.logTrace(String.format("[NodeMonitoringFailoverPlugin::Monitor] node '%s' is not *responding* (%d).", this.hostInfo.getHost(), this.failureCount));
        }
      }
      else {
        this.failureCount = 0;
        this.isNodeUnhealthy = false;
        this.log.logTrace(String.format("[NodeMonitoringFailoverPlugin::Monitor] node '%s' is *alive*.", this.hostInfo.getHost()));
      }
    }

    protected boolean isConnectionHealthy() {
      try {
        if (this.monitoringConn == null || this.monitoringConn.isClosed()) {

          // open a new connection
          Map<String, String> properties = new HashMap<String, String>();
          properties.put(PropertyKey.tcpKeepAlive.getKeyName(), this.propertySet.getBooleanProperty(PropertyKey.tcpKeepAlive).getStringValue());
          properties.put(PropertyKey.connectTimeout.getKeyName(), this.propertySet.getBooleanProperty(PropertyKey.connectTimeout).getStringValue());
          //TODO: any other properties to pass? like socket factory

          this.monitoringConn = ConnectionImpl.getInstance(copy(this.hostInfo, properties)); //TODO: use connection provider?

          return true;
        }

        return this.monitoringConn.isValid(this.failureDetectionIntervalMillis / 1000);
      }
      catch (SQLException sqlEx) {
        this.log.logTrace("[NodeMonitoringFailoverPlugin::Monitor]", sqlEx);
        return false;
      }
    }

    @Override
    public void run() {

      try {

        while(true) {

          long elapsedTimeMillis = System.currentTimeMillis() - this.monitoringStartTime;

          if (this.isMonitoring && elapsedTimeMillis > this.failureDetectionTimeMillis) {

            updateFlags(isConnectionHealthy());
            TimeUnit.MILLISECONDS.sleep(this.failureDetectionIntervalMillis);
          }
          else {
            TimeUnit.MILLISECONDS.sleep(THREAD_SLEEP_WHEN_INACTIVE_MILLIS);
          }
        }
      }
      catch(InterruptedException intEx) {
        // do nothing; exit thread
      }
      finally {
        if(this.monitoringConn != null) {
          try {
            this.monitoringConn.close();
          }
          catch (SQLException ex) {
            //ignore
          }
        }
      }
    }
  }
}
