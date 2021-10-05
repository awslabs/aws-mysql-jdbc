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
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.log.Log;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Monitor implements Runnable {
  protected static int THREAD_SLEEP_WHEN_INACTIVE_MILLIS = 100;

  protected Log log;
  protected PropertySet propertySet;
  protected HostInfo hostInfo;
  protected boolean isMonitoring;
  protected long monitoringStartTime;
  protected boolean isNodeUnhealthy;
  protected int failureDetectionCount;
  protected int failureCount;
  protected Connection monitoringConn = null;
  private final IMonitorService monitorService;
  private final String node;

  public Monitor(IMonitorService monitorService, HostInfo hostInfo, PropertySet propertySet, Log log) {
    this.monitorService = monitorService;
    this.hostInfo = hostInfo;
    this.propertySet = propertySet;
    this.log = log;

    this.node = hostInfo.getHost();

    this.isMonitoring = false;
    this.isNodeUnhealthy = false;
    this.failureCount = 0;
  }

  public void startMonitoring(
      int failureDetectionTimeMillis,
      int failureDetectionIntervalMillis,
      int failureDetectionCount) {
    // TODO: do we need locks here or do it earlier?

    final MonitorConfig config =
        new MonitorConfig(
            failureDetectionTimeMillis,
            failureDetectionIntervalMillis,
            failureDetectionCount);

    this.monitorService.addMonitorConfig(this.node, config);

    this.monitoringStartTime = System.currentTimeMillis();
    // TODO: Use Atomics here?
    this.isMonitoring = true;
    this.failureCount = 0;
  }

  public void stopMonitoring() {
    this.isMonitoring = false;
  }

  public boolean isNodeUnhealthy() {
    return this.isNodeUnhealthy;
  }

  protected void updateFlags(boolean isValid, final int failureDetectionCount) {
    if (!isValid) {
      this.failureCount++;
      if (failureCount >= failureDetectionCount) {
        this.isNodeUnhealthy = true;
        this.log.logTrace(
            String.format("[NodeMonitoringFailoverPlugin::Monitor] node '%s' is *dead*.",
                this.hostInfo.getHost()));
      } else {
        this.log.logTrace(String.format(
            "[NodeMonitoringFailoverPlugin::Monitor] node '%s' is not *responding* (%d).",
            this.hostInfo.getHost(), this.failureCount));
      }
    } else {
      this.failureCount = 0;
      this.isNodeUnhealthy = false;
      this.log.logTrace(
          String.format("[NodeMonitoringFailoverPlugin::Monitor] node '%s' is *alive*.",
              this.hostInfo.getHost()));
    }
  }

  protected boolean isConnectionHealthy(final int failureDetectionIntervalMillis) {
    try {
      if (this.monitoringConn == null || this.monitoringConn.isClosed()) {

        // open a new connection
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyKey.tcpKeepAlive.getKeyName(),
            this.propertySet.getBooleanProperty(PropertyKey.tcpKeepAlive).getStringValue());
        properties.put(PropertyKey.connectTimeout.getKeyName(),
            this.propertySet.getBooleanProperty(PropertyKey.connectTimeout).getStringValue());
        //TODO: any other properties to pass? like socket factory

        this.monitoringConn = ConnectionImpl.getInstance(
            copy(this.hostInfo, properties)); //TODO: use connection provider?

        return true;
      }

      return this.monitoringConn.isValid(failureDetectionIntervalMillis / 1000);
    } catch (SQLException sqlEx) {
      this.log.logTrace("[NodeMonitoringFailoverPlugin::Monitor]", sqlEx);
      return false;
    }
  }

  @Override
  public void run() {
    try {

      while (true) {
        final List<MonitorConfig> configList = this.monitorService.getMonitorConfigs(this.node);

        for (MonitorConfig monitorConfig : configList) {
          long elapsedTimeMillis = System.currentTimeMillis() - this.monitoringStartTime;

          final int failureDetectionTimeMillis = monitorConfig.getFailureDetectionTimeMillis();

          if (this.isMonitoring && elapsedTimeMillis > failureDetectionTimeMillis) {

            final int failureDetectionIntervalMillis = monitorConfig.getFailureDetectionIntervalMillis();
            updateFlags(isConnectionHealthy(failureDetectionIntervalMillis), failureDetectionCount);
            TimeUnit.MILLISECONDS.sleep(failureDetectionIntervalMillis);
          } else {
            TimeUnit.MILLISECONDS.sleep(THREAD_SLEEP_WHEN_INACTIVE_MILLIS);
          }
        }

      }
    } catch (InterruptedException intEx) {
      // do nothing; exit thread
    } finally {
      if (this.monitoringConn != null) {
        try {
          this.monitoringConn.close();
        } catch (SQLException ex) {
          //ignore
        }
      }
    }
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
}
