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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Monitor implements IMonitor {
  private static final int THREAD_SLEEP_WHEN_INACTIVE_MILLIS = 100;

  // TODO: lighter implementation
  private final Queue<MonitorConfig> configQueue = new ConcurrentLinkedQueue<>();

  private final Log log;
  private final PropertySet propertySet;
  private final HostInfo hostInfo;
  private final boolean isMonitoring;
  private long monitoringStartTime;
  private final int failureCount;
  private Connection monitoringConn = null;
  private int longestFailureDetectionIntervalMillis;
  private final AtomicLong elapsedTime = new AtomicLong();

  public Monitor(HostInfo hostInfo, PropertySet propertySet, Log log) {
    this.hostInfo = hostInfo;
    this.propertySet = propertySet;
    this.log = log;

    this.isMonitoring = false;
    this.failureCount = 0;
  }

  @Override
  public void startMonitoring(MonitorConfig config) {
    this.longestFailureDetectionIntervalMillis = Math.min(
        this.longestFailureDetectionIntervalMillis,
        config.getFailureDetectionIntervalMillis());

    this.monitoringStartTime = System.currentTimeMillis();
    configQueue.add(config);
  }

  @Override
  public void stopMonitoring(MonitorConfig config) {
    this.longestFailureDetectionIntervalMillis = findLongestIntervalMillis();
    configQueue.remove(config);
  }

  @Override
  public boolean isNodeUnhealthy(MonitorConfig config) {
    if (!config.isValid()) {
      config.incrementFailureCount();

      if (this.failureCount >= config.getFailureDetectionCount()) {
        this.log.logTrace(
            String.format(
                "[NodeMonitoringFailoverPlugin::Monitor] node '%s' is *dead*.",
                this.hostInfo.getHost()));
        return true;
      }
      this.log.logTrace(String.format(
          "[NodeMonitoringFailoverPlugin::Monitor] node '%s' is not *responding* (%d).",
          this.hostInfo.getHost(), this.failureCount));
    } else {
      config.setFailureCount(0);
    }

    this.log.logTrace(
        String.format("[NodeMonitoringFailoverPlugin::Monitor] node '%s' is *alive*.",
            this.hostInfo.getHost()));
    return false;
  }

  @Override
  public void run() {
    try {
      while (true) {
        if (!configQueue.isEmpty()) {
          if (!isConnectionHealthy()) {
            this.log.logTrace(
                String.format(
                    "[NodeMonitoringFailoverPlugin::Monitor] node '%s' is *dead* for all connections.",
                    this.hostInfo.getHost()));
            continue;
          }

          for (MonitorConfig monitorConfig : configQueue) {
            long elapsedTimeMillis = System.currentTimeMillis() - this.monitoringStartTime;

            final int failureDetectionTimeMillis = monitorConfig.getFailureDetectionTimeMillis();

            if (this.isMonitoring && elapsedTimeMillis > failureDetectionTimeMillis) {
              final int intervalMillis = monitorConfig.getFailureDetectionIntervalMillis();
              monitorConfig.setValid(intervalMillis >= this.elapsedTime.get());
            }
          }

          TimeUnit.MILLISECONDS.sleep(this.longestFailureDetectionIntervalMillis);
        } else {
          TimeUnit.MILLISECONDS.sleep(THREAD_SLEEP_WHEN_INACTIVE_MILLIS);
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

  private boolean isConnectionHealthy() {
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

      final long start = System.currentTimeMillis();
      final boolean isValid =
          this.monitoringConn.isValid(this.longestFailureDetectionIntervalMillis / 1000);
      this.elapsedTime.set(System.currentTimeMillis() - start);

      // if false then node is considered dead for all monitor configs
      return isValid;

    } catch (SQLException sqlEx) {
      this.log.logTrace("[NodeMonitoringFailoverPlugin::Monitor]", sqlEx);
      return false;
    }
  }

  private HostInfo copy(HostInfo src, Map<String, String> props) {
    return new HostInfo(
        null,
        src.getHost(),
        src.getPort(),
        src.getUser(),
        src.getPassword(),
        src.isPasswordless(),
        props);
  }

  private int findLongestIntervalMillis() {
    final Optional<MonitorConfig> configWithMaxInterval = configQueue
        .parallelStream()
        .max(Comparator.comparing(MonitorConfig::getFailureDetectionIntervalMillis));

    return configWithMaxInterval
        .map(MonitorConfig::getFailureDetectionIntervalMillis)
        .orElse(0);
  }
}
