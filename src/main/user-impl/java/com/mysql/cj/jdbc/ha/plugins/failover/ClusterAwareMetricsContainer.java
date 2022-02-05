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

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ha.plugins.ICurrentConnectionProvider;
import com.mysql.cj.log.Log;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class ClusterAwareMetricsContainer implements IClusterAwareMetricsContainer {
    // ClusterID, Metrics
    private final static Map<String, ClusterAwareMetrics> clusterMetrics = new ConcurrentHashMap<>();
    private final static Map<String, ClusterAwareTimeMetricsHolder> topologyMetrics = new ConcurrentHashMap<>();
    // Instance URL, Metrics
    private final static Map<String, ClusterAwareMetrics> instanceMetrics = new ConcurrentHashMap<>();

    private ICurrentConnectionProvider currentConnectionProvider = null;
    private PropertySet propertySet = null;
    private String clusterId = "[Unknown Id]";

    public ClusterAwareMetricsContainer() {
    }

    public ClusterAwareMetricsContainer(ICurrentConnectionProvider currentConnectionProvider, PropertySet propertySet) {
        this.currentConnectionProvider = currentConnectionProvider;
        this.propertySet = propertySet;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    @Override
    public void registerFailureDetectionTime(long timeMs) {
        register(metrics -> metrics.registerFailureDetectionTime(timeMs));
    }

    @Override
    public void registerWriterFailoverProcedureTime(long timeMs) {
        register(metrics -> metrics.registerWriterFailoverProcedureTime(timeMs));
    }

    @Override
    public void registerReaderFailoverProcedureTime(long timeMs) {
        register(metrics -> metrics.registerReaderFailoverProcedureTime(timeMs));
    }

    @Override
    public void registerFailoverConnects(boolean isHit) {
        register(metrics -> metrics.registerFailoverConnects(isHit));
    }

    @Override
    public void registerInvalidInitialConnection(boolean isHit) {
        register(metrics -> metrics.registerInvalidInitialConnection(isHit));
    }

    @Override
    public void registerUseLastConnectedReader(boolean isHit) {
        register(metrics -> metrics.registerUseLastConnectedReader(isHit));
    }

    @Override
    public void registerUseCachedTopology(boolean isHit) {
        register(metrics -> metrics.registerUseCachedTopology(isHit));
    }

    @Override
    public void registerTopologyQueryExecutionTime(long timeMs) {
        if (!isEnabled()) {
            return;
        }

        topologyMetrics.computeIfAbsent(clusterId, k -> new ClusterAwareTimeMetricsHolder("Topology Query"))
            .registerQueryExecutionTime(timeMs);
    }

    protected void register(Consumer<ClusterAwareMetrics> lambda) {
        if (!isEnabled()) {
            return;
        }

        lambda.accept(getClusterMetrics(clusterId));

        if (isInstanceMetricsEnabled()) {
            lambda.accept(getInstanceMetrics(getCurrentConnUrl()));
        }
    }

    private boolean isEnabled() {
        if (propertySet != null) {
            return propertySet.getBooleanProperty(PropertyKey.gatherPerfMetrics.getKeyName()).getValue();
        }
        return false;
    }

    private boolean isInstanceMetricsEnabled() {
        if (propertySet != null) {
            return propertySet.getBooleanProperty(PropertyKey.gatherAdditionalMetricsOnInstance.getKeyName()).getValue();
        }
        return false;
    }

    private ClusterAwareMetrics getClusterMetrics(String key) {
        return clusterMetrics.computeIfAbsent(
            key,
            k -> new ClusterAwareMetrics());
    }

    private ClusterAwareMetrics getInstanceMetrics(String key) {
        return instanceMetrics.computeIfAbsent(
            key,
            k -> new ClusterAwareMetrics());
    }
    
    private String getCurrentConnUrl() {
        String currUrl = "[Unknown Url]";
        if (currentConnectionProvider == null) {
            return currUrl;
        }

        final JdbcConnection currConn = currentConnectionProvider.getCurrentConnection();
        if (currConn != null) {
            currUrl = currConn.getHostPortPair();
        }
        return currUrl;
    }

    public static void reportMetrics(String connUrl, Log log) {
        reportMetrics(connUrl, log, false);
    }

    public static void reportMetrics(String connUrl, Log log, boolean forInstances) {
        final ClusterAwareMetrics metrics = forInstances ? instanceMetrics.get(connUrl) : clusterMetrics.get(connUrl);

        if (metrics != null) {
            StringBuilder logMessage = new StringBuilder(256);

            logMessage.append("** Performance Metrics Report for '")
                .append(connUrl)
                .append("' **\n");
            log.logInfo(logMessage);

            final ClusterAwareTimeMetricsHolder topMetric = topologyMetrics.get(connUrl);
            if (topMetric != null) {
                topMetric.reportMetrics(log);
            }

            metrics.reportMetrics(log);
        } else {
            StringBuilder logMessage = new StringBuilder();
            logMessage.append("** No metrics collected for '")
                .append(connUrl)
                .append("' **\n");
            log.logInfo(logMessage);
        }
    }

    public static void resetMetrics() {
        clusterMetrics.clear();
        topologyMetrics.clear();
        instanceMetrics.clear();        
    }
}
