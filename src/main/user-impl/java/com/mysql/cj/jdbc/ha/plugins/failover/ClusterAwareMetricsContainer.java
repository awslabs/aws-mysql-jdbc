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
