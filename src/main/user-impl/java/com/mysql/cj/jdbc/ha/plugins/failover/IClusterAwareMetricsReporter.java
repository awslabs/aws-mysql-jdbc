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

import com.mysql.cj.log.Log;
import com.mysql.cj.util.StringUtils;

public interface IClusterAwareMetricsReporter {

    /**
     * Reports collected failover performance metrics at cluster level to a provided logger.
     *
     * @param connUrl the connection URL to report.
     * @param log A logger to report collected metric.
     */
    static void reportMetrics(String connUrl, Log log) {
        reportMetrics(connUrl, log, false);
    }

    /**
     * Reports collected failover performance metrics to a provided logger.
     *
     * @param connUrl the connection URL to report.
     * @param log logger to report collected metric.
     * @param atInstance whether to print instance or cluster performance metrics.
     */
    static void reportMetrics(String connUrl, Log log, boolean atInstance) {
        if (StringUtils.isNullOrEmpty(connUrl)) {
            return;
        }

        ClusterAwareMetricsContainer.reportMetrics(connUrl, log, atInstance);
    }

    /**
     * Resets all collected failover performance metrics
     */
    static void resetMetrics() {
        ClusterAwareMetricsContainer.resetMetrics();
    }
}
