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

package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.IpAddressUtils;
import com.mysql.cj.util.StringUtils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mysql.cj.jdbc.ha.plugins.RdsUrl.IP_ADDRESS;
import static com.mysql.cj.jdbc.ha.plugins.RdsUrl.OTHER;
import static com.mysql.cj.jdbc.ha.plugins.RdsUrl.RDS_CUSTOM_CLUSTER;
import static com.mysql.cj.jdbc.ha.plugins.RdsUrl.RDS_INSTANCE;
import static com.mysql.cj.jdbc.ha.plugins.RdsUrl.RDS_PROXY;
import static com.mysql.cj.jdbc.ha.plugins.RdsUrl.RDS_READER_CLUSTER;
import static com.mysql.cj.jdbc.ha.plugins.RdsUrl.RDS_WRITER_CLUSTER;

public class ConnectionStringTopologyProvider {
  private final Pattern auroraDnsPattern =
      Pattern.compile(
          "(.+)\\.(proxy-|cluster-|cluster-ro-|cluster-custom-)?([a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
  private final Pattern rdsInstanceDnsPattern =
      Pattern.compile(
          "(.+)\\.(proxy-|cluster-|cluster-ro-|cluster-custom-){0}([a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
  private final Pattern auroraCustomClusterPattern =
      Pattern.compile(
          "(.+)\\.(cluster-custom-[a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
  private final Pattern auroraProxyDnsPattern =
      Pattern.compile(
          "(.+)\\.(proxy-[a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
  private final Log logger;
  private final int failoverConnectTimeoutMs;
  private final int failoverSocketTimeoutMs;
  private final String clusterIdSetting;
  private Map<String, String> initialConnectionProps = null;

  public ConnectionStringTopologyProvider(PropertySet propertySet, Log logger) {
    this.logger = logger;
    this.failoverConnectTimeoutMs =
        propertySet.getIntegerProperty(PropertyKey.connectTimeout).getValue();
    this.failoverSocketTimeoutMs =
        propertySet.getIntegerProperty(PropertyKey.socketTimeout).getValue();
    this.clusterIdSetting =
        propertySet.getStringProperty(PropertyKey.clusterId).getValue();
  }

  public void setInitialConnectionProps(Map<String, String> connectionProps) {
    this.initialConnectionProps = connectionProps;
  }

  public RdsUrl getUrlType(ConnectionUrl connectionUrl) {
    return getUrlType(connectionUrl.getMainHost().getHost());
  }

  public RdsUrl getUrlType(String url) {
    if (IpAddressUtils.isIPv4(url) || IpAddressUtils.isIPv6(url)) {
      return IP_ADDRESS;
    } else if (isWriterClusterDns(url)) {
      return RDS_WRITER_CLUSTER;
    } else if (isReaderClusterDns(url)) {
      return RDS_READER_CLUSTER;
    } else if (isRdsCustomClusterDns(url)) {
      return RDS_CUSTOM_CLUSTER;
    } else if (isRdsProxyDns(url)) {
      return RDS_PROXY;
    } else if (isRdsDns(url)) {
      return RDS_INSTANCE;
    } else {
      return OTHER;
    }
  }

  public RdsHost getRdsHost(HostInfo hostInfo) throws SQLException {
    final String hostName = hostInfo.getHost();
    final int hostPort = hostInfo.getPort();
    final RdsUrl rdsUrl = getUrlType(hostName);
    final String clusterId = getClusterId(rdsUrl, hostName, hostPort);

    String clusterInstanceTemplateHost = hostName;
    if (rdsUrl.isRds()) {
      clusterInstanceTemplateHost = getRdsInstanceHostPattern(hostName);
      if (clusterInstanceTemplateHost == null) {
        this.logger.logError(Messages.getString("ConnectionStringTopologyProvider.5"));
        throw new SQLException(Messages.getString("ConnectionStringTopologyProvider.5"));
      }
    }

    final HostInfo clusterInstanceTemplate = getClusterInstanceTemplate(hostInfo, clusterInstanceTemplateHost, hostPort);
    return new RdsHost(rdsUrl, clusterId, clusterInstanceTemplate, hostName, hostInfo.getPort());
  }

  public RdsHost parseHostPatternSetting(HostInfo hostInfo, String pattern) throws SQLException {
    final ConnectionUrlParser.Pair<String, Integer> pair = ConnectionUrlParser.parseHostPortPair(pattern);
    if (pair == null) {
      // "Invalid value for the 'clusterInstanceHostPattern' configuration setting - the value could not be parsed"
      throw new SQLException(Messages.getString("ConnectionStringTopologyProvider.1"));
    }

    final String hostName = pair.left;
    final RdsUrl rdsUrl = getUrlType(hostName);
    validateHostPatternSetting(hostName, rdsUrl);

    final int hostPort =
        pair.right != RdsHost.NO_PORT ? pair.right : hostInfo.getPort();

    final String clusterId = getClusterId(rdsUrl, hostName, hostPort);
    final HostInfo clusterInstanceTemplate = getClusterInstanceTemplate(hostInfo, hostName, hostPort);
    return new RdsHost(rdsUrl, clusterId, clusterInstanceTemplate, hostName, hostPort);
  }

  private void validateHostPatternSetting(String hostName, RdsUrl rdsUrl) throws SQLException {
    if (!isDnsPatternValid(hostName)) {
      // "Invalid value for the 'clusterInstanceHostPattern' configuration setting - the host pattern must contain a '?'
      // character as a placeholder for the DB instance identifiers of the instances in the cluster"
      this.logger.logError(Messages.getString("ConnectionStringTopologyProvider.2"));
      throw new SQLException(Messages.getString("ConnectionStringTopologyProvider.2"));
    }

    if (RDS_PROXY.equals(rdsUrl)) {
      // "An RDS Proxy url can't be used as the 'clusterInstanceHostPattern' configuration setting."
      this.logger.logError(Messages.getString("ConnectionStringTopologyProvider.3"));
      throw new SQLException(Messages.getString("ConnectionStringTopologyProvider.3"));
    }

    if (RDS_CUSTOM_CLUSTER.equals(rdsUrl)) {
      // "An RDS Custom Cluster endpoint can't be used as the 'clusterInstanceHostPattern' configuration setting."
      this.logger.logError(Messages.getString("ConnectionStringTopologyProvider.4"));
      throw new SQLException(Messages.getString("ConnectionStringTopologyProvider.4"));
    }
  }

  private String getClusterId(RdsUrl rdsUrl, String hostName, int hostPort) {
    String clusterId = null;
    if (!StringUtils.isNullOrEmpty(this.clusterIdSetting)) {
      clusterId = this.clusterIdSetting;
    } else if (RDS_PROXY.equals(rdsUrl)) {
      // Each proxy is associated with a single cluster, so it's safe to use RDS Proxy Url as cluster identification
      clusterId = hostName + RdsHost.HOST_PORT_SEPARATOR + hostPort;
    } else if (rdsUrl.isRds()) {
      // If it's a cluster endpoint, or a reader cluster endpoint, then let's use it as the cluster ID
      String clusterRdsHostUrl = getRdsClusterHostUrl(hostName);
      if (!StringUtils.isNullOrEmpty(clusterRdsHostUrl)) {
        clusterId = clusterRdsHostUrl + RdsHost.HOST_PORT_SEPARATOR + hostPort;
      }
    }
    return clusterId;
  }

  private HostInfo getClusterInstanceTemplate(
      HostInfo hostInfo,
      String host,
      int port) {
    // TODO: review whether we still need this method
    final Map<String, String> properties = new HashMap<>(this.initialConnectionProps);
    properties.put(
        PropertyKey.connectTimeout.getKeyName(),
        String.valueOf(this.failoverConnectTimeoutMs));
    properties.put(
        PropertyKey.socketTimeout.getKeyName(),
        String.valueOf(this.failoverSocketTimeoutMs));

    if (!Objects.equals(hostInfo.getDatabase(), "")) {
      properties.put(
          PropertyKey.DBNAME.getKeyName(),
          hostInfo.getDatabase());
    }

    final Properties connectionProperties = new Properties();
    connectionProperties.putAll(this.initialConnectionProps);

    final ConnectionUrl connectionUrl = ConnectionUrl.getConnectionUrlInstance(
        hostInfo.getDatabaseUrl(),
        connectionProperties);

    return new HostInfo(
        connectionUrl,
        host,
        port,
        hostInfo.getUser(),
        hostInfo.getPassword(),
        hostInfo.isPasswordless(),
        properties);
  }

  private boolean isDnsPatternValid(String pattern) {
    return pattern.contains("?");
  }

  private boolean isWriterClusterDns(String host) {
    final Matcher matcher = auroraDnsPattern.matcher(host);
    return "cluster-".equalsIgnoreCase(getClusterKeyword(matcher));
  }

  private boolean isReaderClusterDns(String host) {
    final Matcher matcher = auroraDnsPattern.matcher(host);
    return "cluster-ro-".equalsIgnoreCase(getClusterKeyword(matcher));
  }

  private boolean isRdsCustomClusterDns(String host) {
    final Matcher matcher = auroraCustomClusterPattern.matcher(host);
    return matcher.find();
  }

  private boolean isRdsDns(String host) {
    final Matcher matcher = rdsInstanceDnsPattern.matcher(host);
    return matcher.find();
  }

  private boolean isRdsProxyDns(String host) {
    final Matcher matcher = auroraProxyDnsPattern.matcher(host);
    return matcher.find();
  }

  private String getClusterKeyword(Matcher matcher) {
    if (matcher.find()
        && matcher.group(2) != null
        && matcher.group(1) != null
        && !matcher.group(1).isEmpty()) {
      return matcher.group(2);
    }
    return null;
  }

  public String getRdsClusterHostUrl(String host) {
    final Matcher matcher = auroraDnsPattern.matcher(host);
    final String clusterKeyword = getClusterKeyword(matcher);
    if ("cluster-".equalsIgnoreCase(clusterKeyword)
        || "cluster-ro-".equalsIgnoreCase(clusterKeyword)) {
      return matcher.group(1) + ".cluster-"
          + matcher.group(3); // always RDS cluster endpoint
    }
    return null;
  }

  private String getRdsInstanceHostPattern(String host) {
    final Matcher matcher = auroraDnsPattern.matcher(host);
    if (matcher.find()) {
      return "?." + matcher.group(3);
    }
    return null;
  }
}
