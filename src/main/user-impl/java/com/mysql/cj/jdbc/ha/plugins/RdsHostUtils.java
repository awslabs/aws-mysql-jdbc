// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0
// (GPLv2), as published by the Free Software Foundation, with the
// following additional permissions:
//
// This program is distributed with certain software that is licensed
// under separate terms, as designated in a particular file or component
// or in the license documentation. Without limiting your rights under
// the GPLv2, the authors of this program hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have included with the program.
//
// Without limiting the foregoing grant of rights under the GPLv2 and
// additional permission as to separately licensed software, this
// program is also subject to the Universal FOSS Exception, version 1.0,
// a copy of which can be found along with its FAQ at
// http://oss.oracle.com/licenses/universal-foss-exception.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see
// http://www.gnu.org/licenses/gpl-2.0.html.

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
import com.mysql.cj.util.Util;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mysql.cj.jdbc.ha.plugins.RdsUrlType.IP_ADDRESS;
import static com.mysql.cj.jdbc.ha.plugins.RdsUrlType.OTHER;
import static com.mysql.cj.jdbc.ha.plugins.RdsUrlType.RDS_CUSTOM_CLUSTER;
import static com.mysql.cj.jdbc.ha.plugins.RdsUrlType.RDS_INSTANCE;
import static com.mysql.cj.jdbc.ha.plugins.RdsUrlType.RDS_PROXY;
import static com.mysql.cj.jdbc.ha.plugins.RdsUrlType.RDS_READER_CLUSTER;
import static com.mysql.cj.jdbc.ha.plugins.RdsUrlType.RDS_WRITER_CLUSTER;

public class RdsHostUtils {
  public static final int NO_CONNECTION_INDEX = -1;
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

  public RdsHostUtils(Log logger) {
    this.logger = logger;
  }

  public RdsUrlType getUrlType(ConnectionUrl connectionUrl) {
    return getUrlType(connectionUrl.getMainHost().getHost());
  }

  public RdsUrlType getUrlType(String url) {
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

  public RdsHost getRdsHost(PropertySet propertySet, HostInfo hostInfo) throws SQLException {
    final String hostName = hostInfo.getHost();
    final int hostPort = hostInfo.getPort();
    final RdsUrlType rdsUrlType = getUrlType(hostName);
    final String clusterId = getClusterId(propertySet, rdsUrlType, hostName, hostPort);

    String clusterInstanceTemplateHost = hostName;
    if (rdsUrlType.isRds()) {
      clusterInstanceTemplateHost = getRdsInstanceHostPattern(hostName);
      if (clusterInstanceTemplateHost == null) {
        logAndThrow(Messages.getString("RdsHostUtils.5"));
      }
    }

    final HostInfo clusterInstanceTemplate = getClusterInstanceTemplate(propertySet, hostInfo, clusterInstanceTemplateHost, hostPort);
    return new RdsHost(rdsUrlType, clusterId, clusterInstanceTemplate, hostName, hostInfo.getPort());
  }

  public RdsHost getRdsHostFromHostPattern(PropertySet propertySet, HostInfo hostInfo, String pattern) throws SQLException {
    final ConnectionUrlParser.Pair<String, Integer> pair = ConnectionUrlParser.parseHostPortPair(pattern);
    if (pair == null) {
      // "Invalid value for the 'clusterInstanceHostPattern' configuration setting - the value could not be parsed"
      logAndThrow(Messages.getString("RdsHostUtils.1"));
    }

    final String hostName = pair.left;
    final RdsUrlType rdsUrlType = getUrlType(hostName);
    validateHostPatternSetting(hostName, rdsUrlType);

    final int hostPort =
        pair.right != RdsHost.NO_PORT ? pair.right : hostInfo.getPort();

    final String clusterId = getClusterId(propertySet, rdsUrlType, hostName, hostPort);
    final HostInfo clusterInstanceTemplate = getClusterInstanceTemplate(propertySet, hostInfo, hostName, hostPort);
    return new RdsHost(rdsUrlType, clusterId, clusterInstanceTemplate, hostName, hostPort);
  }

  private void validateHostPatternSetting(String hostName, RdsUrlType rdsUrlType) throws SQLException {
    if (!isDnsPatternValid(hostName)) {
      // "Invalid value for the 'clusterInstanceHostPattern' configuration setting - the host pattern must contain a '?'
      // character as a placeholder for the DB instance identifiers of the instances in the cluster"
      logAndThrow(Messages.getString("RdsHostUtils.2"));
    }

    if (RDS_PROXY.equals(rdsUrlType)) {
      // "An RDS Proxy url can't be used as the 'clusterInstanceHostPattern' configuration setting."
      logAndThrow(Messages.getString("RdsHostUtils.3"));
    }

    if (RDS_CUSTOM_CLUSTER.equals(rdsUrlType)) {
      // "An RDS Custom Cluster endpoint can't be used as the 'clusterInstanceHostPattern' configuration setting."
      logAndThrow(Messages.getString("RdsHostUtils.4"));
    }
  }

  private String getClusterId(PropertySet propertySet, RdsUrlType rdsUrlType, String hostName, int hostPort) {
    String clusterIdSetting = propertySet.getStringProperty(PropertyKey.clusterId).getValue();
    if (!StringUtils.isNullOrEmpty(clusterIdSetting)) {
      return clusterIdSetting;
    } else if (RDS_PROXY.equals(rdsUrlType)) {
      // Each proxy is associated with a single cluster, so it's safe to use RDS Proxy Url as cluster identification
      return hostName + RdsHost.HOST_PORT_SEPARATOR + hostPort;
    } else if (rdsUrlType.isRds()) {
      // If it's a cluster endpoint, or a reader cluster endpoint, then let's use it as the cluster ID
      String clusterRdsHostUrl = getRdsClusterHostUrl(hostName);
      if (!StringUtils.isNullOrEmpty(clusterRdsHostUrl)) {
        return clusterRdsHostUrl + RdsHost.HOST_PORT_SEPARATOR + hostPort;
      }
    }
    return null;
  }

  private HostInfo getClusterInstanceTemplate(
      PropertySet propertySet,
      HostInfo hostInfo,
      String host,
      int port) {
    // TODO: review whether we still need this method
    Map<String, String> explicitlySetProperties = getExplicitlySetProperties(propertySet);
    final Map<String, String> properties = new HashMap<>(explicitlySetProperties);

    final int failoverConnectTimeoutMs =
        propertySet.getIntegerProperty(PropertyKey.connectTimeout).getValue();
    final int failoverSocketTimeoutMs =
        propertySet.getIntegerProperty(PropertyKey.socketTimeout).getValue();
    properties.put(
        PropertyKey.connectTimeout.getKeyName(),
        String.valueOf(failoverConnectTimeoutMs));
    properties.put(
        PropertyKey.socketTimeout.getKeyName(),
        String.valueOf(failoverSocketTimeoutMs));

    if (!Objects.equals(hostInfo.getDatabase(), "")) {
      properties.put(
          PropertyKey.DBNAME.getKeyName(),
          hostInfo.getDatabase());
    }

    final Properties connectionProperties = new Properties();
    connectionProperties.putAll(explicitlySetProperties);

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

  public Map<String, String> getExplicitlySetProperties(final PropertySet propertySet) {
    final Map<String, String> explicitlySetProperties = new HashMap<>();
    final Properties originalProperties = propertySet.exposeAsProperties();
    originalProperties.stringPropertyNames()
        .stream()
        .filter(x -> propertySet.getProperty(x).isExplicitlySet())
        .forEach(x -> explicitlySetProperties.put(x, originalProperties.getProperty(x)));

    return explicitlySetProperties;
  }

  public int getHostIndex(List<HostInfo> hostList, HostInfo host) {
    if (Util.isNullOrEmpty(hostList) || host == null) {
      return NO_CONNECTION_INDEX;
    }
    return getHostIndex(hostList, host.getHostPortPair());
  }

  public int getHostIndex(List<HostInfo> hostList, String hostPortPair) {
    if (Util.isNullOrEmpty(hostList) || StringUtils.isNullOrEmpty(hostPortPair)) {
      return NO_CONNECTION_INDEX;
    }

    for (int i = 0; i < hostList.size(); i++) {
      HostInfo potentialMatch = hostList.get(i);
      if (hostPortPair.equals(potentialMatch.getHostPortPair())) {
        return i;
      }
    }
    return NO_CONNECTION_INDEX;
  }

  public HostInfo getHostInfo(List<HostInfo> hostList, HostInfo host) {
    if (Util.isNullOrEmpty(hostList) || host == null) {
      return null;
    }
    return getHostInfo(hostList, host.getHostPortPair());
  }

  public HostInfo getHostInfo(List<HostInfo> hostList, String hostPortPair) {
    if (Util.isNullOrEmpty(hostList) || StringUtils.isNullOrEmpty(hostPortPair)) {
      return null;
    }

    for (HostInfo potentialMatch : hostList) {
      if (potentialMatch.getHostPortPair().equals(hostPortPair)) {
        return potentialMatch;
      }
    }
    return null;
  }

  private void logAndThrow(String msg) throws SQLException {
    this.logger.logError(msg);
    throw new SQLException(msg);
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
