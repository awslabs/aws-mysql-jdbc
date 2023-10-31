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

package com.mysql.cj.jdbc.ha.util;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.DatabaseUrlContainer;
import com.mysql.cj.conf.HostInfo;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.util.StringUtils;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class for copying {@link HostInfo} objects.
 */
public class ConnectionUtils {
  /**
   * Create a copy of the given {@link HostInfo} object where all details are the same except for the host properties,
   * which will contain both the original properties and the properties passed into the function.
   *
   * @param baseHostInfo The {@link HostInfo} object to copy
   * @param additionalProps The map of properties to add to the new {@link HostInfo} copy
   *
   * @return A copy of the given {@link HostInfo} object where all details are the same except for the host properties,
   *      will contain both the original properties and the properties passed into the function. Returns null if
   *      baseHostInfo is null
   */
  public static HostInfo copyWithAdditionalProps(
      HostInfo baseHostInfo,
      Map<String, String> additionalProps) {
    if (baseHostInfo == null || additionalProps == null) {
      return baseHostInfo;
    }

    DatabaseUrlContainer urlContainer = ConnectionUrl.getConnectionUrlInstance(
        baseHostInfo.getDatabaseUrl(),
        new Properties());
    Map<String, String> originalProps = baseHostInfo.getHostProperties();
    Map<String, String> mergedProps = new HashMap<>();
    mergedProps.putAll(originalProps);
    mergedProps.putAll(additionalProps);
    return new HostInfo(
        urlContainer,
        baseHostInfo.getHost(),
        baseHostInfo.getPort(),
        baseHostInfo.getUser(),
        baseHostInfo.getPassword(),
        mergedProps);
  }

  /**
   * Create a copy of {@link HostInfo} object where host and port are the same while all others are from {@link ConnectionUrl} object.
   *
   * @param baseHostInfo The {@link HostInfo} object to copy host and port from
   * @param connectionUrl All other properties to add to the new {@link HostInfo}
   *
   * @return A copy of {@link HostInfo} object where host and port are the same while all others are from {@link ConnectionUrl} object
   *      Returns baseHostInfo if connectionUrl is null
   *      Returns connectionUrl's HostInfo if baseHostInfo is null
   */
  public static HostInfo copyWithAdditionalProps(
      HostInfo baseHostInfo,
      ConnectionUrl connectionUrl) {
    if (connectionUrl == null) {
      return baseHostInfo;
    }

    final HostInfo mainHost = connectionUrl.getMainHost();
    if (baseHostInfo == null) {
      return mainHost;
    }

    return copyWithAdditionalProps(baseHostInfo, mainHost);
  }

  public static HostInfo copyWithAdditionalProps(
      HostInfo baseHostInfo,
      HostInfo newHostInfo) {
    final DatabaseUrlContainer urlContainer = ConnectionUrl.getConnectionUrlInstance(
        baseHostInfo.getDatabaseUrl(),
        new Properties());
    final Map<String, String> originalProps = baseHostInfo.getHostProperties();
    final Map<String, String> mergedProps = new HashMap<>();
    mergedProps.putAll(originalProps);
    mergedProps.putAll(newHostInfo.getHostProperties());

    return new HostInfo(urlContainer, baseHostInfo.getHost(), baseHostInfo.getPort(),
        newHostInfo.getUser(), newHostInfo.getPassword(),
        mergedProps);
  }

  public static HostInfo createHostWithProperties(HostInfo baseHost, Map<String, String> properties)
      throws SQLException {
    final Map<String, String> propertiesCopy = new HashMap<>(properties);
    propertiesCopy.putAll(baseHost.getHostProperties());
    final String hostEndpoint = baseHost.getHost();
    final int port = baseHost.getPort();
    final String user = propertiesCopy.get(PropertyKey.USER.getKeyName());
    final String password = propertiesCopy.get(PropertyKey.PASSWORD.getKeyName());
    propertiesCopy.remove(PropertyKey.USER.getKeyName());
    propertiesCopy.remove(PropertyKey.PASSWORD.getKeyName());
    final Properties connProps = new Properties();
    connProps.putAll(propertiesCopy);

    final ConnectionUrl hostUrl = ConnectionUrl.getConnectionUrlInstance(
        getUrlFromEndpoint(
            hostEndpoint,
            port,
            connProps),
        connProps);

    return new HostInfo(
        hostUrl,
        hostEndpoint,
        port,
        user,
        password,
        propertiesCopy);
  }

  /**
   * Create a copy of {@link HostInfo} object where all properties are the same but the hostInfo points to the provided
   * instance.
   *
   * @param instance The instance name to replace the previous host.
   * @param previousHostInfo The {@link HostInfo} object that all other information to add to the new {@link HostInfo}.
   *
   * @return A copy of {@link HostInfo} object where only the host has been changed.
   */
  public static HostInfo createInstanceHostWithProperties(String instance, HostInfo previousHostInfo)
      throws SQLException {
    final Properties propertiesCopy = new Properties();
    propertiesCopy.putAll(previousHostInfo.getHostProperties());
    propertiesCopy.put(PropertyKey.USER.getKeyName(), previousHostInfo.getUser());
    propertiesCopy.put(PropertyKey.PASSWORD.getKeyName(), previousHostInfo.getPassword());

    final ConnectionUrl hostUrl = ConnectionUrl.getConnectionUrlInstance(
        getUrlFromEndpoint(
            instance,
            previousHostInfo.getPort(),
            propertiesCopy),
        propertiesCopy);

    return new HostInfo(
        hostUrl,
        instance,
        previousHostInfo.getPort(),
        previousHostInfo.getUser(),
        previousHostInfo.getPassword(),
        previousHostInfo.getHostProperties());
  }

  public static String getUrlFromEndpoint(String endpoint, int port, Properties props) throws SQLException {
    final Properties propsCopy = new Properties();
    propsCopy.putAll(props);

    final String portString = port < 1 ? "" : ":" + port;

    final StringBuilder urlBuilder = new StringBuilder(
      String.format(
        "%s//%s%s",
        ConnectionUrl.Type.SINGLE_CONNECTION_AWS.getScheme(),
        endpoint,
        portString)
    );

    final String dbName = propsCopy.getProperty(PropertyKey.DBNAME.getKeyName());
    boolean containsDbName = false;
    if (!StringUtils.isNullOrEmpty(dbName)) {
      urlBuilder.append("/").append(dbName);
      containsDbName = true;
    }
    propsCopy.remove(PropertyKey.DBNAME.getKeyName());

    final StringBuilder queryBuilder = new StringBuilder();

    if (propsCopy.size() != 0) {
      final Enumeration<?> propertyNames = propsCopy.propertyNames();
      while (propertyNames.hasMoreElements()) {
        final String propertyName = propertyNames.nextElement().toString();
        if (propertyName != null && !propertyName.trim().equals("")) {
          if (queryBuilder.length() != 0) {
            queryBuilder.append("&");
          }
          final String propertyValue = propsCopy.getProperty(propertyName);
          try {
            queryBuilder
                .append(propertyName)
                .append("=")
                .append(URLEncoder.encode(propertyValue, StandardCharsets.UTF_8.toString()));
          } catch (final UnsupportedEncodingException e) {
            throw new SQLException(Messages.getString("ConnectionUtils.1"));
          }
        }
      }
    }

    if (queryBuilder.length() != 0) {
      urlBuilder.append("?").append(queryBuilder);
    } else if (!containsDbName) {
      urlBuilder.append("/");
    }

    return urlBuilder.toString();
  }

  /**
   * Check whether the given exception is caused by network errors.
   *
   * @param exception The {@link SQLException} raised by the driver.
   * @return true if the exception is caused by network errors; false otherwise.
   */
  public static boolean isNetworkException(final SQLException exception) {
    final String sqlState = exception.getSQLState();
    return isNetworkException(sqlState);
  }

  /**
   * Check whether the given SQLState is caused by network errors.
   *
   * @param sqlState The SQLState of an exception raised by the driver.
   * @return true if the exception is caused by network errors; false otherwise.
   */
  public static boolean isNetworkException(final String sqlState) {
    return sqlState != null && sqlState.startsWith("08");
  }
}
