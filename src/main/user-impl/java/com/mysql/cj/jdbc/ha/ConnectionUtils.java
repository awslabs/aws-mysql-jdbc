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

package com.mysql.cj.jdbc.ha;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.DatabaseUrlContainer;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.jdbc.JdbcConnection;

import java.sql.SQLException;
import java.util.HashMap;
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

  public static HostInfo copyWithAdditionalProps(HostInfo currentHostInfo, HostInfo baseHostInfo,
                                                  Map<String, String> additionalProps) {
    if (baseHostInfo == null || additionalProps == null) {
      return baseHostInfo;
    }
    DatabaseUrlContainer urlContainer = ConnectionUrl.getConnectionUrlInstance(
            replaceDatabaseName(baseHostInfo.getDatabaseUrl(), currentHostInfo.getDatabase()),
            new Properties());
    Map<String, String> originalProps = baseHostInfo.getHostProperties();
    Map<String, String> mergedProps = new HashMap<>();
    mergedProps.putAll(originalProps);
    mergedProps.putAll(additionalProps);
    mergedProps.putAll(currentHostInfo.getHostProperties());
    return new HostInfo(
            urlContainer,
            baseHostInfo.getHost(),
            baseHostInfo.getPort(),
            currentHostInfo.getUser(),
            currentHostInfo.getPassword(),
            mergedProps);
  }

  private static String replaceDatabaseName(String url, String databaseName) {
    int begin = url.lastIndexOf("/");
    if (begin < 0) {
      return url;
    }
    int end = url.indexOf("?");
    if (end < 0) {
      return url.replace(url.substring(begin), "/" + databaseName);
    } else {
      return url.replace(url.substring(begin, end + 1), "/" + databaseName + "?");
    }
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
    DatabaseUrlContainer urlContainer = ConnectionUrl.getConnectionUrlInstance(
        baseHostInfo.getDatabaseUrl(),
        new Properties());
    Map<String, String> originalProps = baseHostInfo.getHostProperties();
    Map<String, String> mergedProps = new HashMap<>();
    mergedProps.putAll(originalProps);
    mergedProps.putAll(newHostInfo.getHostProperties());

    return new HostInfo(urlContainer, baseHostInfo.getHost(), baseHostInfo.getPort(),
        newHostInfo.getUser(), newHostInfo.getPassword(),
        mergedProps);
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
