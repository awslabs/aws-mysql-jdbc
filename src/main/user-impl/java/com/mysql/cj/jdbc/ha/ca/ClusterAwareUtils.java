/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of this program hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of this connector, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc.ha.ca;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.DatabaseUrlContainer;
import com.mysql.cj.conf.HostInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class for copying {@link HostInfo} objects.
 */
public class ClusterAwareUtils {
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

    DatabaseUrlContainer urlContainer = ConnectionUrl.getConnectionUrlInstance(
        baseHostInfo.getDatabaseUrl(),
        new Properties());
    Map<String, String> originalProps = baseHostInfo.getHostProperties();
    Map<String, String> mergedProps = new HashMap<>();
    mergedProps.putAll(originalProps);
    mergedProps.putAll(mainHost.getHostProperties());

    return new HostInfo(urlContainer, baseHostInfo.getHost(), baseHostInfo.getPort(),
        mainHost.getUser(), mainHost.getPassword(),
        mergedProps);
  }
}
