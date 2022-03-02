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

package com.mysql.cj.jdbc.ha;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.DatabaseUrlContainer;
import com.mysql.cj.conf.HostInfo;

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
}
