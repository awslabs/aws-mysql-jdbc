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

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ha.ConnectionProxyTest;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.mockito.ArgumentMatcher;

/**
 * Class containing helper methods for {@link ConnectionProxyTest},
 * {@link ClusterAwareReaderFailoverHandlerTest} and {@link ClusterAwareWriterFailoverHandlerTest}.
 */
public class ClusterAwareTestUtils {
  protected static HostInfo createBasicHostInfo(String instanceName) {
    return createBasicHostInfo(instanceName, null, null, null);
  }

  protected static HostInfo createBasicHostInfo(String instanceName, String db) {
    return createBasicHostInfo(instanceName, db, null, null);
  }

  protected static HostInfo createBasicHostInfo(
      String instanceName,
      String db,
      String user,
      String password) {
    final Map<String, String> properties = new HashMap<>();
    properties.put(TopologyServicePropertyKeys.INSTANCE_NAME, instanceName);
    properties.put(PropertyKey.connectTimeout.getKeyName(), "0");
    properties.put(PropertyKey.socketTimeout.getKeyName(), "0");
    String url = "jdbc:mysql:aws://" + instanceName + ".com:1234/";
    String host = instanceName + ".com";
    if (db != null) {
      properties.put(PropertyKey.DBNAME.getKeyName(), db);
    }
    final ConnectionUrl conStr =
        ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    return new HostInfo(conStr, host, 1234, user, password, properties);
  }

  protected static class HostInfoMatcher implements ArgumentMatcher<HostInfo> {

    private final HostInfo expectedHost;

    HostInfoMatcher(HostInfo hostInfo) {
      this.expectedHost = hostInfo;
    }

    @Override
    public boolean matches(HostInfo host) {
      return hostsAreTheSame(expectedHost, host);
    }
  }

  protected static boolean hostsAreTheSame(HostInfo hostInfo1, HostInfo hostInfo2) {
    if (hostInfo1 == hostInfo2) {
      return true;
    }

    return hostInfo1.getPort() == hostInfo2.getPort() &&
        Objects.equals(hostInfo1.getUser(), hostInfo2.getUser()) &&
        Objects.equals(hostInfo1.getPassword(), hostInfo2.getPassword()) &&
        Objects.equals(hostInfo1.exposeAsProperties(), hostInfo2.exposeAsProperties()) &&
        Objects.equals(hostInfo1.getDatabaseUrl(), hostInfo2.getDatabaseUrl());
  }
}
