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

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ha.ConnectionProxyTest;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Class containing helper methods for {@link ConnectionProxyTest},
 * {@link ClusterAwareReaderFailoverHandlerTest} and {@link ClusterAwareWriterFailoverHandlerTest}.
 */
public class ClusterAwareTestUtils {
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
    String url = "jdbc:mysql:aws://" + instanceName + ".com:1234/";
    db = (db == null) ? "" : db;
    properties.put(PropertyKey.DBNAME.getKeyName(), db);
    url += db;
    final ConnectionUrl conStr =
        ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    return new HostInfo(conStr, instanceName, 1234, user, password, properties);
  }
}
