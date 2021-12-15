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
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Class containing helper methods for {@link ClusterAwareConnectionProxyTest},
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
