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

package testsuite.integration.container.standard;

import com.mysql.cj.conf.PropertyKey;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import software.aws.rds.jdbc.mysql.Driver;
import testsuite.integration.utility.ContainerHelper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StandardMysqlBaseTest {

  protected static final String DB_CONN_STR_PREFIX = "jdbc:mysql:aws://";
  protected static final String TEST_WRITER_HOST = System.getenv("TEST_WRITER_HOST");
  protected static final String TEST_READER_HOST = System.getenv("TEST_READER_HOST");
  protected static final String TEST_PORT = System.getenv("TEST_PORT");
  protected static final String TEST_DB = System.getenv("TEST_DB");
  protected static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  protected static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");
  protected static final String QUERY_FOR_HOSTNAME = "SELECT @@hostname";
  protected static final int clusterSize = 2;
  protected static final String[] instanceIDs = {TEST_WRITER_HOST, TEST_READER_HOST};

  protected static final String TOXIPROXY_WRITER = System.getenv("TOXIPROXY_WRITER");
  protected static final String TOXIPROXY_READER = System.getenv("TOXIPROXY_READER");
  protected static ToxiproxyClient toxiproxyClientWriter;
  protected static ToxiproxyClient toxiproxyClientReader;
  protected static final int TOXIPROXY_CONTROL_PORT = 8474;

  protected static final String PROXIED_DOMAIN_NAME_SUFFIX = System.getenv("PROXIED_DOMAIN_NAME_SUFFIX");
  protected static final String PROXY_PORT = System.getenv("PROXY_PORT");
  protected static Proxy proxyWriter;
  protected static Proxy proxyReader;
  protected static final Map<String, Proxy> proxyMap = new HashMap<>();

  protected final ContainerHelper containerHelper = new ContainerHelper();

  @BeforeAll
  public static void setUp() throws SQLException, IOException {
    toxiproxyClientWriter = new ToxiproxyClient(TOXIPROXY_WRITER, TOXIPROXY_CONTROL_PORT);
    toxiproxyClientReader = new ToxiproxyClient(TOXIPROXY_READER, TOXIPROXY_CONTROL_PORT);

    proxyWriter = getProxy(toxiproxyClientWriter, TEST_WRITER_HOST, Integer.parseInt(TEST_PORT));
    proxyReader = getProxy(toxiproxyClientReader, TEST_READER_HOST, Integer.parseInt(TEST_PORT));

    proxyMap.put(TEST_WRITER_HOST, proxyWriter);
    proxyMap.put(TEST_READER_HOST, proxyReader);

    DriverManager.registerDriver(new Driver());
  }

  @BeforeEach
  public void setUpEach() throws InterruptedException, SQLException {
    proxyMap.forEach((instance, proxy) -> containerHelper.enableConnectivity(proxy));
  }

  protected static Proxy getProxy(ToxiproxyClient proxyClient, String host, int port) throws IOException {
    final String upstream = host + ":" + port;
    return proxyClient.getProxy(upstream);
  }

  protected Connection connect() throws SQLException {
    String url = DB_CONN_STR_PREFIX + TEST_WRITER_HOST + ":" + TEST_PORT + ","
        + TEST_READER_HOST + ":" + TEST_PORT + "/" + TEST_DB;
    return DriverManager.getConnection(url, initDefaultProps());
  }

  protected Connection connectWithProxy() throws SQLException {
    String url = DB_CONN_STR_PREFIX + TEST_WRITER_HOST + PROXIED_DOMAIN_NAME_SUFFIX + ":" + PROXY_PORT + ","
        + TEST_READER_HOST + PROXIED_DOMAIN_NAME_SUFFIX + ":" + PROXY_PORT + "/" + TEST_DB;
    return DriverManager.getConnection(url, initDefaultProps());
  }

  protected String queryInstanceId(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(QUERY_FOR_HOSTNAME);
    rs.next();
    return rs.getString(1);
  }

  protected Properties initDefaultProps() {
    final Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "3000");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "3000");

    return props;
  }

  protected Properties initDefaultPropsNoTimeouts() {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.tcpKeepAlive.getKeyName(), Boolean.FALSE.toString());

    // Uncomment the following line to ease debugging
    //props.setProperty(PropertyKey.logger.getKeyName(), "com.mysql.cj.log.StandardLogger");

    return props;
  }
}
