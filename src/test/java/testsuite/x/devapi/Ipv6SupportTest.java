/*
 * Copyright (c) 2016, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
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

package testsuite.x.devapi;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.Inet6Address;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.xdevapi.Session;

import testsuite.TestUtils;

public class Ipv6SupportTest extends DevApiBaseTestCase {
    List<String> ipv6Addrs;
    String testUser = "testIPv6User";

    @BeforeEach
    public void setupIpv6SupportTest() {
        if (setupTestSession()) {
            List<Inet6Address> ipv6List = TestUtils.getIpv6List();
            this.ipv6Addrs = ipv6List.stream().map((e) -> e.getHostAddress()).collect(Collectors.toList());
            this.ipv6Addrs.add("::1"); // IPv6 loopback

            this.session.sql("DROP USER IF EXISTS '" + this.testUser + "'@'%'").execute();
            this.session.sql("CREATE USER '" + this.testUser + "'@'%' IDENTIFIED WITH mysql_native_password BY '" + this.testUser + "'").execute();
            this.session.sql("GRANT ALL ON *.* TO '" + this.testUser + "'@'%'").execute();
        }
    }

    @AfterEach
    public void teardownIpv6SupportTest() {
        if (this.isSetForXTests && this.session.isOpen()) {
            this.session.sql("DROP USER '" + this.testUser + "'@'%'").execute();
            destroyTestSession();
        }
    }

    /**
     * Tests the creation of {@link Session}s referencing the host by its IPv6. This feature was introduced in MySQL 5.7.17 and requires a server started
     * with the option "mysqlx-bind-address=*" (future versions may set this value by default).
     */
    @Test
    public void testIpv6SupportInSession() {
        assumeTrue(this.isSetForXTests,
                "Not set to run X tests. Set the url to the X server using the property " + PropertyDefinitions.SYSP_testsuite_url_mysqlx);
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("5.7.17")), "Server version 5.7.17 or higher is required.");

        // Although per specification IPv6 addresses must be enclosed by square brackets, we actually support them directly.
        String[] urls = new String[] { "mysqlx://%s:%s@%s:%d", "mysqlx://%s:%s@[%s]:%d", "mysqlx://%s:%s@(address=%s:%d)", "mysqlx://%s:%s@(address=[%s]:%d)",
                "mysqlx://%s:%s@address=(host=%s)(port=%d)", "mysqlx://%s:%s@address=(host=[%s])(port=%d)" };

        int port = getTestPort();

        boolean atLeastOne = false;
        for (String host : this.ipv6Addrs) {
            if (TestUtils.serverListening(host, port)) {
                atLeastOne = true;
                for (String url : urls) {
                    String ipv6Url = String.format(url, this.testUser, this.testUser, TestUtils.encodePercent(host), port);
                    Session sess = this.fact.getSession(ipv6Url);
                    assertFalse(sess.getSchemas().isEmpty());
                    sess.close();
                }
            }
        }

        if (!atLeastOne) {
            fail("None of the tested hosts have server sockets listening on the port " + port
                    + ". This test requires a MySQL server with X Protocol running in local host with IPv6 support enabled "
                    + "(set '--mysqlx-bind-address = *' if needed.");
        }
    }
}
