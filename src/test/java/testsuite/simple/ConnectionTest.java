/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
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

package testsuite.simple;

import com.mysql.cj.*;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyDefinitions.DatabaseTerm;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.InvalidConnectionAttributeException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.*;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.MessageSender;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.a.*;
import com.mysql.cj.util.TimeUtil;
import com.mysql.cj.util.Util;
import com.mysql.jdbc.Driver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import testsuite.BaseQueryInterceptor;
import testsuite.BaseTestCase;
import testsuite.BufferingLogger;
import testsuite.TestUtils;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests java.sql.Connection functionality
 */
public class ConnectionTest extends BaseTestCase {
    private String sslFreeBaseUrl = "";

    @BeforeEach
    public void setupSecureSessionTest() {
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStoreType");
        System.clearProperty("javax.net.ssl.trustStorePassword");

        this.sslFreeBaseUrl = dbUrl;
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.sslMode.getKeyName() + "=", PropertyKey.sslMode.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.trustCertificateKeyStoreUrl.getKeyName() + "=",
                PropertyKey.trustCertificateKeyStoreUrl.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.trustCertificateKeyStoreType.getKeyName() + "=",
                PropertyKey.trustCertificateKeyStoreType.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.trustCertificateKeyStorePassword.getKeyName() + "=",
                PropertyKey.trustCertificateKeyStorePassword.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.fallbackToSystemTrustStore.getKeyName() + "=",
                PropertyKey.fallbackToSystemTrustStore.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.clientCertificateKeyStoreUrl.getKeyName() + "=",
                PropertyKey.clientCertificateKeyStoreUrl.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.clientCertificateKeyStoreType.getKeyName() + "=",
                PropertyKey.clientCertificateKeyStoreType.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.clientCertificateKeyStorePassword.getKeyName() + "=",
                PropertyKey.clientCertificateKeyStorePassword.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.fallbackToSystemKeyStore.getKeyName() + "=",
                PropertyKey.fallbackToSystemKeyStore.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiTlsCiphersuites.getKeyName() + "=",
                PropertyKey.xdevapiTlsCiphersuites.getKeyName() + "VOID=");
        this.sslFreeBaseUrl = this.sslFreeBaseUrl.replaceAll(PropertyKey.xdevapiTlsVersions.getKeyName() + "=",
                PropertyKey.xdevapiTlsVersions.getKeyName() + "VOID=");
        if (!this.sslFreeBaseUrl.contains("?")) {
            this.sslFreeBaseUrl += "?";
        }
    }

    @AfterEach
    public void teardownConnectionTest() {
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStoreType");
        System.clearProperty("javax.net.ssl.trustStorePassword");
        System.clearProperty("javax.net.ssl.keyStore");
        System.clearProperty("javax.net.ssl.keyStoreType");
        System.clearProperty("javax.net.ssl.keyStorePassword");
    }

    /**
     * Tests catalog functionality
     * 
     * @throws Exception
     */
    @Test
    public void testCatalog() throws Exception {
        if (((JdbcConnection) this.conn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA) {
            String currentSchema = this.conn.getSchema();
            this.conn.setSchema(currentSchema);
            assertTrue(currentSchema.equals(this.conn.getSchema()));
        } else {
            String currentCatalog = this.conn.getCatalog();
            this.conn.setCatalog(currentCatalog);
            assertTrue(currentCatalog.equals(this.conn.getCatalog()));
        }
    }

    /**
     * Tests a cluster connection for failover, requires a two-node cluster URL specified in com.mysql.jdbc.testsuite.ClusterUrl system property.
     * 
     * @throws Exception
     */
    @Test
    public void testClusterConnection() throws Exception {
        String url = System.getProperty(PropertyDefinitions.SYSP_testsuite_url_cluster);

        if ((url != null) && (url.length() > 0)) {
            Object versionNumObj = getSingleValueWithQuery("SHOW VARIABLES LIKE 'version'");

            if ((versionNumObj != null) && (versionNumObj.toString().indexOf("cluster") != -1)) {
                Connection clusterConn = null;
                Statement clusterStmt = null;

                try {
                    clusterConn = new NonRegisteringDriver().connect(url, null);

                    clusterStmt = clusterConn.createStatement();
                    clusterStmt.executeUpdate("DROP TABLE IF EXISTS testClusterConn");
                    clusterStmt.executeUpdate("CREATE TABLE testClusterConn (field1 INT) ENGINE=ndbcluster");
                    clusterStmt.executeUpdate("INSERT INTO testClusterConn VALUES (1)");

                    clusterConn.setAutoCommit(false);

                    clusterStmt.execute("SELECT * FROM testClusterConn");
                    clusterStmt.executeUpdate("UPDATE testClusterConn SET field1=4");

                    // Kill the connection
                    @SuppressWarnings("unused")
                    String connectionId = getSingleValueWithQuery("SELECT CONNECTION_ID()").toString();

                    System.out.println("Please kill the MySQL server now and press return...");
                    System.in.read();

                    System.out.println("Waiting for TCP/IP timeout...");
                    Thread.sleep(10);

                    System.out.println("Attempting auto reconnect");

                    try {
                        clusterConn.setAutoCommit(true);
                        clusterConn.setAutoCommit(false);
                    } catch (SQLException sqlEx) {
                        System.out.println(sqlEx);
                    }

                    //
                    // Test that this 'new' connection is not read-only
                    //
                    clusterStmt.executeUpdate("UPDATE testClusterConn SET field1=5");

                    ResultSet rset = clusterStmt.executeQuery("SELECT * FROM testClusterConn WHERE field1=5");

                    assertTrue(rset.next(), "One row should be returned");
                } finally {
                    if (clusterStmt != null) {
                        clusterStmt.executeUpdate("DROP TABLE IF EXISTS testClusterConn");
                        clusterStmt.close();
                    }

                    if (clusterConn != null) {
                        clusterConn.close();
                    }
                }
            }
        }
    }

    /**
     * Old test was passing due to http://bugs.mysql.com/bug.php?id=989 which is fixed for 5.5+
     * 
     * @throws Exception
     */
    @Test
    public void testDeadlockDetection() throws Exception {
        try {
            this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_lock_wait_timeout'");
            this.rs.next();

            int timeoutSecs = this.rs.getInt(2);

            createTable("t1", "(id INTEGER, x INTEGER) ", "INNODB");
            this.stmt.executeUpdate("INSERT INTO t1 VALUES(0, 0)");
            this.conn.setAutoCommit(false);

            Properties props = new Properties();
            props.setProperty(PropertyKey.includeInnodbStatusInDeadlockExceptions.getKeyName(), "true");
            props.setProperty(PropertyKey.includeThreadDumpInDeadlockExceptions.getKeyName(), "true");

            Connection deadlockConn = getConnectionWithProps(props);
            deadlockConn.setAutoCommit(false);

            try {
                this.conn.createStatement().execute("SELECT * FROM t1 WHERE id=0 FOR UPDATE");

                // The following query should hang because con1 is locking the page
                deadlockConn.createStatement().executeUpdate("UPDATE t1 SET x=2 WHERE id=0");
            } finally {
                this.conn.commit();
                deadlockConn.commit();
            }

            Thread.sleep(timeoutSecs * 2 * 1000);
        } catch (SQLException sqlEx) {
            System.out.println("Caught SQLException due to deadlock/lock timeout");
            System.out.println("SQLState: " + sqlEx.getSQLState());
            System.out.println("Vendor error: " + sqlEx.getErrorCode());
            System.out.println("Message: " + sqlEx.getMessage());
            System.out.println("Stacktrace: ");
            sqlEx.printStackTrace();

            //
            // Check whether the driver thinks it really is deadlock...
            //
            assertTrue(MysqlErrorNumbers.SQL_STATE_ROLLBACK_SERIALIZATION_FAILURE.equals(sqlEx.getSQLState()));
            assertTrue(sqlEx.getErrorCode() == 1205);
            // Make sure INNODB Status is getting dumped into error message

            if (sqlEx.getMessage().indexOf("PROCESS privilege") != -1) {
                fail("This test requires user with process privilege");
            }

            assertTrue(sqlEx.getMessage().indexOf("INNODB MONITOR") != -1, "Can't find INNODB MONITOR in:\n\n" + sqlEx.getMessage());

            assertTrue(sqlEx.getMessage().indexOf("testsuite.simple.ConnectionTest.testDeadlockDetection") != -1,
                    "Can't find thread dump in:\n\n" + sqlEx.getMessage());

        } finally {
            this.conn.setAutoCommit(true);
        }
    }

    @Test
    public void testCharsets() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");

        Connection utfConn = getConnectionWithProps(props);

        this.stmt = utfConn.createStatement();

        createTable("t1", "(comment CHAR(32) ASCII NOT NULL,koi8_ru_f CHAR(32) CHARACTER SET koi8r NOT NULL) CHARSET=latin5");

        this.stmt.executeUpdate("ALTER TABLE t1 CHANGE comment comment CHAR(32) CHARACTER SET latin2 NOT NULL");
        this.stmt.executeUpdate("ALTER TABLE t1 ADD latin5_f CHAR(32) NOT NULL");
        this.stmt.executeUpdate("ALTER TABLE t1 CHARSET=latin2");
        this.stmt.executeUpdate("ALTER TABLE t1 ADD latin2_f CHAR(32) NOT NULL");
        this.stmt.executeUpdate("ALTER TABLE t1 DROP latin2_f, DROP latin5_f");

        this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment) VALUES ('a','LAT SMALL A')");
        /*
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('b','LAT SMALL B')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('c','LAT SMALL C')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('d','LAT SMALL D')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('e','LAT SMALL E')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('f','LAT SMALL F')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('g','LAT SMALL G')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('h','LAT SMALL H')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('i','LAT SMALL I')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('j','LAT SMALL J')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('k','LAT SMALL K')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('l','LAT SMALL L')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('m','LAT SMALL M')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('n','LAT SMALL N')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('o','LAT SMALL O')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('p','LAT SMALL P')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('q','LAT SMALL Q')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('r','LAT SMALL R')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('s','LAT SMALL S')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('t','LAT SMALL T')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('u','LAT SMALL U')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('v','LAT SMALL V')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('w','LAT SMALL W')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('x','LAT SMALL X')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('y','LAT SMALL Y')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('z','LAT SMALL Z')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('A','LAT CAPIT A')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('B','LAT CAPIT B')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('C','LAT CAPIT C')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('D','LAT CAPIT D')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('E','LAT CAPIT E')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('F','LAT CAPIT F')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('G','LAT CAPIT G')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('H','LAT CAPIT H')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('I','LAT CAPIT I')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('J','LAT CAPIT J')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('K','LAT CAPIT K')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('L','LAT CAPIT L')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('M','LAT CAPIT M')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('N','LAT CAPIT N')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('O','LAT CAPIT O')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('P','LAT CAPIT P')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('Q','LAT CAPIT Q')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('R','LAT CAPIT R')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('S','LAT CAPIT S')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('T','LAT CAPIT T')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('U','LAT CAPIT U')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('V','LAT CAPIT V')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('W','LAT CAPIT W')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('X','LAT CAPIT X')"); this.stmt.executeUpdate("INSERT
         * INTO t1 (koi8_ru_f,comment) VALUES ('Y','LAT CAPIT Y')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES ('Z','LAT CAPIT Z')");
         */

        String cyrillicSmallA = "\u0430";
        this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment) VALUES ('" + cyrillicSmallA + "','CYR SMALL A')");

        /*
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL BE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL VE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL GE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL DE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL IE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL IO')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL ZHE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL ZE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL I')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL KA')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL EL')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL EM')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL EN')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL O')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL PE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL ER')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL ES')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL TE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL U')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL EF')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL HA')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL TSE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL CHE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL SHA')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL SCHA')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL HARD SIGN')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL YERU')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL SOFT SIGN')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL E')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL YU')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR SMALL YA')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT A')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT BE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT VE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT GE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT DE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT IE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT IO')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT ZHE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT ZE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT I')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT KA')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT EL')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT EM')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT EN')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT O')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT PE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT ER')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT ES')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT TE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT U')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT EF')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT HA')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT TSE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT CHE')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT SHA')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT SCHA')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT HARD SIGN')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT YERU')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT SOFT SIGN')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT E')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT YU')");
         * this.stmt.executeUpdate("INSERT INTO t1 (koi8_ru_f,comment)
         * VALUES (_koi8r'?��','CYR CAPIT YA')");
         */

        this.stmt.executeUpdate("ALTER TABLE t1 ADD utf8_f CHAR(32) CHARACTER SET utf8 NOT NULL");
        this.stmt.executeUpdate("UPDATE t1 SET utf8_f=CONVERT(koi8_ru_f USING utf8)");
        this.stmt.executeUpdate("SET CHARACTER SET koi8r");
        // this.stmt.executeUpdate("SET CHARACTER SET UTF8");
        this.rs = this.stmt.executeQuery("SELECT * FROM t1");

        ResultSetMetaData rsmd = this.rs.getMetaData();

        int numColumns = rsmd.getColumnCount();

        for (int i = 0; i < numColumns; i++) {
            System.out.print(rsmd.getColumnName(i + 1));
            System.out.print("\t\t");
        }

        System.out.println();

        while (this.rs.next()) {
            System.out.println(this.rs.getString(1) + "\t\t" + this.rs.getString(2) + "\t\t" + this.rs.getString(3));

            if (this.rs.getString(1).equals("CYR SMALL A")) {
                this.rs.getString(2);
            }
        }

        System.out.println();

        this.stmt.executeUpdate("SET NAMES utf8");
        this.rs = this.stmt.executeQuery("SELECT _koi8r 0xC1;");

        rsmd = this.rs.getMetaData();

        numColumns = rsmd.getColumnCount();

        for (int i = 0; i < numColumns; i++) {
            System.out.print(rsmd.getColumnName(i + 1));
            System.out.print("\t\t");
        }

        System.out.println();

        while (this.rs.next()) {
            System.out.println(this.rs.getString(1).equals("\u0430") + "\t\t");
            System.out.println(new String(this.rs.getBytes(1), "KOI8_R"));

        }

        char[] c = new char[] { 0xd0b0 };

        System.out.println(new String(c));
        System.out.println("\u0430");
    }

    /**
     * Tests isolation level functionality
     * 
     * @throws Exception
     */
    @Test
    public void testIsolationLevel() throws Exception {
        // Check initial transaction isolation level
        ((MysqlConnection) this.conn).getPropertySet().getBooleanProperty(PropertyKey.useLocalSessionState).setValue(true);
        int initialTransactionIsolation = this.conn.getTransactionIsolation();

        ((MysqlConnection) this.conn).getPropertySet().getBooleanProperty(PropertyKey.useLocalSessionState).setValue(false);
        int actualTransactionIsolation = this.conn.getTransactionIsolation();

        assertEquals(actualTransactionIsolation, initialTransactionIsolation, "Inital transaction isolation level doesn't match the server's");

        // Check setting all allowed transaction isolation levels
        String[] isoLevelNames = new String[] { "Connection.TRANSACTION_NONE", "Connection.TRANSACTION_READ_COMMITTED",
                "Connection.TRANSACTION_READ_UNCOMMITTED", "Connection.TRANSACTION_REPEATABLE_READ", "Connection.TRANSACTION_SERIALIZABLE" };

        int[] isolationLevels = new int[] { Connection.TRANSACTION_NONE, Connection.TRANSACTION_READ_COMMITTED, Connection.TRANSACTION_READ_UNCOMMITTED,
                Connection.TRANSACTION_REPEATABLE_READ, Connection.TRANSACTION_SERIALIZABLE };

        DatabaseMetaData dbmd = this.conn.getMetaData();
        for (int i = 0; i < isolationLevels.length; i++) {
            if (dbmd.supportsTransactionIsolationLevel(isolationLevels[i])) {
                this.conn.setTransactionIsolation(isolationLevels[i]);

                assertTrue(this.conn.getTransactionIsolation() == isolationLevels[i] || this.conn.getTransactionIsolation() > isolationLevels[i],
                        "Transaction isolation level that was set (" + isoLevelNames[i]
                                + ") was not returned, nor was a more restrictive isolation level used by the server");
            }
        }
    }

    /**
     * Tests the savepoint functionality in MySQL.
     * 
     * @throws Exception
     */
    @Test
    public void testSavepoint() throws Exception {
        DatabaseMetaData dbmd = this.conn.getMetaData();

        if (dbmd.supportsSavepoints()) {
            System.out.println("Testing SAVEPOINTs");

            try {
                this.conn.setAutoCommit(true);

                createTable("testSavepoints", "(field1 int)", "InnoDB");

                // Try with named save points
                this.conn.setAutoCommit(false);
                this.stmt.executeUpdate("INSERT INTO testSavepoints VALUES (1)");

                Savepoint afterInsert = this.conn.setSavepoint("afterInsert");
                this.stmt.executeUpdate("UPDATE testSavepoints SET field1=2");

                Savepoint afterUpdate = this.conn.setSavepoint("afterUpdate");
                this.stmt.executeUpdate("DELETE FROM testSavepoints");

                assertTrue(getRowCount("testSavepoints") == 0, "Row count should be 0");
                this.conn.rollback(afterUpdate);
                assertTrue(getRowCount("testSavepoints") == 1, "Row count should be 1");
                assertTrue("2".equals(getSingleValue("testSavepoints", "field1", null).toString()), "Value should be 2");
                this.conn.rollback(afterInsert);
                assertTrue("1".equals(getSingleValue("testSavepoints", "field1", null).toString()), "Value should be 1");
                this.conn.rollback();
                assertTrue(getRowCount("testSavepoints") == 0, "Row count should be 0");

                // Try with 'anonymous' save points
                this.conn.rollback();

                this.stmt.executeUpdate("INSERT INTO testSavepoints VALUES (1)");
                afterInsert = this.conn.setSavepoint();
                this.stmt.executeUpdate("UPDATE testSavepoints SET field1=2");
                afterUpdate = this.conn.setSavepoint();
                this.stmt.executeUpdate("DELETE FROM testSavepoints");

                assertTrue(getRowCount("testSavepoints") == 0, "Row count should be 0");
                this.conn.rollback(afterUpdate);
                assertTrue(getRowCount("testSavepoints") == 1, "Row count should be 1");
                assertTrue("2".equals(getSingleValue("testSavepoints", "field1", null).toString()), "Value should be 2");
                this.conn.rollback(afterInsert);
                assertTrue("1".equals(getSingleValue("testSavepoints", "field1", null).toString()), "Value should be 1");
                this.conn.rollback();

                this.conn.releaseSavepoint(this.conn.setSavepoint());
            } finally {
                this.conn.setAutoCommit(true);
            }
        } else {
            System.out.println("MySQL version does not support SAVEPOINTs");
        }
    }

    /**
     * Tests the ability to set the connection collation via properties.
     * 
     * @throws Exception
     */
    @Test
    public void testNonStandardConnectionCollation() throws Exception {
        String collationToSet = "utf8_bin";
        String characterSet = "utf-8";

        Properties props = new Properties();
        props.setProperty(PropertyKey.connectionCollation.getKeyName(), collationToSet);
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), characterSet);

        Connection collConn = null;
        Statement collStmt = null;
        ResultSet collRs = null;

        try {
            collConn = getConnectionWithProps(props);

            collStmt = collConn.createStatement();

            collRs = collStmt.executeQuery("SHOW VARIABLES LIKE 'collation_connection'");

            assertTrue(collRs.next());
            assertTrue(collationToSet.equalsIgnoreCase(collRs.getString(2)));
        } finally {
            if (collConn != null) {
                collConn.close();
            }
        }
    }

    @Test
    public void testDumpQueriesOnException() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.dumpQueriesOnException.getKeyName(), "true");
        String bogusSQL = "SELECT 1 TO BAZ";
        Connection dumpConn = getConnectionWithProps(props);

        try {
            dumpConn.createStatement().executeQuery(bogusSQL);
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage().indexOf(bogusSQL) != -1);
        }

        try {
            ((com.mysql.cj.jdbc.JdbcConnection) dumpConn).clientPrepareStatement(bogusSQL).executeQuery();
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage().indexOf(bogusSQL) != -1);
        }

        try {
            createTable("testDumpQueriesOnException", "(field1 int UNIQUE)");
            this.stmt.executeUpdate("INSERT INTO testDumpQueriesOnException VALUES (1)");

            PreparedStatement pStmt = dumpConn.prepareStatement("INSERT INTO testDumpQueriesOnException VALUES (?)");
            pStmt.setInt(1, 1);
            pStmt.executeUpdate();
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage().indexOf("INSERT INTO testDumpQueriesOnException") != -1);
        }

        try {
            dumpConn.prepareStatement(bogusSQL);
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage().indexOf(bogusSQL) != -1);
        }
    }

    /**
     * Tests functionality of the ConnectionPropertiesTransform interface.
     * 
     * @throws Exception
     */
    @Test
    public void testConnectionPropertiesTransform() throws Exception {
        String transformClassName = SimpleTransformer.class.getName();

        Properties props = new Properties();

        props.setProperty(PropertyKey.propertiesTransform.getKeyName(), transformClassName);

        Properties transformedProps = ConnectionUrl.getConnectionUrlInstance(BaseTestCase.dbUrl, props).getConnectionArgumentsAsProperties();

        assertTrue("albequerque".equals(transformedProps.getProperty(PropertyKey.HOST.getKeyName())));
    }

    /**
     * Tests functionality of using URLs in 'LOAD DATA LOCAL INFILE' statements.
     * 
     * @throws Exception
     */
    @Test
    public void testLocalInfileWithUrl() throws Exception {
        File infile = File.createTempFile("foo", "txt");
        infile.deleteOnExit();
        String url = infile.toURI().toURL().toExternalForm();
        FileWriter output = new FileWriter(infile);
        output.write("Test");
        output.flush();
        output.close();

        createTable("testLocalInfileWithUrl", "(field1 LONGTEXT)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.allowLoadLocalInfile.getKeyName(), "true");
        props.setProperty(PropertyKey.allowUrlInLocalInfile.getKeyName(), "true");

        Connection loadConn = getConnectionWithProps(props);
        Statement loadStmt = loadConn.createStatement();

        String charset = " CHARACTER SET " + CharsetMapping.getMysqlCharsetForJavaEncoding(
                ((MysqlConnection) loadConn).getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue(),
                ((JdbcConnection) loadConn).getServerVersion());

        try {
            loadStmt.executeQuery("LOAD DATA LOCAL INFILE '" + url + "' INTO TABLE testLocalInfileWithUrl" + charset);
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();

            throw sqlEx;
        }

        this.rs = this.stmt.executeQuery("SELECT * FROM testLocalInfileWithUrl");
        assertTrue(this.rs.next());
        assertTrue("Test".equals(this.rs.getString(1)));
        int count = this.stmt.executeUpdate("DELETE FROM testLocalInfileWithUrl");
        assertTrue(count == 1);

        StringBuilder escapedPath = new StringBuilder();
        String path = infile.getCanonicalPath();

        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);

            if (c == '\\') {
                escapedPath.append('\\');
            }

            escapedPath.append(c);
        }

        loadStmt.execute("LOAD DATA LOCAL INFILE '" + escapedPath.toString() + "' INTO TABLE testLocalInfileWithUrl" + charset);
        this.rs = this.stmt.executeQuery("SELECT * FROM testLocalInfileWithUrl");
        assertTrue(this.rs.next());
        assertTrue("Test".equals(this.rs.getString(1)));

        try {
            loadStmt.executeQuery("LOAD DATA LOCAL INFILE 'foo:///' INTO TABLE testLocalInfileWithUrl" + charset);
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage() != null);
            assertTrue(sqlEx.getMessage().indexOf("FileNotFoundException") != -1);
        }
    }

    @Test
    public void testLocalInfileDisabled() throws Exception {
        createTable("testLocalInfileDisabled", "(field1 varchar(255))");

        File infile = File.createTempFile("foo", "txt");
        infile.deleteOnExit();
        //String url = infile.toURL().toExternalForm();
        FileWriter output = new FileWriter(infile);
        output.write("Test");
        output.flush();
        output.close();

        // Test load local infile support disabled via client capabilities by default.
        assertThrows(SQLSyntaxErrorException.class,
                versionMeetsMinimum(8, 0, 19) ? "Loading local data is disabled;.*" : "The used command is not allowed with this MySQL version", () -> {
                    this.stmt.executeUpdate("LOAD DATA LOCAL INFILE '" + infile.getCanonicalPath() + "' INTO TABLE testLocalInfileDisabled");
                    return null;
                });

        // Test load local infile support enabled via client capabilities but disabled on the connector.
        Properties props = new Properties();
        props.setProperty(PropertyKey.allowLoadLocalInfile.getKeyName(), "true");
        Connection loadConn = getConnectionWithProps(props);

        try {
            // Must be set after connect, otherwise it's the server that's enforcing it.
            ((com.mysql.cj.jdbc.JdbcConnection) loadConn).getPropertySet().getProperty(PropertyKey.allowLoadLocalInfile).setValue(false);

            assertThrows(SQLException.class,
                    "Server asked for stream in response to \"LOAD DATA LOCAL INFILE\" but functionality is not enabled at client by setting "
                            + "\"allowLoadLocalInfile=true\" or specifying a path with 'allowLoadLocalInfileInPath'\\.",
                    () -> {
                        loadConn.createStatement().execute("LOAD DATA LOCAL INFILE '" + infile.getCanonicalPath() + "' INTO TABLE testLocalInfileDisabled");
                        return null;
                    });

            assertFalse(loadConn.createStatement().executeQuery("SELECT * FROM testLocalInfileDisabled").next());
        } finally {
            loadConn.close();
        }
    }

    @Test
    public void testServerConfigurationCache() throws Exception {
        Properties props = new Properties();

        props.setProperty(PropertyKey.cacheServerConfiguration.getKeyName(), "true");
        props.setProperty(PropertyKey.profileSQL.getKeyName(), "true");
        props.setProperty(PropertyKey.logger.getKeyName(), BufferingLogger.class.getName());

        Connection conn1 = null;
        Connection conn2 = null;
        try {
            conn1 = getConnectionWithProps(props);

            try {
                // eliminate side-effects when not run in isolation
                BufferingLogger.startLoggingToBuffer();

                conn2 = getConnectionWithProps(props);

                assertTrue(BufferingLogger.getBuffer().toString().indexOf("SHOW VARIABLES") == -1, "Configuration wasn't cached");

                assertTrue(BufferingLogger.getBuffer().toString().indexOf("SHOW COLLATION") == -1, "Configuration wasn't cached");
            } finally {
                BufferingLogger.dropBuffer();
            }
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
            if (conn2 != null) {
                conn2.close();
            }
        }
    }

    /**
     * Tests whether or not the configuration 'useLocalSessionState' actually prevents non-needed 'set autocommit=', 'set session transaction isolation ...'
     * and 'show variables like tx_isolation' queries.
     * 
     * @throws Exception
     */
    @Test
    public void testUseLocalSessionState() throws Exception {
        Properties props = new Properties();

        props.setProperty(PropertyKey.useLocalSessionState.getKeyName(), "true");
        props.setProperty(PropertyKey.profileSQL.getKeyName(), "true");
        props.setProperty(PropertyKey.logger.getKeyName(), BufferingLogger.class.getName());

        Connection conn1 = getConnectionWithProps(props);
        conn1.setAutoCommit(true);
        conn1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

        BufferingLogger.startLoggingToBuffer();

        conn1.setAutoCommit(true);
        conn1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        conn1.getTransactionIsolation();

        String logAsString = BufferingLogger.getBuffer().toString();

        String s = versionMeetsMinimum(8, 0, 3) ? "transaction_isolation" : "tx_isolation";

        assertTrue(logAsString.indexOf("SET SESSION") == -1 && logAsString.indexOf("SHOW VARIABLES LIKE '" + s + "'") == -1
                && logAsString.indexOf("SET autocommit=") == -1);
    }

    /**
     * Tests whether re-connect with non-read-only connection can happen.
     * 
     * @throws Exception
     */
    @Test
    public void testFailoverConnection() throws Exception {

        if (!isServerRunningOnWindows()) { // windows sockets don't work for this test
            Properties props = new Properties();
            props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
            props.setProperty(PropertyKey.failOverReadOnly.getKeyName(), "false");

            Connection failoverConnection = null;

            try {
                failoverConnection = getFailoverConnection(props);

                String originalConnectionId = getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT connection_id()").toString();
                System.out.println("Original Connection Id = " + originalConnectionId);

                assertTrue(!failoverConnection.isReadOnly(), "Connection should not be in READ_ONLY state");

                // Kill the connection
                this.stmt.executeUpdate("KILL " + originalConnectionId);

                // This takes a bit to occur

                Thread.sleep(3000);

                try {
                    failoverConnection.createStatement().execute("SELECT 1");
                    fail("We expect an exception here, because the connection should be gone until the reconnect code picks it up again");
                } catch (SQLException sqlEx) {
                    // do-nothing
                }

                // Tickle re-connect

                failoverConnection.setAutoCommit(true);

                String newConnectionId = getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT connection_id()").toString();
                System.out.println("new Connection Id = " + newConnectionId);

                assertTrue(!newConnectionId.equals(originalConnectionId), "We should have a new connection to the server in this case");
                assertTrue(!failoverConnection.isReadOnly(), "Connection should not be read-only");
            } finally {
                if (failoverConnection != null) {
                    failoverConnection.close();
                }
            }
        }
    }

    @Test
    public void testCannedConfigs() throws Exception {
        Properties cannedProps = ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:///?useConfigs=clusterBase", null).getConnectionArgumentsAsProperties();

        assertTrue("true".equals(cannedProps.getProperty(PropertyKey.autoReconnect.getKeyName())));
        assertTrue("false".equals(cannedProps.getProperty(PropertyKey.failOverReadOnly.getKeyName())));

        // this will fail, but we test that too
        assertThrows(InvalidConnectionAttributeException.class, "Can't find configuration template named 'clusterBase2'", new Callable<Void>() {
            public Void call() throws Exception {
                try {
                    ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:///?useConfigs=clusterBase,clusterBase2", null);
                    return null;
                } catch (Throwable t) {
                    t.printStackTrace();
                    throw t;
                }
            }
        });
    }

    /**
     * Checks implementation of 'dontTrackOpenResources' property.
     * 
     * @throws Exception
     */
    @Test
    public void testDontTrackOpenResources() throws Exception {
        Properties props = new Properties();

        props.setProperty(PropertyKey.dontTrackOpenResources.getKeyName(), "true");
        Connection noTrackConn = null;
        Statement noTrackStatement = null;
        PreparedStatement noTrackPstmt = null;
        ResultSet rs2 = null;

        try {
            noTrackConn = getConnectionWithProps(props);
            noTrackStatement = noTrackConn.createStatement();
            noTrackPstmt = noTrackConn.prepareStatement("SELECT 1");
            rs2 = noTrackPstmt.executeQuery();
            rs2.next();

            this.rs = noTrackStatement.executeQuery("SELECT 1");
            this.rs.next();

            noTrackConn.close();

            // Under 'strict' JDBC requirements, these calls should fail
            // (and _do_ if dontTrackOpenResources == false)

            this.rs.getString(1);
            rs2.getString(1);
        } finally {
            if (rs2 != null) {
                rs2.close();
            }

            if (noTrackStatement != null) {
                noTrackStatement.close();
            }

            if (noTrackConn != null && !noTrackConn.isClosed()) {
                noTrackConn.close();
            }
        }
    }

    @Test
    public void testPing() throws SQLException {
        Connection conn2 = getConnectionWithProps((String) null);

        ((com.mysql.cj.jdbc.JdbcConnection) conn2).ping();
        conn2.close();

        try {
            ((com.mysql.cj.jdbc.JdbcConnection) conn2).ping();
            fail("Should have failed with an exception");
        } catch (SQLException sqlEx) {
            // ignore for now
        }

        //
        // This feature caused BUG#8975, so check for that too!

        Properties props = new Properties();
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");

        getConnectionWithProps(props);
    }

    @Test
    public void testSessionVariables() throws Exception {
        String getInitialWaitTimeout = getMysqlVariable("wait_timeout");

        int newWaitTimeout = Integer.parseInt(getInitialWaitTimeout) + 10000;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sessionVariables.getKeyName(), "wait_timeout=" + newWaitTimeout);
        props.setProperty(PropertyKey.profileSQL.getKeyName(), "true");

        Connection varConn = getConnectionWithProps(props);

        assertTrue(!getInitialWaitTimeout.equals(getMysqlVariable(varConn, "wait_timeout")));
    }

    /**
     * Tests setting profileSQL on/off in the span of one connection.
     * 
     * @throws Exception
     */
    @Test
    public void testSetProfileSql() throws Exception {
        ((com.mysql.cj.jdbc.JdbcConnection) this.conn).getPropertySet().getProperty(PropertyKey.profileSQL).setValue(false);
        this.stmt.execute("SELECT 1");
        ((com.mysql.cj.jdbc.JdbcConnection) this.conn).getPropertySet().getProperty(PropertyKey.profileSQL).setValue(true);
        this.stmt.execute("SELECT 1");
    }

    @Test
    public void testCreateDatabaseIfNotExist() throws Exception {
        String databaseName = "testcreatedatabaseifnotexist";

        this.stmt.executeUpdate("DROP DATABASE IF EXISTS " + databaseName);

        Properties props = new Properties();
        props.setProperty(PropertyKey.createDatabaseIfNotExist.getKeyName(), "true");
        props.setProperty(PropertyKey.DBNAME.getKeyName(), databaseName);

        Connection con = getConnectionWithProps(props);

        this.rs = this.stmt.executeQuery("show databases like '" + databaseName + "'");
        if (this.rs.next()) {
            assertEquals(databaseName, this.rs.getString(1));
        } else {
            fail("Database " + databaseName + " is not found.");
        }

        con.createStatement().executeUpdate("DROP DATABASE IF EXISTS " + databaseName);
    }

    /**
     * Tests if gatherPerfMetrics works.
     * 
     * @throws Exception
     */
    @Test
    public void testGatherPerfMetrics() throws Exception {
        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
            props.setProperty(PropertyKey.logSlowQueries.getKeyName(), "true");
            props.setProperty(PropertyKey.slowQueryThresholdMillis.getKeyName(), "2000");
            // these properties were reported as the cause of NullPointerException
            props.setProperty(PropertyKey.gatherPerfMetrics.getKeyName(), "true");
            props.setProperty(PropertyKey.reportMetricsIntervalMillis.getKeyName(), "3000");

            Connection conn1 = getConnectionWithProps(props);
            Statement stmt1 = conn1.createStatement();
            ResultSet rs1 = stmt1.executeQuery("SELECT 1");
            rs1.next();
            conn1.close();
        } catch (NullPointerException e) {
            e.printStackTrace();
            fail();
        }
    }

    /**
     * Tests if useCompress works.
     * 
     * @throws Exception
     */
    @Test
    public void testUseCompress() throws Exception {
        this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'max_allowed_packet'");
        this.rs.next();
        if (this.rs.getInt(2) < 4 + 1024 * 1024 * 16 - 1) {
            fail("You need to increase max_allowed_packet to at least " + (4 + 1024 * 1024 * 16 - 1) + " before running this test!");
        }

        if (versionMeetsMinimum(5, 6, 20) && !versionMeetsMinimum(5, 7)) {
            /*
             * The 5.6.20 patch for Bug #16963396, Bug #19030353, Bug #69477 limits the size of redo log BLOB writes
             * to 10% of the redo log file size. The 5.7.5 patch addresses the bug without imposing a limitation.
             * As a result of the redo log BLOB write limit introduced for MySQL 5.6, innodb_log_file_size should be set to a value
             * greater than 10 times the largest BLOB data size found in the rows of your tables plus the length of other variable length
             * fields (VARCHAR, VARBINARY, and TEXT type fields).
             */
            this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_log_file_size'");
            this.rs.next();
            if (this.rs.getInt(2) < 1024 * 1024 * 32 * 10) {
                fail("You need to increase innodb_log_file_size to at least " + (1024 * 1024 * 32 * 10) + " before running this test!");
            }
        }

        testCompressionWith("false", 1024 * 1024 * 16 - 2); // no split
        testCompressionWith("false", 1024 * 1024 * 16 - 1); // split with additional empty packet
        testCompressionWith("false", 1024 * 1024 * 32);   // big payload

        testCompressionWith("true", 1024 * 1024 * 16 - 2 - 3); // no split, one compressed packet
        testCompressionWith("true", 1024 * 1024 * 16 - 2 - 2); // no split, two compressed packets
        testCompressionWith("true", 1024 * 1024 * 16 - 1);   // split with additional empty packet, two compressed packets
        testCompressionWith("true", 1024 * 1024 * 32);     // big payload
    }

    /**
     * @param useCompression
     * @param maxPayloadSize
     * 
     * @throws Exception
     */
    private void testCompressionWith(String useCompression, int maxPayloadSize) throws Exception {
        String sqlToSend = "INSERT INTO BLOBTEST(blobdata) VALUES (?)";
        int requiredSize = maxPayloadSize - sqlToSend.length() - "_binary''".length();

        File testBlobFile = File.createTempFile("cmj-testblob", ".dat");
        testBlobFile.deleteOnExit();

        // TODO: following cleanup doesn't work correctly during concurrent execution of testsuite 
        // cleanupTempFiles(testBlobFile, "cmj-testblob");

        BufferedOutputStream bOut = new BufferedOutputStream(new FileOutputStream(testBlobFile));

        // generate a random sequence of letters. this ensures that no escaped characters cause packet sizes that interfere with bounds tests
        Random random = new Random();
        for (int i = 0; i < requiredSize; i++) {
            bOut.write((byte) (65 + random.nextInt(26)));
        }

        bOut.flush();
        bOut.close();

        Properties props = new Properties();
        props.setProperty(PropertyKey.useCompression.getKeyName(), useCompression);
        Connection conn1 = getConnectionWithProps(props);
        Statement stmt1 = conn1.createStatement();

        createTable("BLOBTEST", "(pos int PRIMARY KEY auto_increment, blobdata LONGBLOB)");
        BufferedInputStream bIn = new BufferedInputStream(new FileInputStream(testBlobFile));

        this.pstmt = conn1.prepareStatement(sqlToSend);

        this.pstmt.setBinaryStream(1, bIn, (int) testBlobFile.length());
        this.pstmt.execute();
        this.pstmt.clearParameters();

        this.rs = stmt1.executeQuery("SELECT blobdata from BLOBTEST LIMIT 1");
        this.rs.next();
        InputStream is = this.rs.getBinaryStream(1);

        bIn.close();
        bIn = new BufferedInputStream(new FileInputStream(testBlobFile));
        int blobbyte = 0;
        int count = 0;
        while ((blobbyte = is.read()) > -1) {
            int filebyte = bIn.read();
            if (filebyte < 0 || filebyte != blobbyte) {
                fail("Blob is not identical to initial data.");
            }
            count++;
        }
        assertEquals(requiredSize, count);

        is.close();
        if (bIn != null) {
            bIn.close();
        }
    }

    /**
     * Tests feature of "localSocketAddress", by enumerating local IF's and trying each one in turn. This test might take a long time to run, since we can't set
     * timeouts if we're using localSocketAddress. We try and keep the time down on the testcase by spawning the checking of each interface off into separate
     * threads.
     * 
     * @throws Exception
     *             if the test can't use at least one of the local machine's interfaces to make an outgoing connection to the server.
     */
    @Test
    public void testLocalSocketAddress() throws Exception {
        Enumeration<NetworkInterface> allInterfaces = NetworkInterface.getNetworkInterfaces();

        SpawnedWorkerCounter counter = new SpawnedWorkerCounter();

        List<LocalSocketAddressCheckThread> allChecks = new ArrayList<>();

        while (allInterfaces.hasMoreElements()) {
            NetworkInterface intf = allInterfaces.nextElement();

            Enumeration<InetAddress> allAddresses = intf.getInetAddresses();

            allChecks.add(new LocalSocketAddressCheckThread(allAddresses, counter));
        }

        counter.setWorkerCount(allChecks.size());

        for (LocalSocketAddressCheckThread t : allChecks) {
            t.start();
        }

        // Wait for tests to complete....
        synchronized (counter) {

            while (counter.workerCount > 0 /* safety valve */) {

                counter.wait();

                if (counter.workerCount == 0) {
                    System.out.println("Done!");
                    break;
                }
            }
        }

        boolean didOneWork = false;
        boolean didOneFail = false;

        for (LocalSocketAddressCheckThread t : allChecks) {
            if (t.atLeastOneWorked) {
                didOneWork = true;

                break;
            }
            if (!didOneFail) {
                didOneFail = true;
            }
        }

        assertTrue(didOneWork, "At least one connection was made with the localSocketAddress set");

        String hostname = getHostFromTestsuiteUrl();

        if (!hostname.startsWith(":") && !hostname.startsWith("localhost")) {

            int indexOfColon = hostname.indexOf(":");

            if (indexOfColon != -1) {
                hostname = hostname.substring(0, indexOfColon);
            }

            boolean isLocalIf = false;

            isLocalIf = (null != NetworkInterface.getByName(hostname));

            if (!isLocalIf) {
                try {
                    isLocalIf = (null != NetworkInterface.getByInetAddress(InetAddress.getByName(hostname)));
                } catch (Throwable t) {
                    isLocalIf = false;
                }
            }

            if (!isLocalIf) {
                assertTrue(didOneFail, "At least one connection didn't fail with localSocketAddress set");
            }
        }
    }

    class SpawnedWorkerCounter {
        protected int workerCount = 0;

        synchronized void setWorkerCount(int i) {
            this.workerCount = i;
        }

        synchronized void decrementWorkerCount() {
            this.workerCount--;
            notify();
        }
    }

    class LocalSocketAddressCheckThread extends Thread {
        boolean atLeastOneWorked = false;
        Enumeration<InetAddress> allAddresses = null;
        SpawnedWorkerCounter counter = null;

        LocalSocketAddressCheckThread(Enumeration<InetAddress> e, SpawnedWorkerCounter c) {
            this.allAddresses = e;
            this.counter = c;
        }

        @Override
        public void run() {

            while (this.allAddresses.hasMoreElements()) {
                InetAddress addr = this.allAddresses.nextElement();

                try {
                    Properties props = new Properties();
                    props.setProperty(PropertyKey.localSocketAddress.getKeyName(), addr.getHostAddress());
                    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "2000");
                    getConnectionWithProps(props).close();

                    this.atLeastOneWorked = true;

                    break;
                } catch (SQLException sqlEx) {
                    // ignore, we're only seeing if one of these tests succeeds
                }
            }

            this.counter.decrementWorkerCount();
        }
    }

    @Test
    public void testUsageAdvisorTooLargeResultSet() throws Exception {
        Connection uaConn = null;

        PrintStream stderr = System.err;

        BufferingLogger.startLoggingToBuffer();

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.useUsageAdvisor.getKeyName(), "true");
            props.setProperty(PropertyKey.resultSetSizeThreshold.getKeyName(), "4");
            props.setProperty(PropertyKey.logger.getKeyName(), BufferingLogger.class.getName());

            uaConn = getConnectionWithProps(props);
            this.rs = uaConn.createStatement().executeQuery("SHOW VARIABLES");
            this.rs.close();

            assertTrue(BufferingLogger.getBuffer().toString().indexOf("larger than \"resultSetSizeThreshold\" of 4 rows") != -1,
                    "Result set threshold message not present");
        } finally {
            BufferingLogger.dropBuffer();
            System.setErr(stderr);

            if (uaConn != null) {
                uaConn.close();
            }
        }
    }

    @Test
    public void testUseLocalSessionStateRollback() throws Exception {
        if (!versionMeetsMinimum(5, 5, 0)) {
            return;
        }

        Properties props = new Properties();
        props.setProperty(PropertyKey.useLocalSessionState.getKeyName(), "true");
        props.setProperty(PropertyKey.useLocalTransactionState.getKeyName(), "true");
        props.setProperty(PropertyKey.profileSQL.getKeyName(), "true");
        props.setProperty(PropertyKey.logger.getKeyName(), BufferingLogger.class.getName());

        BufferingLogger.startLoggingToBuffer();

        createTable("testUseLocalSessionState", "(field1 varchar(32))", "InnoDB");

        Connection localStateConn = null;
        Statement localStateStmt = null;
        String searchIn = "";

        try {
            localStateConn = getConnectionWithProps(props);
            localStateStmt = localStateConn.createStatement();

            localStateConn.setAutoCommit(false);
            localStateStmt.executeUpdate("INSERT INTO testUseLocalSessionState VALUES ('abc')");
            localStateConn.rollback();
            localStateConn.rollback();
            localStateStmt.executeUpdate("INSERT INTO testUseLocalSessionState VALUES ('abc')");
            localStateConn.commit();
            localStateConn.commit();
            localStateStmt.close();
        } finally {
            searchIn = BufferingLogger.getBuffer().toString();
            BufferingLogger.dropBuffer();

            if (localStateStmt != null) {
                localStateStmt.close();
            }

            if (localStateConn != null) {
                localStateConn.close();
            }
        }

        int rollbackCount = 0;
        int rollbackPos = 0;

        // space is important here, we don't want to count occurrences in stack traces
        while (rollbackPos != -1) {
            rollbackPos = searchIn.indexOf(" rollback", rollbackPos);

            if (rollbackPos != -1) {
                rollbackPos += "rollback".length();
                rollbackCount++;
            }
        }

        assertEquals(1, rollbackCount);

        int commitCount = 0;
        int commitPos = 0;

        // space is important here, we don't want to count "autocommit" nor occurrences in stack traces
        while (commitPos != -1) {
            commitPos = searchIn.indexOf(" commit", commitPos);

            if (commitPos != -1) {
                commitPos += " commit".length();
                commitCount++;
            }
        }

        assertEquals(1, commitCount);
    }

    /**
     * Checks if setting useCursorFetch to "true" automatically enables server-side prepared statements.
     * 
     * @throws Exception
     */
    @Test
    public void testCouplingOfCursorFetch() throws Exception {
        Connection fetchConn = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false"); // force the issue
            props.setProperty(PropertyKey.useCursorFetch.getKeyName(), "true");
            fetchConn = getConnectionWithProps(props);

            String classname = com.mysql.cj.jdbc.ServerPreparedStatement.class.getName();

            assertEquals(classname, fetchConn.prepareStatement("SELECT 1").getClass().getName());
        } finally {
            if (fetchConn != null) {
                fetchConn.close();
            }
        }
    }

    @Test
    public void testInterfaceImplementation() throws Exception {
        testInterfaceImplementation(getConnectionWithProps((Properties) null));
        MysqlConnectionPoolDataSource cpds = new MysqlConnectionPoolDataSource();
        cpds.setUrl(dbUrl);
        testInterfaceImplementation(cpds.getPooledConnection().getConnection());
    }

    @Test
    private void testInterfaceImplementation(Connection connToCheck) throws Exception {
        Method[] dbmdMethods = java.sql.DatabaseMetaData.class.getMethods();

        // can't do this statically, as we return different
        // implementations depending on JDBC version
        DatabaseMetaData dbmd = connToCheck.getMetaData();

        checkInterfaceImplemented(dbmdMethods, dbmd.getClass(), dbmd);

        Statement stmtToCheck = connToCheck.createStatement();

        checkInterfaceImplemented(java.sql.Statement.class.getMethods(), stmtToCheck.getClass(), stmtToCheck);

        PreparedStatement pStmtToCheck = connToCheck.prepareStatement("SELECT 1");
        ParameterMetaData paramMd = pStmtToCheck.getParameterMetaData();

        checkInterfaceImplemented(java.sql.PreparedStatement.class.getMethods(), pStmtToCheck.getClass(), pStmtToCheck);
        checkInterfaceImplemented(java.sql.ParameterMetaData.class.getMethods(), paramMd.getClass(), paramMd);

        pStmtToCheck = ((com.mysql.cj.jdbc.JdbcConnection) connToCheck).serverPrepareStatement("SELECT 1");

        checkInterfaceImplemented(java.sql.PreparedStatement.class.getMethods(), pStmtToCheck.getClass(), pStmtToCheck);
        ResultSet toCheckRs = connToCheck.createStatement().executeQuery("SELECT 1");
        checkInterfaceImplemented(java.sql.ResultSet.class.getMethods(), toCheckRs.getClass(), toCheckRs);
        toCheckRs = connToCheck.createStatement().executeQuery("SELECT 1");
        checkInterfaceImplemented(java.sql.ResultSetMetaData.class.getMethods(), toCheckRs.getMetaData().getClass(), toCheckRs.getMetaData());

        createProcedure("interfaceImpl", "(IN p1 INT)\nBEGIN\nSELECT 1;\nEND");

        CallableStatement cstmt = connToCheck.prepareCall("{CALL interfaceImpl(?)}");

        checkInterfaceImplemented(java.sql.CallableStatement.class.getMethods(), cstmt.getClass(), cstmt);
        checkInterfaceImplemented(java.sql.Connection.class.getMethods(), connToCheck.getClass(), connToCheck);
    }

    private void checkInterfaceImplemented(Method[] interfaceMethods, Class<?> implementingClass, Object invokeOn) throws NoSuchMethodException {
        for (int i = 0; i < interfaceMethods.length; i++) {
            Method toFind = interfaceMethods[i];
            Method toMatch = implementingClass.getMethod(toFind.getName(), toFind.getParameterTypes());
            assertNotNull(toMatch, toFind.toString());
            Class<?> paramTypes[] = toFind.getParameterTypes();

            Object[] args = new Object[paramTypes.length];
            fillPrimitiveDefaults(paramTypes, args, paramTypes.length);

            try {
                toMatch.invoke(invokeOn, args);
            } catch (IllegalArgumentException e) {

            } catch (IllegalAccessException e) {

            } catch (InvocationTargetException e) {

            } catch (java.lang.AbstractMethodError e) {
                throw e;
            }
        }
    }

    @Test
    public void testNonVerifyServerCert() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");
        props.setProperty(PropertyKey.requireSSL.getKeyName(), "true");
        getConnectionWithProps(props);
    }

    @Test
    public void testSelfDestruct() throws Exception {
        Connection selfDestructingConn = getConnectionWithProps("selfDestructOnPingMaxOperations=2");

        boolean failed = false;

        for (int i = 0; i < 20; i++) {
            selfDestructingConn.createStatement().execute("SELECT 1");

            try {
                selfDestructingConn.createStatement().executeQuery("/* ping */ SELECT 1");
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();

                assertEquals("08S01", sqlState);

                failed = true;

                break;
            }
        }

        if (!failed) {
            fail("Connection should've self-destructed");
        }

        failed = false;

        selfDestructingConn = getConnectionWithProps("selfDestructOnPingSecondsLifetime=1");

        for (int i = 0; i < 20; i++) {
            selfDestructingConn.createStatement().execute("SELECT SLEEP(1)");

            try {
                selfDestructingConn.createStatement().executeQuery("/* ping */ SELECT 1");
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();

                assertEquals("08S01", sqlState);

                failed = true;

                break;
            }
        }

        if (!failed) {
            fail("Connection should've self-destructed");
        }
    }

    @Test
    public void testLifecyleInterceptor() throws Exception {
        createTable("testLifecycleInterceptor", "(field1 int)", "InnoDB");
        Connection liConn = null;

        try {
            liConn = getConnectionWithProps("connectionLifecycleInterceptors=testsuite.simple.TestLifecycleInterceptor");
            liConn.setAutoCommit(false);

            liConn.createStatement().executeUpdate("INSERT INTO testLifecycleInterceptor VALUES (1)");
            liConn.commit();
            assertEquals(TestLifecycleInterceptor.transactionsBegun, 1);
            assertEquals(TestLifecycleInterceptor.transactionsCompleted, 1);
            liConn.createStatement().execute("SELECT * FROM testLifecycleInterceptor");
            assertEquals(TestLifecycleInterceptor.transactionsBegun, 2);
            // implicit commit
            liConn.createStatement().executeUpdate("CREATE TABLE testLifecycleFoo (field1 int)");
            assertEquals(TestLifecycleInterceptor.transactionsCompleted, 2);
        } finally {
            if (liConn != null) {
                liConn.createStatement().executeUpdate("DROP TABLE IF EXISTS testLifecycleFoo");
                liConn.close();
            }
        }
    }

    @Test
    public void testNewHostParsing() throws Exception {
        Properties parsedProps = getPropertiesFromTestsuiteUrl();
        String host = parsedProps.getProperty(PropertyKey.HOST.getKeyName());
        String port = parsedProps.getProperty(PropertyKey.PORT.getKeyName());
        String user = parsedProps.getProperty(PropertyKey.USER.getKeyName());
        String password = parsedProps.getProperty(PropertyKey.PASSWORD.getKeyName());
        String database = parsedProps.getProperty(PropertyKey.DBNAME.getKeyName());

        String newUrl = String.format("jdbc:mysql:aws://address=(protocol=tcp)(host=%s)(port=%s)(user=%s)(password=%s)/%s", TestUtils.encodePercent(host), port,
                user != null ? user : "", password != null ? password : "", database);

        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        props.remove(PropertyKey.USER.getKeyName());
        props.remove(PropertyKey.PASSWORD.getKeyName());
        props.remove(PropertyKey.DBNAME.getKeyName());

        try {
            getConnectionWithProps(newUrl, props);
        } catch (SQLException sqlEx) {
            throw new RuntimeException("Failed to connect with URL " + newUrl, sqlEx);
        }
    }

    @Test
    public void testCompression() throws Exception {
        Connection compressedConn = getConnectionWithProps("useCompression=true,maxAllowedPacket=33554432");
        Statement compressedStmt = compressedConn.createStatement();
        compressedStmt.setFetchSize(Integer.MIN_VALUE);
        this.rs = compressedStmt.executeQuery("select repeat('a', 256 * 256 * 256 - 5)");
        this.rs.next();
        String str = this.rs.getString(1);

        assertEquals((256 * 256 * 256 - 5), str.length());

        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) != 'a') {
                fail();
            }
        }
    }

    @Disabled
    @Test
    public void testIsLocal() throws Exception {
        Properties parsedProps = getPropertiesFromTestsuiteUrl();
        String host = parsedProps.getProperty(PropertyKey.HOST.getKeyName(), "localhost");

        if (host.equals("localhost") || host.equals("127.0.0.1")) {
            // we can actually test this
            assertTrue(((com.mysql.cj.jdbc.ConnectionImpl) this.conn).isServerLocal());
        }
    }

    @Test
    public void testReadOnly56() throws Exception {
        if (!versionMeetsMinimum(5, 6, 5)) {
            return;
        }
        try {
            Connection notLocalState = getConnectionWithProps("profileSQL=true,logger=" + BufferingLogger.class.getName());

            for (int i = 0; i < 2; i++) {
                BufferingLogger.startLoggingToBuffer();
                notLocalState.setReadOnly(true);
                assertTrue(BufferingLogger.getBuffer().toString().indexOf("set session transaction read only") != -1);
                notLocalState.createStatement().execute("set session transaction read write");
                assertFalse(notLocalState.isReadOnly());
            }

            for (int i = 0; i < 2; i++) {
                BufferingLogger.startLoggingToBuffer();
                notLocalState.setReadOnly(false);
                assertTrue(BufferingLogger.getBuffer().toString().indexOf("set session transaction read write") != -1);
                notLocalState.createStatement().execute("set session transaction read only");
                assertTrue(notLocalState.isReadOnly());
            }

            Connection localState = getConnectionWithProps("profileSQL=true,useLocalSessionState=true,logger=" + BufferingLogger.class.getName());

            String s = versionMeetsMinimum(8, 0, 3) ? "@@session.transaction_read_only" : "@@session.tx_read_only";

            for (int i = 0; i < 2; i++) {
                BufferingLogger.startLoggingToBuffer();
                localState.setReadOnly(true);
                if (i == 0) {
                    assertTrue(BufferingLogger.getBuffer().toString().indexOf("set session transaction read only") != -1);
                } else {
                    assertTrue(BufferingLogger.getBuffer().toString().indexOf("set session transaction read only") == -1);
                }
                BufferingLogger.startLoggingToBuffer();
                localState.isReadOnly();
                assertTrue(BufferingLogger.getBuffer().toString().indexOf("select @@session." + s) == -1);
            }

            Connection noOptimization = getConnectionWithProps("profileSQL=true,readOnlyPropagatesToServer=false,logger=" + BufferingLogger.class.getName());

            for (int i = 0; i < 2; i++) {
                BufferingLogger.startLoggingToBuffer();
                noOptimization.setReadOnly(true);
                assertTrue(BufferingLogger.getBuffer().toString().indexOf("set session transaction read only") == -1);
                BufferingLogger.startLoggingToBuffer();
                noOptimization.isReadOnly();
                assertTrue(BufferingLogger.getBuffer().toString().indexOf("select @@session." + s) == -1);
            }
        } finally {
            BufferingLogger.dropBuffer();
        }
    }

    /**
     * IPv6 Connection test.
     * 
     * @throws Exception
     */
    @Test
    @Disabled // This test is disabled since Github Actions doesn't support IPv6
    public void testIPv6() throws Exception {
        if (!versionMeetsMinimum(5, 6)) {
            return;
            // this test could work with MySQL 5.5 but requires specific server configuration, e.g. "--bind-address=::"
        }

        String testUser = "testIPv6User";
        createUser("'" + testUser + "'@'%'", "IDENTIFIED BY '" + testUser + "'");
        this.stmt.execute("GRANT ALL ON *.* TO '" + testUser + "'@'%'");

        Properties connProps = getHostFreePropertiesFromTestsuiteUrl();
        connProps.setProperty(PropertyKey.USER.getKeyName(), testUser);
        connProps.setProperty(PropertyKey.PASSWORD.getKeyName(), testUser);

        List<Inet6Address> ipv6List = TestUtils.getIpv6List();
        List<String> ipv6Addrs = ipv6List.stream().map((e) -> e.getHostAddress()).collect(Collectors.toList());
        ipv6Addrs.add("::1"); // IPv6 loopback
        int port = getPortFromTestsuiteUrl();

        boolean atLeastOne = false;
        for (String host : ipv6Addrs) {
            if (TestUtils.serverListening(host, port)) {
                atLeastOne = true;
                String ipv6Url = String.format("jdbc:mysql:aws://address=(protocol=tcp)(host=%s)(port=%d)", TestUtils.encodePercent(host), port);

                Connection testConn = null;
                Statement testStmt = null;
                ResultSet testRs = null;

                testConn = DriverManager.getConnection(ipv6Url, connProps);
                testStmt = testConn.createStatement();
                testRs = testStmt.executeQuery("SELECT USER()");

                assertTrue(testRs.next());
                assertTrue(testRs.getString(1).startsWith(testUser));

                testRs.close();
                testStmt.close();
                testConn.close();
            }
        }

        if (!atLeastOne) {
            fail("None of the tested hosts have server sockets listening on the port " + port + ". This test requires a MySQL server running in local host.");
        }
    }

    /**
     * Test for Driver.acceptsURL() behavior clarification:
     * - acceptsURL() throws SQLException if URL is null.
     */
    @Test
    public void testDriverAcceptsURLNullArgument() {
        assertThrows(SQLException.class, "The database URL cannot be null.", new Callable<Void>() {
            public Void call() throws Exception {
                Driver mysqlDriver = new Driver();
                mysqlDriver.acceptsURL(null);
                return null;
            }
        });
    }

    /**
     * Test for Driver.connect() behavior clarifications:
     * - connect() throws SQLException if URL is null.
     * 
     * @throws Exception
     */
    @Test
    public void testDriverConnectNullArgument() throws Exception {
        assertThrows(SQLException.class,
                "Cannot load connection class because of underlying exception: com.mysql.cj.exceptions.WrongArgumentException: The database URL cannot be null.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        Driver mysqlDriver = new Driver();
                        mysqlDriver.connect(null, null);
                        return null;
                    }
                });

        assertThrows(SQLException.class, "The url cannot be null", new Callable<Void>() {
            public Void call() throws Exception {
                DriverManager.getConnection(null);
                return null;
            }
        });
    }

    /**
     * Test for Driver.connect() behavior clarifications:
     * - connect() properties precedence is implementation-defined.
     * 
     * @throws Exception
     */
    @Test
    public void testDriverConnectPropertiesPrecedence() throws Exception {
        assertThrows(SQLException.class, "Access denied for user 'dummy'@'[^']+' \\(using password: YES\\)", new Callable<Void>() {
            public Void call() throws Exception {
                DriverManager.getConnection(BaseTestCase.dbUrl, "dummy", "dummy");
                return null;
            }
        });

        // make sure the connection string doesn't contain 'maxRows'
        String testUrl = BaseTestCase.dbUrl;
        int b = testUrl.indexOf("maxRows");
        if (b != -1) {
            int e = testUrl.indexOf('&', b);
            if (e == -1) {
                e = testUrl.length();
                b--;
            } else {
                e++;
            }
            testUrl = testUrl.substring(0, b) + testUrl.substring(e, testUrl.length());
        }

        Properties props = new Properties();
        props.setProperty(PropertyKey.maxRows.getKeyName(), "123");

        // Default property value.
        JdbcConnection testConn = (JdbcConnection) DriverManager.getConnection(testUrl);
        assertEquals(-1, testConn.getPropertySet().getIntegerProperty(PropertyKey.maxRows).getValue().intValue());
        testConn = (JdbcConnection) DriverManager.getConnection(testUrl, new Properties());
        assertEquals(-1, testConn.getPropertySet().getIntegerProperty(PropertyKey.maxRows).getValue().intValue());

        // Property in properties only.
        testConn = (JdbcConnection) DriverManager.getConnection(testUrl, props);
        assertEquals(123, testConn.getPropertySet().getIntegerProperty(PropertyKey.maxRows).getValue().intValue());

        testUrl += (testUrl.indexOf('?') == -1 ? "?" : "&") + "maxRows=321";

        // Property in URL only.
        testConn = (JdbcConnection) DriverManager.getConnection(testUrl);
        assertEquals(321, testConn.getPropertySet().getIntegerProperty(PropertyKey.maxRows).getValue().intValue());
        testConn = (JdbcConnection) DriverManager.getConnection(testUrl, new Properties());
        assertEquals(321, testConn.getPropertySet().getIntegerProperty(PropertyKey.maxRows).getValue().intValue());

        // Property in both.
        testConn = (JdbcConnection) DriverManager.getConnection(testUrl, props);
        assertEquals(123, testConn.getPropertySet().getIntegerProperty(PropertyKey.maxRows).getValue().intValue());
    }

    /**
     * Test for REF_CURSOR support checking.
     * 
     * @throws Exception
     */
    @Test
    public void testSupportsRefCursors() throws Exception {
        assertFalse(this.conn.getMetaData().supportsRefCursors());
    }

    /**
     * Test the new connection property 'enableEscapeProcessing', as well as the old connection property 'processEscapeCodesForPrepStmts' and interrelation
     * between them.
     * 
     * This test uses a QueryInterceptor to capture the query sent to the server and assert whether escape processing has been done in the client side or if
     * the query is sent untouched and escape processing will be done at server side, according to provided connection properties and type of Statement objects
     * in use.
     * 
     * @throws Exception
     */
    @Test
    public void testEnableEscapeProcessing() throws Exception {
        // make sure the connection string doesn't contain 'enableEscapeProcessing'
        String testUrl = BaseTestCase.dbUrl;
        int b = testUrl.indexOf("enableEscapeProcessing");
        if (b != -1) {
            int e = testUrl.indexOf('&', b);
            if (e == -1) {
                e = testUrl.length();
                b--;
            } else {
                e++;
            }
            testUrl = testUrl.substring(0, b) + testUrl.substring(e, testUrl.length());
        }
        String query = "SELECT /* testEnableEscapeProcessing: (%d) */ {fn sin(pi()/2)}, {ts '2015-08-16 11:22:33'}, {fn ucase('this is mysql')}";
        Timestamp testTimestamp = new Timestamp(TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd HH:mm:ss", null).parse("2015-08-16 11:22:33").getTime());

        for (int tst = 0; tst < 8; tst++) {
            boolean enableEscapeProcessing = (tst & 0x1) != 0;
            boolean processEscapeCodesForPrepStmts = (tst & 0x2) != 0;
            boolean useServerPrepStmts = (tst & 0x4) != 0;

            Properties props = new Properties();
            props.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestEnableEscapeProcessingQueryInterceptor.class.getName());
            props.setProperty(PropertyKey.enableEscapeProcessing.getKeyName(), Boolean.toString(enableEscapeProcessing));
            props.setProperty(PropertyKey.processEscapeCodesForPrepStmts.getKeyName(), Boolean.toString(processEscapeCodesForPrepStmts));
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useServerPrepStmts));
            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "LOCAL");

            Connection testConn = getConnectionWithProps(testUrl, props);
            this.stmt = testConn.createStatement();
            this.rs = this.stmt.executeQuery(String.format(query, tst));

            String testCase = String.format("Case: %d [ %s | %s | %s ]/Statement", tst, enableEscapeProcessing ? "enEscProc" : "-",
                    processEscapeCodesForPrepStmts ? "procEscProcPS" : "-", useServerPrepStmts ? "useSSPS" : "-");
            assertTrue(this.rs.next(), testCase);
            assertEquals(1d, this.rs.getDouble(1), testCase);

            Timestamp ts = !enableEscapeProcessing && this.rs.getMetaData().getColumnType(2) == Types.VARCHAR ?
            // MySQL 5.5 returns {ts '2015-08-16 11:22:33'} as a VARCHAR column, while newer servers return it as a DATETIME
                    Timestamp.from(ZonedDateTime
                            .of(2015, 8, 16, 11, 22, 33, 0, ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone().toZoneId())
                            .withZoneSameInstant(ZoneId.systemDefault()).toInstant())
                    : testTimestamp;

            assertEquals(ts, this.rs.getTimestamp(2), testCase);
            assertEquals("THIS IS MYSQL", this.rs.getString(3), testCase);
            assertFalse(this.rs.next(), testCase);

            this.pstmt = testConn.prepareStatement(String.format(query, tst));
            this.rs = this.pstmt.executeQuery();

            ts = !processEscapeCodesForPrepStmts && this.rs.getMetaData().getColumnType(2) == Types.VARCHAR ?
            // MySQL 5.5 returns {ts '2015-08-16 11:22:33'} as a VARCHAR column, while newer servers return it as a DATETIME
                    Timestamp.from(ZonedDateTime
                            .of(2015, 8, 16, 11, 22, 33, 0, ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone().toZoneId())
                            .withZoneSameInstant(ZoneId.systemDefault()).toInstant())
                    : testTimestamp;

            testCase = String.format("Case: %d [ %s | %s | %s ]/PreparedStatement", tst, enableEscapeProcessing ? "enEscProc" : "-",
                    processEscapeCodesForPrepStmts ? "procEscProcPS" : "-", useServerPrepStmts ? "useSSPS" : "-");
            assertTrue(this.rs.next(), testCase);
            assertEquals(1d, this.rs.getDouble(1), testCase);
            assertEquals(ts, this.rs.getTimestamp(2), testCase);
            assertEquals("THIS IS MYSQL", this.rs.getString(3), testCase);
            assertFalse(this.rs.next(), testCase);

            testConn.close();
        }
    }

    public static class TestEnableEscapeProcessingQueryInterceptor extends BaseQueryInterceptor {
        @Override
        public <T extends Resultset> T preProcess(Supplier<String> str, Query interceptedQuery) {
            String sql = str == null ? null : str.get();
            if (sql == null) {
                try {
                    if (interceptedQuery instanceof ClientPreparedStatement) {
                        sql = ((ClientPreparedStatement) interceptedQuery).asSql();
                    } else if (interceptedQuery instanceof PreparedQuery<?>) {
                        sql = ((PreparedQuery<?>) interceptedQuery).asSql();
                    }
                } catch (SQLException ex) {
                    throw ExceptionFactory.createException(ex.getMessage(), ex);
                }
            }

            int p;
            if (sql != null && (p = sql.indexOf("testEnableEscapeProcessing:")) != -1) {
                int tst = Integer.parseInt(sql.substring(sql.indexOf('(', p) + 1, sql.indexOf(')', p)));
                boolean enableEscapeProcessing = (tst & 0x1) != 0;
                boolean processEscapeCodesForPrepStmts = (tst & 0x2) != 0;
                boolean useServerPrepStmts = (tst & 0x4) != 0;
                boolean isPreparedStatement = interceptedQuery instanceof PreparedStatement || interceptedQuery instanceof PreparedQuery<?>;

                String testCase = String.format("Case: %d [ %s | %s | %s ]/%s", tst, enableEscapeProcessing ? "enEscProc" : "-",
                        processEscapeCodesForPrepStmts ? "procEscProcPS" : "-", useServerPrepStmts ? "useSSPS" : "-",
                        isPreparedStatement ? "PreparedStatement" : "Statement");

                boolean escapeProcessingDone = sql.indexOf('{') == -1;
                assertTrue(isPreparedStatement && processEscapeCodesForPrepStmts == escapeProcessingDone
                        || !isPreparedStatement && enableEscapeProcessing == escapeProcessingDone, testCase);
            }
            final String fsql = sql;
            return super.preProcess(() -> {
                return fsql;
            }, interceptedQuery);
        }
    }

    @Test
    public void testDecoratorsChain() throws Exception {
        Connection c = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.useCompression.getKeyName(), "false");
            props.setProperty(PropertyKey.maintainTimeStats.getKeyName(), "true");
            props.setProperty(PropertyKey.traceProtocol.getKeyName(), "true");
            props.setProperty(PropertyKey.enablePacketDebug.getKeyName(), "true");
            c = getConnectionWithProps(props);

            NativeProtocol p = ((NativeSession) ((JdbcConnection) c).getSession()).getProtocol();
            MessageSender<NativePacketPayload> sender = p.getPacketSender();
            MessageReader<NativePacketHeader, NativePacketPayload> reader = p.getPacketReader();

            assertEquals(DebugBufferingPacketSender.class, sender.getClass());
            assertEquals(TracingPacketSender.class, sender.undecorate().getClass());
            assertEquals(TimeTrackingPacketSender.class, sender.undecorate().undecorate().getClass());
            assertEquals(SimplePacketSender.class, sender.undecorate().undecorate().undecorate().getClass());

            assertEquals(MultiPacketReader.class, reader.getClass());
            assertEquals(DebugBufferingPacketReader.class, reader.undecorate().getClass());
            assertEquals(TracingPacketReader.class, reader.undecorate().undecorate().getClass());
            assertEquals(TimeTrackingPacketReader.class, reader.undecorate().undecorate().undecorate().getClass());
            assertEquals(SimplePacketReader.class, reader.undecorate().undecorate().undecorate().undecorate().getClass());

            // remove traceProtocol
            p.getPropertySet().getProperty(PropertyKey.traceProtocol).setValue(false);
            sender = p.getPacketSender();
            reader = p.getPacketReader();

            assertEquals(DebugBufferingPacketSender.class, sender.getClass());
            assertEquals(TimeTrackingPacketSender.class, sender.undecorate().getClass());
            assertEquals(SimplePacketSender.class, sender.undecorate().undecorate().getClass());

            assertEquals(MultiPacketReader.class, reader.getClass());
            assertEquals(DebugBufferingPacketReader.class, reader.undecorate().getClass());
            assertEquals(TimeTrackingPacketReader.class, reader.undecorate().undecorate().getClass());
            assertEquals(SimplePacketReader.class, reader.undecorate().undecorate().undecorate().getClass());

            // remove maintainTimeStats
            p.getPropertySet().getProperty(PropertyKey.maintainTimeStats).setValue(false);
            sender = p.getPacketSender();
            reader = p.getPacketReader();

            assertEquals(DebugBufferingPacketSender.class, sender.getClass());
            assertEquals(SimplePacketSender.class, sender.undecorate().getClass());

            assertEquals(MultiPacketReader.class, reader.getClass());
            assertEquals(DebugBufferingPacketReader.class, reader.undecorate().getClass());
            assertEquals(SimplePacketReader.class, reader.undecorate().undecorate().getClass());

            assertNotEquals(TimeTrackingPacketSender.class, p.getPacketSentTimeHolder().getClass());
            assertNotEquals(TimeTrackingPacketReader.class, p.getPacketReceivedTimeHolder().getClass());

            // remove enablePacketDebug
            p.getPropertySet().getProperty(PropertyKey.enablePacketDebug).setValue(false);
            sender = p.getPacketSender();
            reader = p.getPacketReader();

            assertEquals(SimplePacketSender.class, sender.getClass());

            assertEquals(MultiPacketReader.class, reader.getClass());
            assertEquals(SimplePacketReader.class, reader.undecorate().getClass());

            // add maintainTimeStats
            p.getPropertySet().getProperty(PropertyKey.maintainTimeStats).setValue(true);
            sender = p.getPacketSender();
            reader = p.getPacketReader();

            assertEquals(TimeTrackingPacketSender.class, sender.getClass());
            assertEquals(SimplePacketSender.class, sender.undecorate().getClass());

            assertEquals(MultiPacketReader.class, reader.getClass());
            assertEquals(TimeTrackingPacketReader.class, reader.undecorate().getClass());
            assertEquals(SimplePacketReader.class, reader.undecorate().undecorate().getClass());

            assertEquals(TimeTrackingPacketSender.class, p.getPacketSentTimeHolder().getClass());
            assertEquals(TimeTrackingPacketReader.class, p.getPacketReceivedTimeHolder().getClass());

            // remove listener and try to enable traceProtocol, it should be missed in this case
            p.getPropertySet().getBooleanProperty(PropertyKey.traceProtocol).removeListener(p);
            p.getPropertySet().getProperty(PropertyKey.traceProtocol).setValue(true); // please note that the property is changed anyways, see the next step
            sender = p.getPacketSender();
            reader = p.getPacketReader();

            assertEquals(TimeTrackingPacketSender.class, sender.getClass());
            assertEquals(SimplePacketSender.class, sender.undecorate().getClass());

            assertEquals(MultiPacketReader.class, reader.getClass());
            assertEquals(TimeTrackingPacketReader.class, reader.undecorate().getClass());
            assertEquals(SimplePacketReader.class, reader.undecorate().undecorate().getClass());

            // ensure that other listeners are still working
            p.getPropertySet().getProperty(PropertyKey.enablePacketDebug).setValue(true);
            sender = p.getPacketSender();
            reader = p.getPacketReader();

            assertEquals(DebugBufferingPacketSender.class, sender.getClass());
            assertEquals(TracingPacketSender.class, sender.undecorate().getClass()); // it's here because we changed the traceProtocol previously
            assertEquals(TimeTrackingPacketSender.class, sender.undecorate().undecorate().getClass());
            assertEquals(SimplePacketSender.class, sender.undecorate().undecorate().undecorate().getClass());

            assertEquals(MultiPacketReader.class, reader.getClass());
            assertEquals(DebugBufferingPacketReader.class, reader.undecorate().getClass());
            assertEquals(TracingPacketReader.class, reader.undecorate().undecorate().getClass());
            assertEquals(TimeTrackingPacketReader.class, reader.undecorate().undecorate().undecorate().getClass());
            assertEquals(SimplePacketReader.class, reader.undecorate().undecorate().undecorate().undecorate().getClass());

        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    /**
     * Test authentication with a user that requires an SSL connection.
     * 
     * This test requires the CA truststore and the client keystore available in src/test/config/ssl-test-certs.
     * The server needs to be configured with the CA and server certificates from src/test/config/ssl-test-certs.
     * 
     * @throws Exception
     */
    @Test
    public void testUserRequireSSL() throws Exception {
        if (!versionMeetsMinimum(5, 7, 6)) {
            return;
        }

        Connection testConn;
        Statement testStmt;

        final String user = "testUserReqSSL";
        final String password = "testUserReqSSL";

        final Properties props = new Properties();
        props.setProperty(PropertyKey.USER.getKeyName(), user);
        props.setProperty(PropertyKey.PASSWORD.getKeyName(), password);

        createUser("'" + user + "'@'%'", "IDENTIFIED BY '" + password + "' REQUIRE SSL");
        this.stmt.execute("GRANT SELECT ON *.* TO '" + user + "'@'%'");

        /*
         * No SSL.
         */
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        /*
         * SSL: no server certificate validation & no client certificate.
         */
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();

        /*
         * SSL: server certificate validation & no client certificate.
         */
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "true");
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();

        /*
         * SSL: server certificate validation & client certificate.
         */
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/client-keystore");
        props.setProperty(PropertyKey.clientCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), "password");
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();

        /*
         * SSL: no server certificate validation & client certificate.
         */
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");
        props.remove(PropertyKey.trustCertificateKeyStoreUrl.getKeyName());
        props.remove(PropertyKey.trustCertificateKeyStoreType.getKeyName());
        props.remove(PropertyKey.trustCertificateKeyStorePassword.getKeyName());
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();
    }

    /**
     * Test authentication with a user that requires an SSL connection and an authorized client certificate.
     * 
     * This test requires the CA truststore and the client keystore available in src/test/config/ssl-test-certs.
     * The server needs to be configured with the CA and server certificates from src/test/config/ssl-test-certs.
     * 
     * @throws Exception
     */
    @Test
    public void testUserRequireX509() throws Exception {
        if (!versionMeetsMinimum(5, 7, 6)) {
            return;
        }

        Connection testConn;
        Statement testStmt;

        final String user = "testUserReqX509";
        final String password = "testUserReqX509";

        final Properties props = new Properties();
        props.setProperty(PropertyKey.USER.getKeyName(), user);
        props.setProperty(PropertyKey.PASSWORD.getKeyName(), password);

        createUser("'" + user + "'@'%'", "IDENTIFIED BY '" + password + "' REQUIRE X509");
        this.stmt.execute("GRANT SELECT ON *.* TO '" + user + "'@'%'");

        /*
         * No SSL.
         */
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        /*
         * SSL: no server certificate validation & no client certificate.
         */
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        /*
         * SSL: server certificate validation & no client certificate.
         */
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "true");
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        /*
         * SSL: server certificate validation & client certificate.
         */
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/client-keystore");
        props.setProperty(PropertyKey.clientCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), "password");
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();

        /*
         * SSL: no server certificate validation & client certificate.
         */
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");
        props.remove(PropertyKey.trustCertificateKeyStoreUrl.getKeyName());
        props.remove(PropertyKey.trustCertificateKeyStoreType.getKeyName());
        props.remove(PropertyKey.trustCertificateKeyStorePassword.getKeyName());
        testConn = getConnectionWithProps(props);
        testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT CURRENT_USER()");
        assertTrue(this.rs.next());
        assertEquals(user, this.rs.getString(1).split("@")[0]);
        testConn.close();
    }

    /**
     * Tests that given SSL/TLS related connection properties values are processed as expected.
     *
     * @throws Exception
     */
    @Test
    public void testSslConnectionOptions() throws Exception {
        Connection testConn;
        JdbcPropertySet propSet;

        testConn = getConnectionWithProps(this.sslFreeBaseUrl, "");
        propSet = ((JdbcConnection) testConn).getPropertySet();
        assertEquals(SslMode.PREFERRED, propSet.getProperty(PropertyKey.sslMode).getValue());
        assertNull(propSet.getProperty(PropertyKey.trustCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.trustCertificateKeyStoreType).getValue());
        assertNull(propSet.getProperty(PropertyKey.trustCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue());
        assertNull(propSet.getProperty(PropertyKey.clientCertificateKeyStoreUrl).getValue());
        assertEquals("JKS", propSet.getProperty(PropertyKey.clientCertificateKeyStoreType).getValue());
        assertNull(propSet.getProperty(PropertyKey.clientCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemKeyStore).getValue());
        testConn.close();

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.toString());
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "trust-cert-keystore-url");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "trust-cert-keystore-type");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "trust-cert-keystore-pwd");
        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "false");
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), "client-cert-keystore-url");
        props.setProperty(PropertyKey.clientCertificateKeyStoreType.getKeyName(), "client-cert-keystore-type");
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), "client-cert-keystore-pwd");
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "false");

        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        propSet = ((JdbcConnection) testConn).getPropertySet();
        assertEquals(SslMode.DISABLED, propSet.getProperty(PropertyKey.sslMode).getValue());
        assertEquals("trust-cert-keystore-url", propSet.getProperty(PropertyKey.trustCertificateKeyStoreUrl).getValue());
        assertEquals("trust-cert-keystore-type", propSet.getProperty(PropertyKey.trustCertificateKeyStoreType).getValue());
        assertEquals("trust-cert-keystore-pwd", propSet.getProperty(PropertyKey.trustCertificateKeyStorePassword).getValue());
        assertFalse(propSet.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue());
        assertEquals("client-cert-keystore-url", propSet.getProperty(PropertyKey.clientCertificateKeyStoreUrl).getValue());
        assertEquals("client-cert-keystore-type", propSet.getProperty(PropertyKey.clientCertificateKeyStoreType).getValue());
        assertEquals("client-cert-keystore-pwd", propSet.getProperty(PropertyKey.clientCertificateKeyStorePassword).getValue());
        assertFalse(propSet.getBooleanProperty(PropertyKey.fallbackToSystemKeyStore).getValue());
        testConn.close();

        props.setProperty(PropertyKey.fallbackToSystemTrustStore.getKeyName(), "true");
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "true");

        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        propSet = ((JdbcConnection) testConn).getPropertySet();
        assertEquals(SslMode.DISABLED, propSet.getProperty(PropertyKey.sslMode).getValue());
        assertEquals("trust-cert-keystore-url", propSet.getProperty(PropertyKey.trustCertificateKeyStoreUrl).getValue());
        assertEquals("trust-cert-keystore-type", propSet.getProperty(PropertyKey.trustCertificateKeyStoreType).getValue());
        assertEquals("trust-cert-keystore-pwd", propSet.getProperty(PropertyKey.trustCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue());
        assertEquals("client-cert-keystore-url", propSet.getProperty(PropertyKey.clientCertificateKeyStoreUrl).getValue());
        assertEquals("client-cert-keystore-type", propSet.getProperty(PropertyKey.clientCertificateKeyStoreType).getValue());
        assertEquals("client-cert-keystore-pwd", propSet.getProperty(PropertyKey.clientCertificateKeyStorePassword).getValue());
        assertTrue(propSet.getBooleanProperty(PropertyKey.fallbackToSystemKeyStore).getValue());
        testConn.close();
    }

    /**
     * Tests connection property 'testFallbackToSystemTrustStore' behavior.
     *
     * @throws Exception
     */
    @Test
    public void testFallbackToSystemTrustStore() throws Exception {
        Connection testConn;

        /*
         * Valid system-wide TrustStore.
         */
        System.setProperty("javax.net.ssl.trustStore", "file:src/test/config/ssl-test-certs/ca-truststore");
        System.setProperty("javax.net.ssl.trustStoreType", "JKS");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        // No connection-local TrustStore.
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=REQUIRED");
        assertSecureConnection(testConn);
        testConn.close();
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=VERIFY_CA");
        assertSecureConnection(testConn);
        testConn.close();
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=VERIFY_CA,fallbackToSystemTrustStore=true");
        assertSecureConnection(testConn);
        testConn.close();
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=VERIFY_CA,fallbackToSystemTrustStore=false"));

        // Invalid connection-local TrustStore:
        testConn = getConnectionWithProps(this.sslFreeBaseUrl,
                "sslMode=REQUIRED,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore-ext,"
                        + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password");
        assertSecureConnection(testConn);
        testConn.close();
        assertThrows(CommunicationsException.class,
                () -> getConnectionWithProps(this.sslFreeBaseUrl,
                        "sslMode=VERIFY_CA,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore-ext,"
                                + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password"));
        assertThrows(CommunicationsException.class,
                () -> getConnectionWithProps(this.sslFreeBaseUrl,
                        "sslMode=VERIFY_CA,fallbackToSystemTrustStore=true,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore-ext,"
                                + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password"));
        assertThrows(CommunicationsException.class,
                () -> getConnectionWithProps(this.sslFreeBaseUrl,
                        "sslMode=VERIFY_CA,fallbackToSystemTrustStore=false,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore-ext,"
                                + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password"));

        /*
         * Invalid system-wide TrustStore.
         */
        System.setProperty("javax.net.ssl.trustStore", "file:src/test/config/ssl-test-certs/ca-truststore-ext");
        System.setProperty("javax.net.ssl.trustStoreType", "JKS");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        // No connection-local TrustStore.
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=REQUIRED");
        assertSecureConnection(testConn);
        testConn.close();
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=VERIFY_CA"));
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=VERIFY_CA,fallbackToSystemTrustStore=true"));
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=VERIFY_CA,fallbackToSystemTrustStore=false"));

        // Valid connection-local TrustStore:
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, "sslMode=REQUIRED,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore,"
                + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password");
        assertSecureConnection(testConn);
        testConn.close();
        testConn = getConnectionWithProps(this.sslFreeBaseUrl,
                "sslMode=VERIFY_CA,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore,"
                        + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password");
        assertSecureConnection(testConn);
        testConn.close();
        testConn = getConnectionWithProps(this.sslFreeBaseUrl,
                "sslMode=VERIFY_CA,fallbackToSystemTrustStore=true,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore,"
                        + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password");
        assertSecureConnection(testConn);
        testConn.close();
        testConn = getConnectionWithProps(this.sslFreeBaseUrl,
                "sslMode=VERIFY_CA,fallbackToSystemTrustStore=false,trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore,"
                        + "trustCertificateKeyStoreType=JKS,trustCertificateKeyStorePassword=password");
        assertSecureConnection(testConn);
        testConn.close();
    }

    /**
     * Tests connection property 'testFallbackToSystemKeyStore' behavior.
     *
     * @throws Exception
     */
    @Test
    public void testFallbackToSystemKeyStore() throws Exception {
        if (!versionMeetsMinimum(5, 7, 6)) {
            return;
        }

        final String user = "testFbToSysKS";
        createUser(user, "IDENTIFIED BY 'password' REQUIRE X509");
        this.stmt.execute("GRANT ALL ON *.* TO '" + user + "'@'%'");

        final Properties props = new Properties();
        props.setProperty(PropertyKey.USER.getKeyName(), user);
        props.setProperty(PropertyKey.PASSWORD.getKeyName(), "password");
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.toString());

        Connection testConn;

        /*
         * Valid system-wide KeyStore.
         */
        System.setProperty("javax.net.ssl.keyStore", "file:src/test/config/ssl-test-certs/client-keystore");
        System.setProperty("javax.net.ssl.keyStoreType", "JKS");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");

        // No connection-local KeyStore.
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn, user);
        testConn.close();
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "true");
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn, user);
        testConn.close();
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "false");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", () -> getConnectionWithProps(this.sslFreeBaseUrl, props));

        props.remove(PropertyKey.fallbackToSystemKeyStore.getKeyName());

        // Invalid connection-local KeyStore:
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/client-keystore-ext");
        props.setProperty(PropertyKey.clientCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), "password");
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "true");
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "false");
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));

        props.remove(PropertyKey.clientCertificateKeyStoreUrl.getKeyName());
        props.remove(PropertyKey.clientCertificateKeyStoreType.getKeyName());
        props.remove(PropertyKey.clientCertificateKeyStorePassword.getKeyName());
        props.remove(PropertyKey.fallbackToSystemKeyStore.getKeyName());

        /*
         * Invalid system-wide KeyStore.
         */
        System.setProperty("javax.net.ssl.keyStore", "file:src/test/config/ssl-test-certs/client-keystore-ext");
        System.setProperty("javax.net.ssl.keyStoreType", "JKS");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");

        // No connection-local KeyStore.
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "true");
        assertThrows(CommunicationsException.class, () -> getConnectionWithProps(this.sslFreeBaseUrl, props));
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "false");
        assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", () -> getConnectionWithProps(this.sslFreeBaseUrl, props));

        props.remove(PropertyKey.fallbackToSystemKeyStore.getKeyName());

        // Valid connection-local KeyStore:
        props.setProperty(PropertyKey.clientCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/client-keystore");
        props.setProperty(PropertyKey.clientCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.clientCertificateKeyStorePassword.getKeyName(), "password");
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn, user);
        testConn.close();
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "true");
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn, user);
        testConn.close();
        props.setProperty(PropertyKey.fallbackToSystemKeyStore.getKeyName(), "false");
        testConn = getConnectionWithProps(this.sslFreeBaseUrl, props);
        assertSecureConnection(testConn, user);
        testConn.close();
    }

    /**
     * Tests "LOAD DATA LOCAL INFILE" statements when enabled but restricted to a specific path, by specifying a path in the connection property
     * 'allowLoadLocalInfileInPath'.
     *
     * @throws Exception
     */
    @Test
    public void testAllowLoadLocalInfileInPath() throws Exception {
        Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"));

        /*
         * Create the following directories structure:
         * /tmp/
         * ...ldli_1_[random]/
         * ......sub_11/
         * .........sub_111/
         * ............testAllowLoadLocalInfileInPath_[random].txt
         * ...ldli_2_[random]/
         * ......lnk_21 --> /tmp/ldli_1_[random]/sub_1/
         * ......testAllowLoadLocalInfileInPath_[random].txt
         */
        Path tmpDir1 = Files.createTempDirectory("ldli_1_");
        tmpDir1.toFile().deleteOnExit();
        Path tmpSDir1 = tmpDir1.resolve("sub_11");
        tmpSDir1.toFile().deleteOnExit();
        Path tmpSSDir1 = tmpSDir1.resolve("sub_111");
        tmpSSDir1.toFile().deleteOnExit();
        Files.createDirectories(tmpSSDir1);
        Path tmpFile1 = Files.createTempFile(tmpSSDir1, "testAllowLoadLocalInfileInPath_", ".txt");
        tmpFile1.toFile().deleteOnExit();
        try (FileWriter output = new FileWriter(tmpFile1.toFile())) {
            output.write("TEST DATA");
            output.flush();
        }
        Path tmpDir2 = Files.createTempDirectory("ldli_2_");
        tmpDir2.toFile().deleteOnExit();
        Path tmpLink2 = tmpDir2.resolve("lnk_11");
        tmpLink2.toFile().deleteOnExit();
        boolean skipLinkCheck = false;
        try {
            Files.createSymbolicLink(tmpLink2, tmpSDir1);
        } catch (IOException e) {
            // Symbolic links fail to create if not using elevated user rights on Windows.
            skipLinkCheck = true;
        }
        Path tmpFile2 = Files.createTempFile(tmpDir2, "testAllowLoadLocalInfileInPath_", ".txt");
        tmpFile2.toFile().deleteOnExit();
        try (FileWriter output = new FileWriter(tmpFile2.toFile())) {
            output.write("TEST DATA");
            output.flush();
        }

        String dataPath1 = tmpFile1.toString().replace("\\", "\\\\");
        String dataUrl1 = tmpFile1.toUri().toURL().toExternalForm();
        String dataPath2 = tmpFile2.toString().replace("\\", "\\\\");
        String dataUrl2 = tmpFile2.toUri().toURL().toExternalForm();

        createTable("testAllowLoadLocalInfileInPath", "(data VARCHAR(100))");
        Properties props = new Properties();

        // Default behavior: 'allowLoadLocalInfile' not set (false) & 'allowLoadLocalInfile' not set (NULL) & 'allowUrlInLocalInfile' not set (false).
        try (Connection testConn = getConnectionWithProps(props)) {
            Statement testStmt = testConn.createStatement();
            assertThrows(SQLSyntaxErrorException.class,
                    versionMeetsMinimum(8, 0, 19) ? "Loading local data is disabled; this must be enabled on both the client and server sides"
                            : "The used command is not allowed with this MySQL version",
                    () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + dataPath1 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
        }

        // 'allowLoadLocalInfile=false' & 'allowLoadLocalInfile' not set (NULL) & 'allowUrlInLocalInfile' not set (false).
        props.setProperty(PropertyKey.allowLoadLocalInfile.getKeyName(), "false");
        try (Connection testConn = getConnectionWithProps(props)) {
            Statement testStmt = testConn.createStatement();
            assertThrows(SQLSyntaxErrorException.class,
                    versionMeetsMinimum(8, 0, 19) ? "Loading local data is disabled; this must be enabled on both the client and server sides"
                            : "The used command is not allowed with this MySQL version",
                    () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + dataPath1 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
        }

        // 'allowLoadLocalInfile=true' & 'allowLoadLocalInfile' not set or set with any value & 'allowUrlInLocalInfile' not set (false).
        // Load file from any path.
        props.setProperty(PropertyKey.allowLoadLocalInfile.getKeyName(), "true");
        try (Connection testConn = getConnectionWithProps(props)) {
            Statement testStmt = testConn.createStatement();
            testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + dataPath1 + "' INTO TABLE testAllowLoadLocalInfileInPath");
            testAllowLoadLocalInfileInPathCheckAndDelete();
        }
        props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), ""); // Empty dir name.
        try (Connection testConn = getConnectionWithProps(props)) {
            Statement testStmt = testConn.createStatement();
            testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + dataPath1 + "' INTO TABLE testAllowLoadLocalInfileInPath");
            testAllowLoadLocalInfileInPathCheckAndDelete();
        }
        props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), "   "); // Dir name with spaces.
        try (Connection testConn = getConnectionWithProps(props)) {
            Statement testStmt = testConn.createStatement();
            testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + dataPath1 + "' INTO TABLE testAllowLoadLocalInfileInPath");
            testAllowLoadLocalInfileInPathCheckAndDelete();
        }
        props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), tmpDir1.toString() + File.separator + "sub_12"); // Non-existing dir.
        try (Connection testConn = getConnectionWithProps(props)) {
            Statement testStmt = testConn.createStatement();
            testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + dataPath1 + "' INTO TABLE testAllowLoadLocalInfileInPath");
            testAllowLoadLocalInfileInPathCheckAndDelete();
        }
        props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), tmpDir2.toString()); // File not in the dir.
        try (Connection testConn = getConnectionWithProps(props)) {
            Statement testStmt = testConn.createStatement();
            testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + dataPath1 + "' INTO TABLE testAllowLoadLocalInfileInPath");
            testAllowLoadLocalInfileInPathCheckAndDelete();
        }

        boolean inclALLI = false;
        boolean inclAUILI = false;
        do {
            if (inclALLI) {
                props.setProperty(PropertyKey.allowLoadLocalInfile.getKeyName(), "false");
            } else {
                props.remove(PropertyKey.allowLoadLocalInfile.getKeyName());
            }

            String fileRef1;
            String fileRef2;
            if (inclAUILI) {
                props.setProperty(PropertyKey.allowUrlInLocalInfile.getKeyName(), "true");
                fileRef1 = dataUrl1;
                fileRef2 = dataUrl2;
            } else {
                props.remove(PropertyKey.allowUrlInLocalInfile.getKeyName());
                fileRef1 = dataPath1;
                fileRef2 = dataPath2;
            }

            // 'allowLoadLocalInfile' not set (rep w/ false) & 'allowLoadLocalInfile' set with matching paths & 'allowUrlInLocalInfile' not set (rep w/ true).
            // Loading files from valid paths works as expected.
            props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), tmpDir.toString());
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef1 + "' INTO TABLE testAllowLoadLocalInfileInPath");
                testAllowLoadLocalInfileInPathCheckAndDelete();
            }
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef2 + "' INTO TABLE testAllowLoadLocalInfileInPath");
                testAllowLoadLocalInfileInPathCheckAndDelete();
            }
            props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), tmpDir1.toString());
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef1 + "' INTO TABLE testAllowLoadLocalInfileInPath");
                testAllowLoadLocalInfileInPathCheckAndDelete();
            }
            props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), tmpSDir1.toString());
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef1 + "' INTO TABLE testAllowLoadLocalInfileInPath");
                testAllowLoadLocalInfileInPathCheckAndDelete();
            }
            props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), tmpSSDir1.toString());
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef1 + "' INTO TABLE testAllowLoadLocalInfileInPath");
                testAllowLoadLocalInfileInPathCheckAndDelete();
            }
            props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(),
                    tmpDir2.toString() + File.separator + ".." + File.separator + tmpDir1.getFileName());
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef1 + "' INTO TABLE testAllowLoadLocalInfileInPath");
                testAllowLoadLocalInfileInPathCheckAndDelete();
            }
            if (!skipLinkCheck) {
                props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), tmpLink2.toString());
                try (Connection testConn = getConnectionWithProps(props)) {
                    Statement testStmt = testConn.createStatement();
                    testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef1 + "' INTO TABLE testAllowLoadLocalInfileInPath");
                    testAllowLoadLocalInfileInPathCheckAndDelete();
                }
            }

            // 'allowLoadLocalInfile' not set (rep w/ false) & 'allowLoadLocalInfile' set with unmatching paths & 'allowUrlInLocalInfile' not set (rep w/ true).
            // Loading files from invalid paths fails with expected exception..
            props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), tmpDir1.toString());
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                assertThrows(SQLException.class, "(?i)The file '" + dataPath2 + "' is not under the safe path '.*'\\.",
                        () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef2 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
            }
            props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), tmpSDir1.toString());
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                assertThrows(SQLException.class, "(?i)The file '" + dataPath2 + "' is not under the safe path '.*'\\.",
                        () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef2 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
            }
            props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), tmpSSDir1.toString());
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                assertThrows(SQLException.class, "(?i)The file '" + dataPath2 + "' is not under the safe path '.*'\\.",
                        () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef2 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
            }
            props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(),
                    tmpDir2.toString() + File.separator + ".." + File.separator + tmpDir1.getFileName());
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                assertThrows(SQLException.class, "(?i)The file '" + dataPath2 + "' is not under the safe path '.*'\\.",
                        () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef2 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
            }
            if (!skipLinkCheck) {
                props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), tmpLink2.toString());
                try (Connection testConn = getConnectionWithProps(props)) {
                    Statement testStmt = testConn.createStatement();
                    assertThrows(SQLException.class, "(?i)The file '" + dataPath2 + "' is not under the safe path '.*'\\.",
                            () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef2 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
                }
            }

            // 'allowLoadLocalInfile' not set (rep w/ false) & 'allowLoadLocalInfile' set with bad paths & 'allowUrlInLocalInfile' not set (rep w/ true).
            // Loading files from any path fails with expected exception.
            props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), ""); // Empty dir name.
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                assertThrows(SQLException.class, "The path '' specified in 'allowLoadLocalInfileInPath' does not exist\\.",
                        () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef1 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
            }
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                assertThrows(SQLException.class, "The path '' specified in 'allowLoadLocalInfileInPath' does not exist\\.",
                        () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef2 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
            }
            props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), "   "); // Dir name with spaces.
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                assertThrows(SQLException.class, "The path '   ' specified in 'allowLoadLocalInfileInPath' does not exist\\.",
                        () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef1 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
            }
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                assertThrows(SQLException.class, "The path '   ' specified in 'allowLoadLocalInfileInPath' does not exist\\.",
                        () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef2 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
            }
            props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), tmpDir1.toString() + File.separator + "sub_12"); // Non-existing dir.
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                assertThrows(SQLException.class,
                        "(?i)The path '" + (tmpDir1.toString() + File.separator + "sub_12").replace("\\", "\\\\")
                                + "' specified in 'allowLoadLocalInfileInPath' does not exist\\.",
                        () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef1 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
            }
            try (Connection testConn = getConnectionWithProps(props)) {
                Statement testStmt = testConn.createStatement();
                assertThrows(SQLException.class,
                        "(?i)The path '" + (tmpDir1.toString() + File.separator + "sub_12").replace("\\", "\\\\")
                                + "' specified in 'allowLoadLocalInfileInPath' does not exist\\.",
                        () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + fileRef2 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
            }
        } while ((inclALLI = !inclALLI) || (inclAUILI = !inclAUILI));

        // 'allowLoadLocalInfile' not set (false) & 'allowLoadLocalInfile' set with valid path & 'allowUrlInLocalInfile=true'.
        // Loading files using different URL formats (2 valid + 2 invalid).
        props.remove(PropertyKey.allowLoadLocalInfile.getKeyName());
        props.setProperty(PropertyKey.allowLoadLocalInfileInPath.getKeyName(), tmpDir1.toString());
        props.setProperty(PropertyKey.allowUrlInLocalInfile.getKeyName(), "true");
        try (Connection testConn = getConnectionWithProps(props)) {
            Statement testStmt = testConn.createStatement();
            testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + dataPath1 + "' INTO TABLE testAllowLoadLocalInfileInPath");
            testAllowLoadLocalInfileInPathCheckAndDelete();
        }
        try (Connection testConn = getConnectionWithProps(props)) {
            String filePrefix = Util.isRunningOnWindows() ? "file://localhost/" : "file://localhost";
            Statement testStmt = testConn.createStatement();
            testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + filePrefix + dataPath1 + "' INTO TABLE testAllowLoadLocalInfileInPath");
            testAllowLoadLocalInfileInPathCheckAndDelete();
        }
        try (Connection testConn = getConnectionWithProps(props)) {
            String filePrefix = Util.isRunningOnWindows() ? "file://somehost/" : "file://somehost";
            Statement testStmt = testConn.createStatement();
            assertThrows(SQLException.class,
                    "Cannot read from '.*'\\. Only local host names are supported when 'allowLoadLocalInfileInPath' is set\\. "
                            + "Consider using the loopback network interface \\('localhost'\\)\\.",
                    () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + filePrefix + dataPath1 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
        }
        try (Connection testConn = getConnectionWithProps(props)) {
            String ftpPrefix = Util.isRunningOnWindows() ? "ftp://localhost/" : "ftp://localhost";
            Statement testStmt = testConn.createStatement();
            assertThrows(SQLException.class, "Unsupported protocol 'ftp'\\. Only protocol 'file' is supported when 'allowLoadLocalInfileInPath' is set\\.",
                    () -> testStmt.executeQuery("LOAD DATA LOCAL INFILE '" + ftpPrefix + dataPath1 + "' INTO TABLE testAllowLoadLocalInfileInPath"));
        }
    }

    private void testAllowLoadLocalInfileInPathCheckAndDelete() throws Exception {
        this.rs = this.stmt.executeQuery("SELECT * FROM testAllowLoadLocalInfileInPath");
        assertTrue(this.rs.next());
        assertEquals("TEST DATA", this.rs.getString(1));
        assertEquals(1, this.stmt.executeUpdate("DELETE FROM testAllowLoadLocalInfileInPath"));
    }
}
