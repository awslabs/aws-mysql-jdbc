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

package testsuite;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Date;

/* TEMPORARILY UNIT TEST TO SUPPORT PLUGIN FEATURE DEVELOPMENT. IT SHOULD BE REVIEWED AND REMOVED LATER. */
public class SmokeTests {

  private static String DB_CLUSTER = "xxx.cluster-yyy.zzz.rds.amazonaws.com";
  private static String DB_USER = "user";
  private static String DB_PASS = "password";

  private static software.aws.rds.jdbc.mysql.Driver registeredDriver;

  public SmokeTests() {
    try {
      if(registeredDriver == null) {
        registeredDriver = new software.aws.rds.jdbc.mysql.Driver();
      }
      DriverManager.registerDriver(registeredDriver);
    } catch (SQLException E) {
      throw new RuntimeException("Can't register driver!");
    }
  }

  @Test
  public void testSimpleConnect() throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:mysql:aws://" + DB_CLUSTER +
                    //"?logger=com.mysql.cj.log.StandardLogger&socketTimeout=30000&tcpKeepAlive=true&nodeCheckTimeMillis=5000",
                    "?logger=com.mysql.cj.log.StandardLogger&tcpKeepAlive=false",
            //"?logger=com.mysql.cj.log.StandardLogger&tcpKeepAlive=false&socketTimeout=30000",
            //"?failoverPluginFactories=com.mysql.cj.jdbc.ha.ca.plugins.CustomProtocolPluginFactory",
            DB_USER, DB_PASS);

    System.out.println(new Date().toString() + ": Connected ============================");
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("select sleep(100)");
    rs.next();
    System.out.println(String.format("%s: Result: %d", new Date().toString(), rs.getInt(1)));
    //System.out.println(String.format("%s: Result: isValid=%s", new Date().toString(), conn.isValid(30)));
    conn.close();
  }
}
