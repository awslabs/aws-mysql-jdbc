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

package testsuite.integration.container.mysql;

import org.junit.jupiter.api.BeforeAll;
import software.aws.rds.jdbc.mysql.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MysqlIntegrationBaseTest {

  protected static final String DB_CONN_STR_PREFIX = "jdbc:mysql:aws://";
  protected static final String TEST_WRITER_HOST = System.getenv("TEST_WRITER_HOST");
  protected static final String TEST_READER_HOST = System.getenv("TEST_READER_HOST");
  protected static final String TEST_PORT = System.getenv("TEST_PORT");
  protected static final String TEST_DB = System.getenv("TEST_DB");
  protected static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  protected static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");

  @BeforeAll
  public static void setUp() throws SQLException {
    DriverManager.registerDriver(new Driver());
  }

  protected Connection connectToHost(String host) throws SQLException {
    return DriverManager.getConnection(
        DB_CONN_STR_PREFIX + host + ":" + TEST_PORT + "/" + TEST_DB,
        TEST_USERNAME,
        TEST_PASSWORD
    );
  }
}
