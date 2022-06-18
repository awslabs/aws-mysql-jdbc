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

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MysqlReadWriteSplittingIntegrationTest extends MysqlIntegrationBaseTest {

  @Test
  public void test_connection() throws SQLException {
    Connection writerConn = connectToHost(TEST_WRITER_HOST);
    Statement stmt = writerConn.createStatement();
    stmt.executeUpdate("CREATE TABLE writer_table(ID INT NOT NULL PRIMARY KEY)");
    stmt.executeUpdate("INSERT INTO writer_table VALUES (1)");
    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM writer_table");
    rs.next();
    assertEquals(1, rs.getInt(1));

    Connection readerConn = connectToHost(TEST_READER_HOST);
    Statement stmt2 = readerConn.createStatement();
    assertThrows(SQLException.class, () -> stmt2.executeQuery("SELECT COUNT(*) FROM writer_table"));
  }
}
