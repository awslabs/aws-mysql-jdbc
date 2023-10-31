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

package com.mysql.cj.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ha.util.ConnectionUtils;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ConnectionUtilsTest {

  @ParameterizedTest
  @MethodSource("urlArguments")
  void test_buildUrl(
      final String endpoint,
      final String server,
      final String port,
      final String db,
      final String expected,
      final Properties props) throws SQLException {
    final Properties properties = new Properties();
    properties.putAll(props);
    properties.setProperty(server, server);
    properties.setProperty(PropertyKey.DBNAME.getKeyName(), db);

    final String actual = ConnectionUtils.getUrlFromEndpoint(
        endpoint,
        Integer.parseInt(port),
        properties);
    assertEquals(expected, actual);
  }

  static Stream<Arguments> urlArguments() {
    final Properties properties = new Properties();
    properties.setProperty("bar", "baz");
    return Stream.of(
        Arguments.of(
            "fooUrl",
            "",
            "3306",
            "database",
            "jdbc:mysql:aws://fooUrl:3306/database?bar=baz",
            properties),
        Arguments.of(
            "fooUrl",
            " ",
            "-1",
            "database",
            "jdbc:mysql:aws://fooUrl/database?bar=baz",
            properties),
        Arguments.of(
            "fooUrl",
            "server",
            "3306",
            "database",
            "jdbc:mysql:aws://fooUrl:3306/database?bar=baz&server=server",
            properties),
        Arguments.of(
            "fooUrl",
            "server",
            "3306",
            "",
            "jdbc:mysql:aws://fooUrl:3306?bar=baz&server=server",
            properties)
    );
  }
}
