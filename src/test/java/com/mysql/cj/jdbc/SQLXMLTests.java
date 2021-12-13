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

package com.mysql.cj.jdbc;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.PropertyKey;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.xml.transform.dom.DOMSource;

public class SQLXMLTests {

    @Test
    public void testAllowXmlUnsafeExternalEntityPropertyFalse() {

        final String url =
          "jdbc:mysql://somehost:1234/test";
        final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        JdbcPropertySetImpl connProperties = new JdbcPropertySetImpl();
        connProperties.initializeProperties(connUrl.getMainHost().exposeAsProperties());
        assertFalse(connProperties.getBooleanProperty("allowXmlUnsafeExternalEntity").getValue());
    }

    @Test
    public void testAllowUnsafeExternalEntityPropertyTrue() {
        final String url =
          "jdbc:mysql://somehost:1234/test?"
            + PropertyKey.allowXmlUnsafeExternalEntity.getKeyName()
            + "=true";
        final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        JdbcPropertySetImpl connProperties = new JdbcPropertySetImpl();
        connProperties.initializeProperties(connUrl.getMainHost().exposeAsProperties());
        assertTrue(connProperties.getBooleanProperty("allowXmlUnsafeExternalEntity").getValue());
    }

    @Test
    public void testAllowXmlUnsafeExternalEntityFlagFalse(){

        final String url =
          "jdbc:mysql://somehost:1234/test";
        final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        JdbcPropertySetImpl connProperties = new JdbcPropertySetImpl();
        connProperties.initializeProperties(connUrl.getMainHost().exposeAsProperties());

        MysqlSQLXML xmlTest = new MysqlSQLXML(null, connProperties);

        assertFalse(xmlTest.getAllowXmlUnsafeExternalEntity());
    }

    @Test
    public void testAllowXmlUnsafeExternalEntityFlagTrue(){

        final String url =
            "jdbc:mysql://somehost:1234/test?"
                + PropertyKey.allowXmlUnsafeExternalEntity.getKeyName()
                + "=true";
        final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        JdbcPropertySetImpl connProperties = new JdbcPropertySetImpl();
        connProperties.initializeProperties(connUrl.getMainHost().exposeAsProperties());

        MysqlSQLXML xmlTest = new MysqlSQLXML(null, connProperties);

        assertTrue(xmlTest.getAllowXmlUnsafeExternalEntity());

    }

    @Test
    public void testXXEInjectionPrevention() throws SQLException {

        final String xmlString = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n" +
            "<!DOCTYPE foo [\n" +
            "<!ELEMENT foo ANY >\n" +
            "<!ENTITY xxe SYSTEM \"file:///dev/nonExistantFile\" >]><foo>&xxe;</foo>";
        final String url =
            "jdbc:mysql://somehost:1234/test";
        final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        final String expectedErrorMsg = "DOCTYPE is disallowed when the feature \"http://apache.org/xml/features/disallow-doctype-decl\" set to true.";

        JdbcPropertySetImpl connProperties = new JdbcPropertySetImpl();
        connProperties.initializeProperties(connUrl.getMainHost().exposeAsProperties());

        MysqlSQLXML xmlTest = new MysqlSQLXML(null, connProperties);

        xmlTest.setString(xmlString);
        SQLException exception = assertThrows( SQLException.class, () -> xmlTest.getSource(DOMSource.class));

        // Assert correct error code was produced
        assertSame("S1009", exception.getSQLState());
        // Assert that the reason for this exception was because of the security measurements put in place (DTDs disabled)
        assertTrue(expectedErrorMsg.equals(exception.getMessage()));
    }

    @Test
    public void testXXEInjectionAllowance() throws SQLException {

        final String xmlString = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n" +
                "<!DOCTYPE foo [\n" +
                "<!ELEMENT foo ANY >\n" +
                "<!ENTITY xxe SYSTEM \"file:///dev/nonExistantFile\" >]><foo>&xxe;</foo>";
        final String url =
                "jdbc:mysql://somehost:1234/test?"
                        + PropertyKey.allowXmlUnsafeExternalEntity.getKeyName()
                        + "=true";
        final String possibleErrorMsg1 = "(No such file or directory)";
        final String possibleErrorMsg2 = "(The system cannot find the path specified)";
        final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        JdbcPropertySetImpl connProperties = new JdbcPropertySetImpl();
        connProperties.initializeProperties(connUrl.getMainHost().exposeAsProperties());

        MysqlSQLXML xmlTest = new MysqlSQLXML(null, connProperties);

        xmlTest.setString(xmlString);
        SQLException exception = assertThrows( SQLException.class, () -> xmlTest.getSource(DOMSource.class));

        // Assert correct error code was produced
        assertSame("S1009", exception.getSQLState());
        // Assert that the reason for this exception is because the file does not exist (and not because DTDs is disabled)
        assertTrue(exception.getMessage().contains(possibleErrorMsg1) || exception.getMessage().contains(possibleErrorMsg2));
    }

}
