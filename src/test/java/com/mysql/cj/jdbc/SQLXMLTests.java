package com.mysql.cj.jdbc;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.PropertyKey;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertSame;
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
        final String expectedErrorMsg = "/dev/nonExistantFile (No such file or directory)";
        final ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        JdbcPropertySetImpl connProperties = new JdbcPropertySetImpl();
        connProperties.initializeProperties(connUrl.getMainHost().exposeAsProperties());

        MysqlSQLXML xmlTest = new MysqlSQLXML(null, connProperties);

        xmlTest.setString(xmlString);
        SQLException exception = assertThrows( SQLException.class, () -> xmlTest.getSource(DOMSource.class));

        // Assert correct error code was produced
        assertSame("S1009", exception.getSQLState());
        // Assert that the reason for this exception is because the file does not exist (and not because DTDs is disabled)
        assertTrue(expectedErrorMsg.equals(exception.getMessage()));
    }

}
