package com.mysql.cj.jdbc;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.PropertyKey;
import org.junit.jupiter.api.Test;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SQLXMLTests {

    @Test
    public void testAllowXmlUnsafeExternalEntityPropertyFalse() {

        final String url =
                "jdbc:mysql:aws://somehost:1234/test";
        final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        JdbcPropertySetImpl connProps1 = new JdbcPropertySetImpl();
        connProps1.initializeProperties(conStr.getMainHost().exposeAsProperties());
        assertFalse(connProps1.getBooleanProperty("allowXmlUnsafeExternalEntity").getValue());
    }

    @Test
    public void testAllowUnsafeExternalEntityPropertyTrue() {
        final String url2 =
                "jdbc:mysql:aws://somehost:1234/test?"
                        + PropertyKey.allowXmlUnsafeExternalEntity.getKeyName()
                        + "=true";
        final ConnectionUrl conStr2 = ConnectionUrl.getConnectionUrlInstance(url2, new Properties());
        JdbcPropertySetImpl connProps2 = new JdbcPropertySetImpl();
        connProps2.initializeProperties(conStr2.getMainHost().exposeAsProperties());
        assertTrue(connProps2.getBooleanProperty("allowXmlUnsafeExternalEntity").getValue());
    }

    @Test
    public void testAllowXmlUnsafeExternalEntityFlagFalse(){

        final String url =
                "jdbc:mysql:aws://somehost:1234/test";
        final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        JdbcPropertySetImpl connProps1 = new JdbcPropertySetImpl();
        connProps1.initializeProperties(conStr.getMainHost().exposeAsProperties());

        MysqlSQLXML xmlTest = new MysqlSQLXML(null, connProps1);

        assertFalse(xmlTest.getAllowXmlUnsafeExternalEntity());

    }

    @Test
    public void testAllowXmlUnsafeExternalEntityFlagTrue(){

        final String url =
                "jdbc:mysql:aws://somehost:1234/test?"
                        + PropertyKey.allowXmlUnsafeExternalEntity.getKeyName()
                        + "=true";
        final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        JdbcPropertySetImpl connProps1 = new JdbcPropertySetImpl();
        connProps1.initializeProperties(conStr.getMainHost().exposeAsProperties());

        MysqlSQLXML xmlTest = new MysqlSQLXML(null, connProps1);

        assertTrue(xmlTest.getAllowXmlUnsafeExternalEntity());

    }


}
