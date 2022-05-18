package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.jdbc.ha.ConnectionProxy;
import com.mysql.cj.log.Log;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.*;

public class ReadWriteSplittingPluginTest {

  @Test
  public void testPluginInstantiation() throws SQLException {
    String url = "jdbc:mysql:aws://localhost:5432/test?connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
    JdbcPropertySetImpl connProps = new JdbcPropertySetImpl();
    connProps.initializeProperties(conStr.getConnectionArgumentsAsProperties());

    ConnectionProxy mockProxy = mock(ConnectionProxy.class);
    Log mockLog = mock(Log.class);

    try(MockedConstruction<ReadWriteSplittingPluginFactory> mockReadWriteSplitterConstruction = mockConstruction(ReadWriteSplittingPluginFactory.class)) {
        ConnectionPluginManager manager = new ConnectionPluginManager(mockLog);
        manager.init(mockProxy, connProps);
        assertEquals(1, mockReadWriteSplitterConstruction.constructed().size());
        ReadWriteSplittingPluginFactory mockSplitterFactory = mockReadWriteSplitterConstruction.constructed().get(0);

        verify(mockSplitterFactory, times(1)).getInstance(
            any(ICurrentConnectionProvider.class),
            any(PropertySet.class),
            any(IConnectionPlugin.class),
            any(Log.class)
        );
    }
  }
}
