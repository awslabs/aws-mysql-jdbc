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
  public void testHostInfoStored() throws SQLException {
    String url = "jdbc:mysql:aws://host1,host2,host3/test?" +
        "connectionPluginFactories=com.mysql.cj.jdbc.ha.plugins.ReadWriteSplittingPluginFactory";
    ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, new Properties());

    ICurrentConnectionProvider mockCurrentConnectionProvider = Mockito.mock(ICurrentConnectionProvider.class);
    PropertySet mockPropertySet = Mockito.mock(PropertySet.class);
    IConnectionPlugin mockNextPlugin = Mockito.mock(IConnectionPlugin.class);
    Log mockLog = Mockito.mock(Log.class);

    when(mockCurrentConnectionProvider.getOriginalUrl()).thenReturn(connUrl);

    ReadWriteSplittingPlugin plugin = new ReadWriteSplittingPlugin(mockCurrentConnectionProvider,
        mockPropertySet,
        mockNextPlugin,
        mockLog);

    assertEquals(3, plugin.getHosts().size());
  }
}
