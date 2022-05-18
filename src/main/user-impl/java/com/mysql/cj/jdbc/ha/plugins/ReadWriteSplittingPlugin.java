package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class ReadWriteSplittingPlugin implements IConnectionPlugin {
  private ICurrentConnectionProvider currentConnectionProvider;
  private PropertySet propertySet;
  final IConnectionPlugin nextPlugin;
  private final Log logger;

  protected boolean inTransaction = false;
  private List<HostInfo> hosts;

  public ReadWriteSplittingPlugin(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger) {
    this.currentConnectionProvider = currentConnectionProvider;
    this.propertySet = propertySet;
    this.nextPlugin = nextPlugin;
    this.logger = logger;
    this.hosts = new ArrayList<>(currentConnectionProvider.getOriginalUrl().getHostsList());
  }

  @Override
  public Object execute(Class<?> methodInvokeOn, String methodName, Callable<?> executeSqlFunc, Object[] args) throws Exception {
    return this.nextPlugin.execute(methodInvokeOn, methodName, executeSqlFunc, args);
  }

  @Override
  public void openInitialConnection(ConnectionUrl connectionUrl) throws SQLException {
    this.nextPlugin.openInitialConnection(connectionUrl);
  }

  @Override
  public void releaseResources() {
    this.nextPlugin.releaseResources();
  }

  @Override
  public void transactionBegun() {
    this.inTransaction = true;
    this.nextPlugin.transactionBegun();
  }

  @Override
  public void transactionCompleted() {
    this.inTransaction = false;
    this.nextPlugin.transactionCompleted();
  }

  protected List<HostInfo> getHosts() {
    return this.hosts;
  }
}
