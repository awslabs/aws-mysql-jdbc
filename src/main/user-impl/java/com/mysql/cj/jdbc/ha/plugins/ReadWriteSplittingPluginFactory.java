package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;

public class ReadWriteSplittingPluginFactory implements IConnectionPluginFactory {
  @Override
  public IConnectionPlugin getInstance(
      ICurrentConnectionProvider currentConnectionProvider,
      PropertySet propertySet,
      IConnectionPlugin nextPlugin,
      Log logger) {
    return new ReadWriteSplittingPlugin(currentConnectionProvider, propertySet, nextPlugin, logger);
  }
}
