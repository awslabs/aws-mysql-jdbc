package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;

public class DefaultFailoverPluginFactory implements IFailoverPluginFactory{
  @Override
  public IFailoverPlugin getInstance(PropertySet propertySet, HostInfo hostInfo, IFailoverPlugin next, Log log) {
    IFailoverPlugin plugin = new DefaultFailoverPlugin();
    plugin.init(propertySet, hostInfo, next, log);
    return plugin;
  }
}
