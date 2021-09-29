package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;

public class NodeMonitoringFailoverPluginFactory implements IFailoverPluginFactory {
  @Override
  public IFailoverPlugin getInstance(PropertySet propertySet, HostInfo hostInfo, IFailoverPlugin next, Log log) {
    IFailoverPlugin plugin = new NodeMonitoringFailoverPlugin();
    plugin.init(propertySet, hostInfo, next, log);
    return plugin;
  }
}
