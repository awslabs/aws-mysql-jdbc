package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;

public interface IFailoverPluginFactory {
  IFailoverPlugin getInstance(PropertySet propertySet, HostInfo hostInfo, IFailoverPlugin next, Log log);
}
