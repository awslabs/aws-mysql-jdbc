package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;
import org.jboss.util.NullArgumentException;

import java.util.concurrent.Callable;

public class DefaultFailoverPlugin implements IFailoverPlugin {

  protected Log log;

  public DefaultFailoverPlugin() {}

  @Override
  public void init(PropertySet propertySet, HostInfo hostInfo, IFailoverPlugin next, Log log) {
    if (log == null) {
      throw new NullArgumentException("log");
    }

    this.log = log;
  }

  @Override
  public Object execute(String methodName, Callable executeSqlFunc) throws Exception {
    this.log.logTrace(
        String.format("[DefaultFailoverPlugin.execute]: method=%s >>>>>", methodName));
    try {
      return executeSqlFunc.call();
    } catch (Exception ex) {
      this.log.logTrace(
          String.format("[DefaultFailoverPlugin.execute]: method=%s, exception: ", methodName), ex);
      throw ex;
    } finally {
      this.log.logTrace(
          String.format("[DefaultFailoverPlugin.execute]: method=%s <<<<<", methodName));
    }
  }

  @Override
  public void releaseResources() {
    // do nothing
  }
}
