/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;
import org.jboss.util.NullArgumentException;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

public class FailoverPluginManager {

  /* THIS CLASS IS NOT MULTI-THREADING SAFE */
  /* IT'S EXPECTED TO HAVE ONE INSTANCE OF THIS MANAGER PER JDBC CONNECTION */

  protected static final String DEFAULT_PLUGIN_FACTORIES = NodeMonitoringFailoverPluginFactory.class.getName();

  protected Log log;
  protected PropertySet propertySet = null;
  protected HostInfo hostInfo;
  protected IFailoverPlugin headPlugin = null;

  public FailoverPluginManager(Log log) {
    if(log == null) {
      throw new NullArgumentException("log");
    }

    this.log = log;
  }

  public void init(PropertySet propertySet, HostInfo hostInfo) {
    this.propertySet = propertySet;
    this.hostInfo = hostInfo;

    String factoryClazzNames = propertySet.getStringProperty(PropertyKey.failoverPluginsFactories).getValue();

    if(StringUtils.isNullOrEmpty(factoryClazzNames)) {
      factoryClazzNames = DEFAULT_PLUGIN_FACTORIES;
    }

    this.headPlugin = new DefaultFailoverPluginFactory().getInstance(this.propertySet, this.hostInfo, null, this.log);

    if (!StringUtils.isNullOrEmpty(factoryClazzNames)) {
      IFailoverPluginFactory[] factories = Util.<IFailoverPluginFactory>loadClasses(factoryClazzNames,
              "MysqlIo.BadFailoverPluginFactory", null).toArray(new IFailoverPluginFactory[0]);

      // make a chain of analyzers with default one at the tail

      for (int i = factories.length - 1; i >= 0; i--) {
        IFailoverPlugin nextAnalyzer = factories[i].getInstance(this.propertySet, this.hostInfo, this.headPlugin, this.log);
        this.headPlugin = nextAnalyzer;
      }
    }

  }

  public Object execute(String methodName, Callable executeSqlFunc) throws Exception {
    return this.headPlugin.execute(methodName, executeSqlFunc);
  }

  public void releaseResources() {
    this.log.logTrace("[FailoverPluginManager.releaseResources]");
    this.headPlugin.releaseResources();
  }
}
