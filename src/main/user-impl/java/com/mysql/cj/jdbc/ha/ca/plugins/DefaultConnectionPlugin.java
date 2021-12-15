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

import com.mysql.cj.log.Log;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Callable;

/**
 * This connection plugin will always be the last plugin in the connection plugin chain,
 * and will invoke the JDBC method passed down the chain.
 */
public class DefaultConnectionPlugin implements IConnectionPlugin {

  protected Log logger;

  public DefaultConnectionPlugin(Log logger) {
    if (logger == null) {
      throw new IllegalArgumentException(NullArgumentMessage.getMessage("logger"));
    }

    this.logger = logger;
  }

  @Override
  public Object execute(Class<?> methodInvokeOn, String methodName, Callable<?> executeSqlFunc) throws Exception {
    try {
      return executeSqlFunc.call();
    } catch (InvocationTargetException invocationTargetException) {
      Throwable targetException = invocationTargetException.getTargetException();
      this.logger.logTrace(
          String.format("[DefaultConnectionPlugin.execute]: method=%s.%s, exception: ",
              methodInvokeOn.getName(), methodName), targetException);
      if (targetException instanceof Error) {
        throw (Error) targetException;
      }
      throw (Exception) targetException;
    } catch (Exception ex) {
      this.logger.logTrace(
          String.format("[DefaultConnectionPlugin.execute]: method=%s.%s, exception: ", methodInvokeOn.getName(), methodName), ex);
      throw ex;
    }
  }

  @Override
  public void releaseResources() {
    // do nothing
  }
}
