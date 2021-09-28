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

package software.aws.rds.jdbc.mysql.cj.protocol.a.plugins;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;
import org.jboss.util.NullArgumentException;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.Callable;

public class ProtocolPluginManager {

  /* THIS CLASS IS NOT MULTI-THREADING SAFE */
  /* IT'S EXPECTED TO HAVE ONE INSTANCE OF THIS MANAGER PER JDBC CONNECTION */

  protected static final String DEFAULT_PLUGIN_FACTORIES = String.format("%s,%s,%s",
          SocketTimeoutProtocolPluginFactory.class.getName(),
          TcpKeepAliveProtocolPluginFactory.class.getName(),
          NodeMonitoringProtocolPluginFactory.class.getName());

//  protected static final String DEFAULT_PLUGIN_FACTORIES = String.format("%s,%s",
//          SocketTimeoutFailureAnalyzerFactory.class.getName(),
//          TcpKeepAliveFailureAnalyzerFactory.class.getName());

  //protected static final String DEFAULT_PLUGIN_FACTORIES = SocketTimeoutFailureAnalyzerFactory.class.getName();
  //protected static final String DEFAULT_PLUGIN_FACTORIES = TcpKeepAliveFailureAnalyzerFactory.class.getName();
  //protected static final String DEFAULT_PLUGIN_FACTORIES = NodeMonitoringFailureAnalyzerFactory.class.getName();
  //protected static final String DEFAULT_PLUGIN_FACTORIES = "";

  protected Log log;
  protected PropertySet propertySet = null;
  protected HostInfo hostInfo;
  protected IProtocolPlugin headPlugin = null;
  protected ExceptionInterceptor exceptionInterceptor = null;
  protected boolean isHandshakeCompleted = false;

  protected ReadContextImpl context = new ReadContextImpl();

  public ProtocolPluginManager(Log log) {
    if(log == null) {
      throw new NullArgumentException("log");
    }

    this.log = log;
  }

  public void init(PropertySet propertySet, HostInfo hostInfo, ExceptionInterceptor exceptionInterceptor) {
    this.propertySet = propertySet;
    this.exceptionInterceptor = exceptionInterceptor;
    this.hostInfo = hostInfo;

    String factoryClazzNames = propertySet.getStringProperty(PropertyKey.protocolPluginsFactories).getValue();

    if(StringUtils.isNullOrEmpty(factoryClazzNames)) {
      factoryClazzNames = DEFAULT_PLUGIN_FACTORIES;
    }

    this.headPlugin = new DefaultProtocolPluginFactory().getInstance(this.propertySet, this.hostInfo, null, this.log);

    if (!StringUtils.isNullOrEmpty(factoryClazzNames)) {
      IProtocolPluginFactory[] factories = Util.<IProtocolPluginFactory>loadClasses(factoryClazzNames,
              "MysqlIo.BadProtocolPluginFactory", this.exceptionInterceptor).toArray(new IProtocolPluginFactory[0]);

      // make a chain of analyzers with default one at the tail

      for (int i = factories.length - 1; i >= 0; i--) {
        IProtocolPlugin nextAnalyzer = factories[i].getInstance(this.propertySet, this.hostInfo, this.headPlugin, this.log);
        this.headPlugin = nextAnalyzer;
      }
    }

  }

  public void beforeHandshake(SocketConnection socketConnection) throws IOException {
    this.context.setSocketConnection(socketConnection);
    this.headPlugin.beforeHandshake(socketConnection);
  }

  public void afterHandshake(SocketConnection socketConnection) throws IOException {
    this.headPlugin.afterHandshake(socketConnection);
    this.isHandshakeCompleted = true;
  }

  public void beforeCommand(SocketConnection socketConnection, int allowedTimeMillis) throws IOException {
    if(!this.isHandshakeCompleted) {
      return;
    }

    this.log.logTrace("[ProtocolPluginManager.beforeCommand]: allowedTimeMillis=" + allowedTimeMillis);

    this.context.setElapsedTime(0);
    this.context.setReadTimeout(allowedTimeMillis);
    this.context.setReadPortionTimeout(0);

    this.headPlugin.beforeCommand(this.context);
  }

  public <T> T readPortion(SocketConnection socketConnection, Callable<T> readFunc) throws IOException {
    // we don't want to engage enhanced logic until handshaking is completed
    // TODO: do we need it?
    if(!this.isHandshakeCompleted) {
      try {
        return readFunc.call();
      }
      catch(Exception ex) {
        //TODO: need a better exception?
        throw ExceptionFactory.createException(ex.getMessage(), ex);
      }
    }

    T result = null;
    boolean continueReading;
    long portionStartTime;
    SocketTimeoutException lastSocketTimeoutException = null;

    this.context.setElapsedTime(0);

    this.headPlugin.beforeRead(this.context);

    configureReadTimeouts();

    try {
      do {
        this.log.logTrace("[ProtocolPluginManager.readPortion#3]: loop, elapsedTimeMillis=" + this.context.getElapsedTime());

        this.headPlugin.beforeReadPortion(this.context);

        portionStartTime = System.currentTimeMillis();

        try {
          result = null;
          result = readFunc.call();

          long portionEndTime = System.currentTimeMillis();
          this.context.setElapsedTime(this.context.getElapsedTime() + portionEndTime - portionStartTime);
          this.log.logTrace("[ProtocolPluginManager.readPortion#6]: updated elapsedTimeMillis=" + this.context.getElapsedTime());
        }
        catch(SocketTimeoutException ste) {
          //need to read another portion
          //ignore this exception and continue

          long portionEndTime = System.currentTimeMillis();
          this.context.setElapsedTime(this.context.getElapsedTime() + portionEndTime - portionStartTime);
          this.log.logTrace("[ProtocolPluginManager.readPortion#7]: updated elapsedTimeMillis=" + this.context.getElapsedTime());

          this.log.logTrace("[ProtocolPluginManager.readPortion#8]: SocketTimeoutException handled");

          // socket may be reset due to communication issues
          if(this.context.getSocketConnection().getMysqlSocket() == null) {
            throw ste;
          }

          lastSocketTimeoutException = ste;
        }
        catch(SocketException se) {

          long portionEndTime = System.currentTimeMillis();
          this.context.setElapsedTime(this.context.getElapsedTime() + portionEndTime - portionStartTime);
          this.log.logTrace("[ProtocolPluginManager.readPortion#9]: updated elapsedTimeMillis=" + this.context.getElapsedTime());

          if (se.getMessage() == "Connection reset") {
            this.log.logTrace("[ProtocolPluginManager.readPortion#10]: SocketException, Connection reset", se);
            throw se; //socket is dead; end of story
          } else {
            this.log.logTrace("[ProtocolPluginManager.readPortion#11]: SocketException", se);
            this.headPlugin.readPortionInterceptException(this.context, se);
          }
        }
        catch(Throwable throwable) {
          long portionEndTime = System.currentTimeMillis();
          this.context.setElapsedTime(this.context.getElapsedTime() + portionEndTime - portionStartTime);
          this.log.logTrace("[ProtocolPluginManager.readPortion#12]: updated elapsedTimeMillis=" + this.context.getElapsedTime());

          this.log.logTrace("[ProtocolPluginManager.readPortion#13]: Throwable", throwable);
          this.headPlugin.readPortionInterceptException(this.context, throwable);
        }

        result = (T)this.headPlugin.afterReadPortion(this.context, result);

        if (result != null) {
          // data has received
          continueReading = false;
        } else {
          // data hasn't received yet; check if allowed time is over
          continueReading = this.headPlugin.readNextPortion(this.context);
        }
        this.log.logTrace("[ProtocolPluginManager.readPortion#16]: continueReading=" + continueReading);

      } while (continueReading);

      if (result == null) {
        // data hasn't received and allowed time is over
        this.log.logTrace("[ProtocolPluginManager.readPortion#17]: throw SocketTimeoutException");

        try {
          this.context.getSocketConnection().forceClose();
        } catch (Exception ex) {
          // ignore
        }

        if(lastSocketTimeoutException != null) {
          throw lastSocketTimeoutException;
        } else {
          throw new SocketTimeoutException("Read timed out");
        }
      }
    }
    finally {
      this.headPlugin.afterRead(this.context);
    }
    return result;
  }

  protected void configureReadTimeouts() {
    IReadTimeoutHolder holder = new ReadTimeoutHolderImpl();
    holder.setReadTimeout(this.context.getReadTimeout());
    this.headPlugin.negotiateReadTimeouts(holder);
    long portionTimeoutMillis = holder.getReadPortionTimeout();
    if (portionTimeoutMillis <= 0) {
      portionTimeoutMillis = holder.getReadTimeout();
    }

    this.log.logTrace("[ProtocolPluginManager.configureReadTimeouts]: readTimeout=" + holder.getReadTimeout() + ", readPortionTimeout=" + holder.getReadPortionTimeout());

    try {
      this.log.logTrace("[ProtocolPluginManager.configureReadTimeouts]: set socket timeout to " + portionTimeoutMillis);
      this.context.getSocketConnection().getMysqlSocket().setSoTimeout((int)portionTimeoutMillis);
    } catch (Exception ex) {
      /* Ignore if the platform does not support it */
    }

    this.context.setReadTimeout(holder.getReadTimeout());
    this.context.setReadPortionTimeout(holder.getReadPortionTimeout());
  }

  public void afterCommand(SocketConnection socketConnection) throws IOException {
    if(!this.isHandshakeCompleted) {
      return;
    }

    this.log.logTrace("[ProtocolPluginManager.afterCommand]");
    this.headPlugin.afterCommand(this.context);

    if(this.context.getSocketConnection() == null ||
      this.context.getSocketConnection().getMysqlSocket() == null ||
      this.context.getSocketConnection().getMysqlSocket().isClosed()) {
      // socket is closed
      // this connection (including this instance of protocol) is dead
      this.releaseResources();
    }
  }

  public void releaseResources() {
    this.log.logTrace("[ProtocolPluginManager.releaseResources]");
    this.headPlugin.releaseResources();
  }
}
