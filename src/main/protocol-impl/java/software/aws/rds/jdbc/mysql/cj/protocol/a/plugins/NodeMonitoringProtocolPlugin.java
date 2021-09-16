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

import com.mysql.cj.conf.*;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.SocketConnection;
import org.jboss.util.NullArgumentException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class NodeMonitoringProtocolPlugin implements IProtocolPlugin {

  protected static int THREAD_SLEEP_WHEN_INACTIVE_MILLIS = 1000;

  protected IProtocolPlugin next;
  protected Log log;
  protected PropertySet propertySet;
  protected HostInfo hostInfo;
  protected volatile boolean isNodeDead = false;
  protected volatile int nodeCheckTimeMillis;
  protected Connection monitoringConn = null;
  protected Thread monitoringThread = null;
  protected volatile boolean isMonitoring = false;

  public NodeMonitoringProtocolPlugin() {}

  @Override
  public void init(PropertySet propertySet, HostInfo hostInfo, IProtocolPlugin next, Log log) {
    if(next == null) {
      throw new NullArgumentException("next");
    }

    if(log == null) {
      throw new NullArgumentException("log");
    }

    if(propertySet == null) {
      throw new NullArgumentException("propertySet");
    }

    if(hostInfo == null) {
      throw new NullArgumentException("hostInfo");
    }

    this.hostInfo = hostInfo;
    this.propertySet = propertySet;
    this.log = log;
    this.next = next;

    this.nodeCheckTimeMillis = this.propertySet.getIntegerProperty(PropertyKey.nodeMonitoringIntervalMs).getValue();
  }

  @Override
  public void beforeHandshake(SocketConnection socketConnection) throws IOException {
    this.next.beforeHandshake(socketConnection);
  }

  @Override
  public void afterHandshake(SocketConnection socketConnection) throws IOException {
    this.log.logTrace("[NodeMonitoringProtocolPlugin.afterHandshake]");

    try {
      // open a new connection
      Map<String, String> properties = new HashMap<String, String>();
      properties.put(PropertyKey.disableProtocolPlugins.getKeyName(), "true");
      properties.put(PropertyKey.socketTimeout.getKeyName(), "3000");
      properties.put(PropertyKey.tcpKeepAlive.getKeyName(), this.propertySet.getBooleanProperty(PropertyKey.tcpKeepAlive).getStringValue());
      String tmp = this.propertySet.getBooleanProperty(PropertyKey.tcpKeepAlive).getStringValue();
      //TODO: any other properties to pass? like socket factory
      this.monitoringConn = new ConnectionImpl(copy(this.hostInfo, properties));

      // start monitoring thread
      this.isMonitoring = false;
      this.isNodeDead = false;

      this.monitoringThread = new Thread(() -> {
        try {
          boolean isValid = true;
          while(true) {

            if(this.isMonitoring) {
              TimeUnit.MILLISECONDS.sleep(this.nodeCheckTimeMillis);

              try {
                isValid = this.monitoringConn.isValid(this.nodeCheckTimeMillis / 1000);
                this.log.logTrace(String.format("[NodeMonitoringProtocolPlugin::thread] node '%s' is %s.", this.hostInfo.getHost(), isValid ? "*alive*" : "*dead*"));
                this.isNodeDead = !isValid;
              } catch (SQLException sqlEx) {
                // problem with connection?
                this.log.logTrace(String.format("[NodeMonitoringProtocolPlugin::thread] node '%s' is dead.", this.hostInfo.getHost()), sqlEx);
                this.isNodeDead = true;
              }

            } else {
              TimeUnit.MILLISECONDS.sleep(THREAD_SLEEP_WHEN_INACTIVE_MILLIS);
            }
          }
        }
        catch(InterruptedException intEx) {
          // do nothing; exit thread
        }
      });
      this.monitoringThread.start();
    }
    catch(SQLException sqlEx) {
      //TODO: report exception properly
      throw ExceptionFactory.createException(sqlEx.getMessage(), sqlEx);
    }

    this.next.afterHandshake(socketConnection);
  }

  @Override
  public void beforeCommand(IReadContext context) throws IOException {
    this.next.beforeCommand(context);
  }

  @Override
  public void negotiateReadTimeouts(IReadTimeoutHolder timeoutHolder) {
    this.log.logTrace("[NodeMonitoringProtocolPlugin.negotiateReadTimeouts]");

    // the value may change; re-read it
    this.nodeCheckTimeMillis = this.propertySet.getIntegerProperty(PropertyKey.nodeMonitoringIntervalMs).getValue();

    timeoutHolder.setReadPortionTimeout(leastValueIgnoreZeros(timeoutHolder.getReadTimeout(), timeoutHolder.getReadPortionTimeout(), this.nodeCheckTimeMillis));
  }

  @Override
  public void beforeRead(IReadContext context) throws IOException {

    if(this.monitoringThread != null) {
      this.log.logTrace("[NodeMonitoringProtocolPlugin.beforeRead]: monitoring is activated");
      this.isMonitoring = true;
      this.isNodeDead = false;
    }

    this.next.beforeRead(context);
  }

  @Override
  public void beforeReadPortion(IReadContext context) throws IOException {
    this.next.beforeReadPortion(context);
  }

  @Override
  public boolean readNextPortion(IReadContext context) throws IOException {
    if(this.isMonitoring && this.isNodeDead) {
      return false;
    }
    return this.next.readNextPortion(context);
  }

  @Override
  public void readPortionInterceptException(IReadContext context, Throwable throwable) throws IOException {
    this.next.readPortionInterceptException(context, throwable);
  }

  @Override
  public Object afterReadPortion(IReadContext context, Object payload) throws IOException {
    return this.next.afterReadPortion(context, payload);
  }

  @Override
  public void afterRead(IReadContext context) throws IOException {
    this.log.logTrace("[NodeMonitoringProtocolPlugin.beforeRead]: monitoring is deactivated");
    this.isMonitoring = false;

    this.next.afterRead(context);
  }

  @Override
  public void afterCommand(IReadContext context) throws IOException {
    this.next.afterCommand(context);
  }

  @Override
  public void releaseResources() {
    if(this.monitoringThread != null && !this.monitoringThread.isInterrupted()) {
      this.monitoringThread.interrupt();
      this.monitoringThread = null;
    }

    try {
      if (this.monitoringConn != null && !this.monitoringConn.isClosed()) {
        this.monitoringConn.close();
        this.monitoringConn = null;
      }
    }
    catch(Exception ex) {
      /* ignore */
    }

    this.next.releaseResources();
  }

  protected HostInfo copy(HostInfo src, Map<String, String> props) {
    return new HostInfo(
            null,
            src.getHost(),
            src.getPort(),
            src.getUser(),
            src.getPassword(),
            src.isPasswordless(),
            props);
  }

  protected long leastValueIgnoreZeros(long... v) {
    return Arrays.stream(v).filter((x) -> x > 0).min().orElse(0);
  }
}
