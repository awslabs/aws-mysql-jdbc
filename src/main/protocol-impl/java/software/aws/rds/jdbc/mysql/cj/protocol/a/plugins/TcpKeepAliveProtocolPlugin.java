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
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.SocketConnection;
import org.jboss.util.NullArgumentException;

import java.io.IOException;

public class TcpKeepAliveProtocolPlugin implements IProtocolPlugin {

  protected Log log;
  protected IProtocolPlugin next;
  protected PropertySet propertySet;

  public TcpKeepAliveProtocolPlugin() {}

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

    this.propertySet = propertySet;
    this.log = log;
    this.next = next;
  }

  @Override
  public void beforeHandshake(SocketConnection socketConnection) throws IOException {
    boolean tckKeepAlive = this.propertySet.getBooleanProperty(PropertyKey.tcpKeepAlive).getValue();
    this.log.logTrace("[TcpKeepAliveProtocolPlugin.beforeHandshake]: set socket tcpKeepAlive=" + tckKeepAlive);
    socketConnection.getMysqlSocket().setKeepAlive(tckKeepAlive);

    this.next.beforeHandshake(socketConnection);
  }

  @Override
  public void afterHandshake(SocketConnection socketConnection) throws IOException {
    this.next.afterHandshake(socketConnection);
  }

  @Override
  public void beforeCommand(IReadContext context) throws IOException {
    boolean tckKeepAlive = this.propertySet.getBooleanProperty(PropertyKey.tcpKeepAlive).getValue();
    this.log.logTrace("[TcpKeepAliveProtocolPlugin.beforeCommand]: set socket tcpKeepAlive=" + tckKeepAlive);
    context.getSocketConnection().getMysqlSocket().setKeepAlive(tckKeepAlive);

    this.next.beforeCommand(context);
  }

  @Override
  public void negotiateReadTimeouts(IReadTimeoutHolder timeoutHolder) {
    this.next.negotiateReadTimeouts(timeoutHolder);
  }

  @Override
  public void beforeRead(IReadContext context) throws IOException {
    this.next.beforeRead(context);
  }

  @Override
  public void beforeReadPortion(IReadContext context) throws IOException {
    this.next.beforeReadPortion(context);
  }

  @Override
  public boolean readNextPortion(IReadContext context) throws IOException {
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
    this.next.afterRead(context);
  }

  @Override
  public void afterCommand(IReadContext context) throws IOException {
    this.next.afterCommand(context);
  }

  @Override
  public void releaseResources() {
    this.next.releaseResources();
  }
}
