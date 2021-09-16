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

import com.mysql.cj.Session;
import com.mysql.cj.TransactionEventHandler;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.MessageSender;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.protocol.a.*;

import java.io.Closeable;
import java.io.IOException;

public class EnhancedNativeProtocol extends NativeProtocol implements Closeable {

  protected HostInfo hostInfo;
  protected ProtocolPluginManager failureManager;

  public static EnhancedNativeProtocol getInstance(Session session, SocketConnection socketConnection, PropertySet propertySet, Log log,
                                           TransactionEventHandler transactionManager, HostInfo hi) {
    EnhancedNativeProtocol protocol = new EnhancedNativeProtocol(log);

    protocol.hostInfo = hi;
    protocol.init(session, socketConnection, propertySet, transactionManager);

    return protocol;
  }

  public EnhancedNativeProtocol(Log logger) {
    super(logger);
  }

  @Override
  public void init(Session sess, SocketConnection phConnection, PropertySet propSet, TransactionEventHandler trManager) {
    super.init(sess, phConnection, propSet, trManager);

    this.failureManager = new ProtocolPluginManager(log);
    this.failureManager.init(this.propertySet, this.hostInfo, this.exceptionInterceptor);
  }

  @Override
  protected MessageSender<NativePacketPayload> createPacketSender() throws IOException {
    return new SimplePacketSender(this.socketConnection.getMysqlOutput());
  }

  @Override
  protected MessageReader<NativePacketHeader, NativePacketPayload> createPacketReader() {
    return new EnhancedPacketReader(this.socketConnection, this.maxAllowedPacket);
  }

  @Override
  protected NativePacketHeader readMessageHeader(NativePacketPayload reuse) throws IOException {
    NativePacketHeader result = this.failureManager.readPortion(this.socketConnection, () -> super.readMessageHeader(reuse));
    return result;
  }

  @Override
  protected NativePacketPayload readMessageBody(NativePacketPayload reuse, NativePacketHeader header) throws IOException {
    NativePacketPayload result = this.failureManager.readPortion(this.socketConnection, () -> super.readMessageBody(reuse, header));
    return result;
  }

  @Override
  protected void beforeCommand(int allowedTimeMillis) throws IOException {
    this.failureManager.beforeCommand(this.socketConnection, allowedTimeMillis);
  }

  @Override
  protected void afterCommand() throws IOException {
    this.failureManager.afterCommand(this.socketConnection);
  }

  @Override
  public void releaseResources() {
    super.releaseResources();
    this.failureManager.releaseResources();
  }

  @Override
  public void connect(String user, String password, String database) {
    try {
      this.failureManager.beforeHandshake(this.socketConnection);
    } catch (IOException ioEx) {
      throw ExceptionFactory.createException(ioEx.getMessage(), ioEx);
    }

    super.connect(user, password, database);

    try {
      this.failureManager.afterHandshake(this.socketConnection);
    } catch (IOException ioEx) {
      throw ExceptionFactory.createException(ioEx.getMessage(), ioEx);
    }
  }

  @Override
  public void close() throws IOException {
    releaseResources();
  }

}
