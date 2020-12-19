/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
 * Modifications Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.*;
import com.mysql.cj.protocol.a.*;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.EOFException;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

public class NativeSessionTest {
    @Test
    public void testExecSql_EOFExceptionThrowsCjCommunicationsException() throws IOException {
        ProtocolEntityFactory mockFactory = Mockito.mock(ProtocolEntityFactory.class);
        ColumnDefinition mockDefinition = Mockito.mock(ColumnDefinition.class);
        TransactionEventHandler mockTransactionManager = Mockito.mock(TransactionEventHandler.class);
        NativeProtocolProvider mockProtocolProvider = Mockito.mock(NativeProtocolProvider.class);
        NativeProtocol mockProtocol = Mockito.mock(NativeProtocol.class);
        SocketConnectionProvider mockSocketProvider = Mockito.mock(SocketConnectionProvider.class);
        SocketConnection mockSocket = Mockito.mock(SocketConnection.class);

        when(mockProtocol.getServerSession()).thenReturn(Mockito.mock(NativeServerSession.class));
        when(mockProtocol.getAuthenticationProvider()).thenReturn(Mockito.mock(AuthenticationProvider.class));

        HostInfo host = new HostInfo(null, "host-endpoint", 1234, null, null);
        PropertySet props = new JdbcPropertySetImpl();
        NativeSession testSession = new NativeSession(host, props, mockProtocolProvider, mockSocketProvider);

        Query mockQuery = Mockito.mock(Query.class);
        String sqlQuery = "SELECT * FROM arbitrary";
        when(mockProtocolProvider.getProtocolInstance(eq(testSession), any(SocketConnection.class), eq(props), any(Log.class), eq(mockTransactionManager))).thenReturn(mockProtocol);
        when(mockProtocol.sendQueryString(eq(mockQuery), eq(sqlQuery), nullable(String.class), eq(-1), eq(false), eq(mockDefinition), eq(mockFactory))).thenThrow(EOFException.class);
        when(mockSocketProvider.getSocketConnection()).thenReturn(mockSocket);

        testSession.connect(host, "", "", "", 30000,  mockTransactionManager);

        assertThrows(
                CJCommunicationsException.class,
                () -> testSession.execSQL(mockQuery, sqlQuery, -1, null, false, mockFactory, mockDefinition, false));
    }

}
