// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0
// (GPLv2), as published by the Free Software Foundation, with the
// following additional permissions:
//
// This program is distributed with certain software that is licensed
// under separate terms, as designated in a particular file or component
// or in the license documentation. Without limiting your rights under
// the GPLv2, the authors of this program hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have included with the program.
//
// Without limiting the foregoing grant of rights under the GPLv2 and
// additional permission as to separately licensed software, this
// program is also subject to the Universal FOSS Exception, version 1.0,
// a copy of which can be found along with its FAQ at
// http://oss.oracle.com/licenses/universal-foss-exception.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see 
// http://www.gnu.org/licenses/gpl-2.0.html.

package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.JdbcConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

public class ReadWriteStateTest {

    @Mock Exception exception;
    @Mock SQLException failoverException;
    @Mock SQLException communicationsException;
    
    @Mock JdbcConnection conn;

    private AutoCloseable closeable;

    @AfterEach
    void cleanUp() throws Exception {
        closeable.close();
    }

    @BeforeEach
    void init() {
        closeable = MockitoAnnotations.openMocks(this);

        when(failoverException.getSQLState()).thenReturn(MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN);
        when(communicationsException.getSQLState()).thenReturn(MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE);
    }

    @Test
    public void test_setReadOnly() throws SQLException {
        IState nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "setReadOnly", new Object[]{ false });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        when(conn.getAutoCommit()).thenReturn(true);
        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "setReadOnly", new Object[]{ true });
        assertEquals(ReadWriteSplittingStateMachine.AUTOCOMMIT_ON_STATE, nextState);

        when(conn.getAutoCommit()).thenReturn(false);
        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "setReadOnly", new Object[]{ true });
        assertEquals(ReadWriteSplittingStateMachine.AUTOCOMMIT_OFF_STATE, nextState);
    }

    @Test
    public void test_setAutoCommit() throws SQLException {
        IState nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "setAutoCommit", new Object[]{ true });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "setAutoCommit", new Object[]{ false });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "set autocommit = 1" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "set autocommit = 0;" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "SET AUTOCOMMIT = TRUE" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "SET AUTOCOMMIT = FALSE;" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "   SeT  aUtOcOmMiT = 1 ; " });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ " SeT aUtOcOmMiT  = 0  ;" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "SeT aUtOcOmMiT = tRuE;" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ " SeT  aUtOcOmMiT  =  fAlSe  ;  " });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);
    }

    @Test
    public void test_execute() throws SQLException {
        IState nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "SELECT 1" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "executeQuery", new Object[]{ "SELECT 1" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);
    }

    @Test
    public void test_startTransaction() throws SQLException {
        IState nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "begin" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "START TRANSACTION;" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "  bEgIn; " });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "  sTarT  tRaNsAction ; " });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "StaRt  TransActioN  rEad Only;" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "  stART tRanSACtion ReaD WRITe  ;" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);
    }

    @Test
    public void test_closeTransaction() throws SQLException {
        IState nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "commit" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "ROLLBACK;" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "  cOMmit   ;" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ " rOllBACk ;  " });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "execute", new Object[]{ "cOMmit ; " });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "commit", new Object[]{});
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "rollback", new Object[]{});
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);
    }

    @Test
    public void test_otherMethods() throws SQLException {
        IState nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "getAutoCommit", new Object[]{});
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "isClosed", new Object[]{});
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(conn, "setCatalog", new Object[]{ "catalog" });
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);
    }

    @Test
    public void test_exceptions() {
        IState nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(exception);
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(failoverException);
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);

        nextState = ReadWriteSplittingStateMachine.READ_WRITE_STATE.getNextState(communicationsException);
        assertEquals(ReadWriteSplittingStateMachine.READ_WRITE_STATE, nextState);
    }

    @Test
    public void test_shouldSwitchReader() {
        assertFalse(ReadWriteSplittingStateMachine.READ_WRITE_STATE.isTransactionBoundary());
    }
}
