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

    @Mock
    Exception exception;

    @Mock
    SQLException failoverException;

    @Mock
    SQLException communicationsException;

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
    public void test_setReadOnly() {
        IState nextState = ReadWriteState.INSTANCE.getNextState("setReadOnly", new Object[]{ false });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("setReadOnly", new Object[]{ true });
        assertEquals(AutoCommitOnState.INSTANCE, nextState);
    }

    @Test
    public void test_setAutoCommit() {
        IState nextState = ReadWriteState.INSTANCE.getNextState("setAutoCommit", new Object[]{ true });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("setAutoCommit", new Object[]{ false });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("execute", new Object[]{ "SeT aUtOcOmMiT = 1" });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("execute", new Object[]{ "SeT aUtOcOmMiT = 0" });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("execute", new Object[]{ "SeT aUtOcOmMiT = tRuE" });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("execute", new Object[]{ "SeT aUtOcOmMiT = fAlSe" });
        assertEquals(ReadWriteState.INSTANCE, nextState);
    }

    @Test
    public void test_execute() {
        IState nextState = ReadWriteState.INSTANCE.getNextState("execute", new Object[]{ "SELECT 1" });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("executeQuery", new Object[]{ "SELECT 1" });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("executeUpdate", new Object[]{ "UPDATE employees SET name = 'John' WHERE id = 1" });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("executeLargeUpdate", new Object[]{ "UPDATE employees SET name = 'John' WHERE id = 1" });
        assertEquals(ReadWriteState.INSTANCE, nextState);
    }

    @Test
    public void test_startTransaction() {
        IState nextState = ReadWriteState.INSTANCE.getNextState("execute", new Object[]{ "bEgIn" });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("executeUpdate", new Object[]{ "sTarT tRaNsAction" });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("executeLargeUpdate", new Object[]{ "StaRt TransActioN rEad Only" });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("execute", new Object[]{ "stART tRanSACtion ReaD WRITe" });
        assertEquals(ReadWriteState.INSTANCE, nextState);
    }

    @Test
    public void test_closeTransaction() {
        IState nextState = ReadWriteState.INSTANCE.getNextState("execute", new Object[]{ "cOMmit" });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("executeUpdate", new Object[]{ "rOllBACk" });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("executeLargeUpdate", new Object[]{ "cOMmit" });
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("commit", new Object[]{});
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("rollback", new Object[]{});
        assertEquals(ReadWriteState.INSTANCE, nextState);
    }

    @Test
    public void test_otherMethods() {
        IState nextState = ReadWriteState.INSTANCE.getNextState("getAutoCommit", new Object[]{});
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("isClosed", new Object[]{});
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState("setCatalog", new Object[]{ "catalog" });
        assertEquals(ReadWriteState.INSTANCE, nextState);
    }

    @Test
    public void test_exceptions() {
        IState nextState = ReadWriteState.INSTANCE.getNextState(exception);
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState(failoverException);
        assertEquals(ReadWriteState.INSTANCE, nextState);

        nextState = ReadWriteState.INSTANCE.getNextState(communicationsException);
        assertEquals(ReadWriteState.INSTANCE, nextState);
    }

    @Test
    public void test_shouldSwitchReader() {
        assertFalse(ReadWriteState.INSTANCE.shouldSwitchReader());
    }
}
