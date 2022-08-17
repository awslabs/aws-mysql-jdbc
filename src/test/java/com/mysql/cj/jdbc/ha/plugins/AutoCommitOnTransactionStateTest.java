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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

public class AutoCommitOnTransactionStateTest {

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
    public void test_setReadOnly() throws SQLException {
        assertThrows(SQLException.class,
                () -> AutoCommitOnTransactionState.INSTANCE.getNextState("setReadOnly", new Object[]{ false }));

        IState nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("setReadOnly", new Object[]{ true });
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);
    }

    @Test
    public void test_setAutoCommit() throws SQLException {
        IState nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("setAutoCommit", new Object[]{ true });
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("setAutoCommit", new Object[]{ false });
        assertEquals(AutoCommitOffTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("execute", new Object[]{ "SeT aUtOcOmMiT = 1" });
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("execute", new Object[]{ "SeT aUtOcOmMiT = 0" });
        assertEquals(AutoCommitOffTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("execute", new Object[]{ "SeT aUtOcOmMiT = tRuE" });
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("execute", new Object[]{ "SeT aUtOcOmMiT = fAlSe" });
        assertEquals(AutoCommitOffTransactionState.INSTANCE, nextState);
    }

    @Test
    public void test_execute() throws SQLException {
        IState nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("execute", new Object[]{ "SELECT 1" });
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("executeQuery", new Object[]{ "SELECT 1" });
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("executeUpdate", new Object[]{ "UPDATE employees SET name = 'John' WHERE id = 1" });
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("executeLargeUpdate", new Object[]{ "UPDATE employees SET name = 'John' WHERE id = 1" });
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);
    }

    @Test
    public void test_startTransaction() throws SQLException {
        IState nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("execute", new Object[]{ "bEgIn" });
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("executeUpdate", new Object[]{ "sTarT tRaNsAction" });
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("executeLargeUpdate", new Object[]{ "StaRt TransActioN rEad Only" });
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("execute", new Object[]{ "stART tRanSACtion ReaD WRITe" });
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);
    }

    @Test
    public void test_closeTransaction() throws SQLException {
        IState nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("execute", new Object[]{ "cOMmit" });
        assertEquals(AutoCommitOnTransactionBoundaryState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("executeUpdate", new Object[]{ "rOllBACk" });
        assertEquals(AutoCommitOnTransactionBoundaryState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("executeLargeUpdate", new Object[]{ "cOMmit" });
        assertEquals(AutoCommitOnTransactionBoundaryState.INSTANCE, nextState);

        // execute("COMMIT")/execute("ROLLBACK") will not throw an error, but the driver will throw an error if
        // commit()/rollback() are called
        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("commit", new Object[]{});
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("rollback", new Object[]{});
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);
    }

    @Test
    public void test_otherMethods() throws SQLException {
        IState nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("getAutoCommit", new Object[]{});
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("isClosed", new Object[]{});
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState("setCatalog", new Object[]{ "catalog" });
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);
    }

    @Test
    public void test_exceptions() {
        IState nextState = AutoCommitOnTransactionState.INSTANCE.getNextState(exception);
        assertEquals(AutoCommitOnTransactionState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState(failoverException);
        assertEquals(AutoCommitOnState.INSTANCE, nextState);

        nextState = AutoCommitOnTransactionState.INSTANCE.getNextState(communicationsException);
        assertEquals(AutoCommitOnTransactionBoundaryState.INSTANCE, nextState);
    }

    @Test
    public void test_shouldSwitchReader() {
        assertFalse(AutoCommitOnTransactionState.INSTANCE.shouldSwitchReader());
    }
}
