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

import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;

import java.sql.SQLException;

public class ReadWriteSplittingStateMachine implements IStateMachine {

    protected static IState READ_WRITE_STATE = new ReadWriteState();
    protected static IState AUTOCOMMIT_ON_STATE = new AutoCommitOnState();
    protected static IState AUTOCOMMIT_ON_TRANSACTION_STATE = new AutoCommitOnTransactionState();
    protected static IState AUTOCOMMIT_ON_TRANSACTION_BOUNDARY_STATE = new AutoCommitOnTransactionBoundaryState();
    protected static IState AUTOCOMMIT_OFF_STATE = new AutoCommitOffState();
    protected static IState AUTOCOMMIT_OFF_TRANSACTION_STATE = new AutoCommitOffTransactionState();
    protected static IState AUTOCOMMIT_OFF_TRANSACTION_BOUNDARY_STATE = new AutoCommitOffTransactionBoundaryState();

    private final Log logger;
    private IState currentState = READ_WRITE_STATE;

    ReadWriteSplittingStateMachine(Log logger) {
        this.logger = logger;
    }

    @Override
    public void reset() {
        this.currentState = READ_WRITE_STATE;
    }

    @Override
    public void getNextState(JdbcConnection currentConnection, String methodName, Object[] args) throws SQLException {
        this.currentState = this.currentState.getNextState(currentConnection, methodName, args);
    }

    @Override
    public void getNextState(Exception e) {
        this.currentState = this.currentState.getNextState(e);
    }

    @Override
    public boolean isInReaderTransaction() {
        return this.currentState.equals(AUTOCOMMIT_ON_TRANSACTION_STATE)
                || this.currentState.equals(AUTOCOMMIT_OFF_TRANSACTION_STATE);
    }

    @Override
    public boolean isTransactionBoundary() {
        return this.currentState.isTransactionBoundary();
    }
}