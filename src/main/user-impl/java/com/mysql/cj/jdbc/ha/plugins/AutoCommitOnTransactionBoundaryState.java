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

public enum AutoCommitOnTransactionBoundaryState implements IState {

    INSTANCE;

    private ConnectionMethodAnalyzer analyzer = new ConnectionMethodAnalyzer();

    AutoCommitOnTransactionBoundaryState() {
        // singleton class - do not instantiate elsewhere
    }

    @Override
    public IState getNextState(String methodName, Object[] args) {
        if (analyzer.isSetReadOnlyFalse(methodName, args)) {
            return ReadWriteState.INSTANCE;
        }

        if (analyzer.isSetAutoCommitFalse(methodName, args)) {
            return AutoCommitOffState.INSTANCE;
        }

        if (analyzer.isExecuteStartingTransaction(methodName, args)) {
            return AutoCommitOnTransactionState.INSTANCE;
        }

        if (analyzer.isSetReadOnlyTrue(methodName, args) || analyzer.isSetAutoCommitTrue(methodName, args)) {
            return AutoCommitOnState.INSTANCE;
        }

        if (analyzer.isExecuteDml(methodName, args) || analyzer.isMethodClosingTransaction(methodName, args)) {
            return this.INSTANCE;
        }

        return AutoCommitOnState.INSTANCE;
    }

    @Override
    public IState getNextState(Exception e) {
        if (analyzer.isFailoverException(e)) {
            return AutoCommitOnState.INSTANCE;
        }

        return AutoCommitOnTransactionBoundaryState.INSTANCE;
    }

    @Override
    public boolean shouldSwitchReader() {
        return true;
    }
}