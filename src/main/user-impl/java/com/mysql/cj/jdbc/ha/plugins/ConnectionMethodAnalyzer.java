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

import com.mysql.cj.Messages;

import java.sql.SQLException;

public class ConnectionMethodAnalyzer {

    private static final String BEGIN_REGEX = "(?i) *begin.*";
    private static final String COMMIT_REGEX = "(?i) *commit.*";
    private static final String ROLLBACK_REGEX = "(?i) *rollback.*";
    private static final String SET_AUTOCOMMIT_REGEX = "(?i) *set *autocommit .*";
    private static final String START_TRANSACTION_REGEX = "(?i) *start *transaction.*";
    private static final String SET_REGEX = "(?i) *set .*";

    // executeUpdate, executeLargeUpdate, and executeBatch throw an error when setReadOnly(true) has been called
    public boolean isReadOnlyExecuteMethod(String methodName) {
        return "execute".equals(methodName) || "executeQuery".equals(methodName);
    }

    // SQL statements such as "SET autocommit = 0", "START TRANSACTION", and "COMMIT" cannot be executed with executeQuery
    public boolean isExecuteMethod(String methodName) {
        return "execute".equals(methodName);
    }

    public boolean isExecuteDml(String methodName, Object[] args) {
        return isReadOnlyExecuteMethod(methodName)
                && !isExecuteStartingTransaction(methodName, args)
                && !isExecuteClosingTransaction(methodName, args)
                && !isExecuteSettingState(methodName, args);
    }

    public boolean isExecuteSettingState(String methodName, Object[] args) {
        if (!isExecuteMethod(methodName))  {
            return false;
        }

        if (args == null || args.length < 1) {
            return false;
        }

        final String sql = (String) args[0];
        return sql.matches(SET_REGEX);
    }

    public boolean isExecuteStartingTransaction(String methodName, Object[] args) {
        if (!isExecuteMethod(methodName))  {
            return false;
        }

        if (args == null || args.length < 1) {
            return false;
        }

        final String sql = (String) args[0];
        return sql.matches(BEGIN_REGEX) || sql.matches(START_TRANSACTION_REGEX);
    }

    public boolean isMethodClosingTransaction(String methodName, Object[] args) {
         if ("commit".equals(methodName) || "rollback".equals(methodName)) {
             return true;
         }
         return isExecuteClosingTransaction(methodName, args);
    }

    public boolean isExecuteClosingTransaction(String methodName, Object[] args) {
        if (!isExecuteMethod(methodName)) {
            return false;
        }

        if (args == null || args.length < 1) {
            return false;
        }

        final String sql = (String) args[0];
        return sql.matches(COMMIT_REGEX) || sql.matches(ROLLBACK_REGEX);
    }

    public boolean isSetReadOnlyTrue(String methodName, Object[] args) {
        if (!"setReadOnly".equals(methodName) || args == null || args.length < 1) {
            return false;
        }
        return Boolean.TRUE.equals(args[0]);
    }

    public boolean isSetReadOnlyFalse(String methodName, Object[] args) {
        if (!"setReadOnly".equals(methodName) || args == null || args.length < 1) {
            return false;
        }
        return Boolean.FALSE.equals(args[0]);
    }

    public boolean isSetAutoCommitTrue(String methodName, Object[] args) {
        if (!isSetAutoCommit(methodName, args)) {
            return false;
        }

        if ("setAutoCommit".equals(methodName)) {
            return Boolean.TRUE.equals(args[0]);
        }

        try {
            return getAutoCommitValueFromSqlStatement(args);
        } catch (SQLException e) {
            return false;
        }
    }

    public boolean isSetAutoCommitFalse(String methodName, Object[] args) {
        if (!isSetAutoCommit(methodName, args)) {
            return false;
        }

        if ("setAutoCommit".equals(methodName)) {
            return Boolean.FALSE.equals(args[0]);
        }

        try {
            return !getAutoCommitValueFromSqlStatement(args);
        } catch (SQLException e) {
            return false;
        }
    }

    public boolean isSetAutoCommit(String methodName, Object[] args){
        if (args == null || args.length < 1) {
            return false;
        }

        if ("setAutoCommit".equals(methodName)) {
            return true;
        }

        if (!isExecuteMethod(methodName)) {
            return false;
        }

        final String sql = (String) args[0];
        return sql.matches(SET_AUTOCOMMIT_REGEX);
    }

    public boolean getAutoCommitValueFromSqlStatement(Object[] args) throws SQLException {
        String sql = (String) args[0];
        sql = sql.trim();
        sql = sql.toLowerCase();

        int equalsCharacterIndex = sql.indexOf("=");
        if (equalsCharacterIndex == -1) {
            throw new SQLException(Messages.getString("ConnectionMethodAnalyzer.errorParsingAutocommitSql"));
        }
        sql = sql.substring(equalsCharacterIndex + 1);

        if (sql.contains(";")) {
            sql = sql.substring(0, sql.indexOf(";"));
        }

        sql = sql.trim();
        if ("false".equals(sql) || "0".equals(sql)) {
            return false;
        } else if ("true".equals(sql) || "1".equals(sql)) {
            return true;
        } else {
            throw new SQLException(Messages.getString("ConnectionMethodAnalyzer.errorParsingAutocommitSql"));
        }
    }
}
