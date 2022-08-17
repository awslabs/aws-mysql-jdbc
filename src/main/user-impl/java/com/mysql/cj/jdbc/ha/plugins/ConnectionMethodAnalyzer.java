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
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.mysql.cj.jdbc.ha.ConnectionUtils;

import javax.net.ssl.SSLException;
import java.io.EOFException;
import java.sql.SQLException;

public class ConnectionMethodAnalyzer {

    public boolean isExecuteMethod(String methodName) {
        return "execute".equals(methodName)
                || "executeQuery".equals(methodName)
                || "executeUpdate".equals(methodName)
                || "executeLargeUpdate".equals(methodName)
                || "executeBatch".equals(methodName);
    }

    // SQL statements such as "SET autocommit = 0", "START TRANSACTION", and "COMMIT" cannot be executed with executeQuery
    public boolean isExecuteMethodWithoutResultSet(String methodName) {
        return "execute".equals(methodName)
                || "executeUpdate".equals(methodName)
                || "executeLargeUpdate".equals(methodName)
                || "executeBatch".equals(methodName); // TODO: need to handle executeBatch parsing
    }

    public boolean isExecuteDml(String methodName, Object[] args) {
        return isExecuteMethod(methodName)
                && !isExecuteStartingTransaction(methodName, args)
                && !isExecuteClosingTransaction(methodName, args)
                && !isExecuteSettingState(methodName, args);
    }

    public boolean isExecuteSettingState(String methodName, Object[] args) {
        if (!isExecuteMethodWithoutResultSet(methodName))  {
            return false;
        }

        if (args == null || args.length < 1) {
            return false;
        }

        String sql = (String) args[0];
        sql = sql.trim();
        sql = sql.toLowerCase();
        return sql.startsWith("set ");
    }

    public boolean isExecuteStartingTransaction(String methodName, Object[] args) {
        if (!isExecuteMethodWithoutResultSet(methodName))  {
            return false;
        }

        if (args == null || args.length < 1) {
            return false;
        }

        String sql = (String) args[0];
        sql = sql.trim();
        sql = sql.toLowerCase();
        return sql.startsWith("begin") || sql.startsWith("start transaction");
    }

    public boolean isMethodClosingTransaction(String methodName, Object[] args) {
         if ("commit".equals(methodName) || "rollback".equals(methodName)) {
             return true;
         }
         return isExecuteClosingTransaction(methodName, args);
    }

    public boolean isExecuteClosingTransaction(String methodName, Object[] args) {
        if (!isExecuteMethodWithoutResultSet(methodName)) {
            return false;
        }

        if (args == null || args.length < 1) {
            return false;
        }

        String sql = (String) args[0];
        return "commit".equalsIgnoreCase(sql) || "rollback".equalsIgnoreCase(sql);
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

        if (!isExecuteMethodWithoutResultSet(methodName)) {
            return false;
        }

        String sql = (String) args[0];
        sql = sql.trim();
        sql = sql.toLowerCase();
        return sql.startsWith("set autocommit");
    }

    public boolean getAutoCommitValueFromSqlStatement(Object[] args) throws SQLException {
        String sql = (String) args[0];
        sql = sql.trim();
        sql = sql.toLowerCase();

        int equalsCharacterIndex = sql.indexOf("=");
        if (equalsCharacterIndex == -1) {
            throw new SQLException(Messages.getString("ConnectionMethodAnalyzer.1"));
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
            throw new SQLException(Messages.getString("ConnectionMethodAnalyzer.1"));
        }
    }

    public boolean isFailoverException(Exception e) {
        if (!(e instanceof SQLException)) {
            return false;
        }

        SQLException sqlException = (SQLException) e;
        return MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN.equals(sqlException.getSQLState())
                || MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_CHANGED.equals(sqlException.getSQLState());
    }

    public boolean isCommunicationsException(Exception e) {
        if (e instanceof CommunicationsException || e instanceof CJCommunicationsException) {
            return true;
        }

        if (e instanceof SQLException) {
            return ConnectionUtils.isNetworkException((SQLException) e);
        }

        if (e instanceof CJException) {
            if (e.getCause() instanceof EOFException) { // Can not read response from server
                return true;
            }
            if (e.getCause() instanceof SSLException) { // Incomplete packets from server may cause SSL communication issues
                return true;
            }
            return ConnectionUtils.isNetworkException(((CJException) e).getSQLState());
        }

        return false;
    }
}