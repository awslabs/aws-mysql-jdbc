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

import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.mysql.cj.jdbc.ha.ConnectionUtils;

import javax.net.ssl.SSLException;
import java.io.EOFException;
import java.sql.SQLException;

public class ExceptionAnalyzer {
    public boolean isFailoverException(Throwable t) {
        if (!(t instanceof SQLException)) {
            return false;
        }

        SQLException sqlException = (SQLException) t;
        return MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN.equals(sqlException.getSQLState())
                || MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_CHANGED.equals(sqlException.getSQLState());
    }

    public boolean isCommunicationsException(Throwable e) {
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
