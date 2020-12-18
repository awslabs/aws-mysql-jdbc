/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.xdevapi;

import java.util.List;

import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.protocol.x.StatementExecuteOk;

/**
 * {@link SqlResult} for insert, update, delete and DDL statements.
 */
public class SqlUpdateResult extends UpdateResult implements SqlResult {

    /**
     * Constructor.
     * 
     * @param ok
     *            {@link StatementExecuteOk} instance.
     */
    public SqlUpdateResult(StatementExecuteOk ok) {
        super(ok);
    }

    @Override
    public boolean hasData() {
        return false;
    }

    @Override
    public boolean nextResult() {
        throw new FeatureNotAvailableException("Not a multi-result");
    }

    @Override
    public List<Row> fetchAll() {
        throw new FeatureNotAvailableException("No data");
    }

    @Override
    public Row next() {
        throw new FeatureNotAvailableException("No data");
    }

    @Override
    public boolean hasNext() {
        throw new FeatureNotAvailableException("No data");
    }

    @Override
    public int getColumnCount() {
        throw new FeatureNotAvailableException("No data");
    }

    @Override
    public List<Column> getColumns() {
        throw new FeatureNotAvailableException("No data");
    }

    @Override
    public List<String> getColumnNames() {
        throw new FeatureNotAvailableException("No data");
    }

    @Override
    public long count() {
        throw new FeatureNotAvailableException("No data");
    }

    @Override
    public Long getAutoIncrementValue() {
        return this.ok.getLastInsertId();
    }
}
