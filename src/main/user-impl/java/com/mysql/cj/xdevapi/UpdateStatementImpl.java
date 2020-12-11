/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.mysql.cj.MysqlxSession;
import com.mysql.cj.protocol.x.XMessage;
import com.mysql.cj.protocol.x.XMessageBuilder;

/**
 * {@link UpdateStatement} implementation.
 */
public class UpdateStatementImpl extends FilterableStatement<UpdateStatement, Result> implements UpdateStatement {
    private UpdateParams updateParams = new UpdateParams();

    /* package private */ UpdateStatementImpl(MysqlxSession mysqlxSession, String schema, String table) {
        super(new TableFilterParams(schema, table, false));
        this.mysqlxSession = mysqlxSession;
    }

    @Override
    protected Result executeStatement() {
        return this.mysqlxSession.query(getMessageBuilder().buildRowUpdate(this.filterParams, this.updateParams), new UpdateResultBuilder<>());
    }

    @Override
    protected XMessage getPrepareStatementXMessage() {
        return getMessageBuilder().buildPrepareRowUpdate(this.preparedStatementId, this.filterParams, this.updateParams);
    }

    @Override
    protected Result executePreparedStatement() {
        return this.mysqlxSession.query(getMessageBuilder().buildPrepareExecute(this.preparedStatementId, this.filterParams), new UpdateResultBuilder<>());
    }

    public CompletableFuture<Result> executeAsync() {
        return this.mysqlxSession.queryAsync(
                ((XMessageBuilder) this.mysqlxSession.<XMessage>getMessageBuilder()).buildRowUpdate(this.filterParams, this.updateParams),
                new UpdateResultBuilder<>());
    }

    public UpdateStatement set(Map<String, Object> fieldsAndValues) {
        resetPrepareState();
        this.updateParams.setUpdates(fieldsAndValues);
        return this;
    }

    public UpdateStatement set(String field, Object value) {
        resetPrepareState();
        this.updateParams.addUpdate(field, value);
        return this;
    }
}
