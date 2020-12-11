/*
 * Copyright (c) 2017, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj;

import java.util.LinkedList;

import com.mysql.cj.util.StringUtils;

public class AppendingBatchVisitor implements BatchVisitor {
    LinkedList<byte[]> statementComponents = new LinkedList<>();

    public BatchVisitor append(byte[] values) {
        this.statementComponents.addLast(values);

        return this;
    }

    public BatchVisitor increment() {
        // no-op
        return this;
    }

    public BatchVisitor decrement() {
        this.statementComponents.removeLast();

        return this;
    }

    public BatchVisitor merge(byte[] front, byte[] back) {
        int mergedLength = front.length + back.length;
        byte[] merged = new byte[mergedLength];
        System.arraycopy(front, 0, merged, 0, front.length);
        System.arraycopy(back, 0, merged, front.length, back.length);
        this.statementComponents.addLast(merged);
        return this;
    }

    @Override
    public BatchVisitor mergeWithLast(byte[] values) {
        if (this.statementComponents.isEmpty()) {
            return append(values);
        }
        return merge(this.statementComponents.removeLast(), values);
    }

    public byte[][] getStaticSqlStrings() {
        byte[][] asBytes = new byte[this.statementComponents.size()][];
        this.statementComponents.toArray(asBytes);

        return asBytes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (byte[] comp : this.statementComponents) {
            sb.append(StringUtils.toString(comp));
        }
        return sb.toString();
    }
}
