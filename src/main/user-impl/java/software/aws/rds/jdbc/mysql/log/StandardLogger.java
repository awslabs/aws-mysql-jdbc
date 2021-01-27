/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of this program hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of this connector, is also subject to the Universal FOSS Exception,
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

package software.aws.rds.jdbc.mysql.log;

/**
 * Provides logging facilities for those platforms that don't have built-in facilities. Simply logs messages to STDERR.
 */
public class StandardLogger extends com.mysql.cj.log.StandardLogger implements Log {

    /**
     * Creates a new StandardLogger object.
     *
     * @param name
     *            the name of the configuration to use -- ignored
     */
    public StandardLogger(String name) {
        super(name, false);
    }

    /**
     * @param name
     *            the name of the configuration to use -- ignored
     * @param logLocationInfo
     *            logLocationInfo
     */
    public StandardLogger(String name, boolean logLocationInfo) {
        super(name, logLocationInfo);
    }

}
