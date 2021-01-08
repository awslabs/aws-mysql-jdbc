/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.conf;

import java.util.Properties;

/**
 * Implement this interface, and pass the class name as the 'propertiesTransform' property in your URL, and the driver will pass the properties it has
 * parsed to your transform implementation so that you can modify/substitute/add any that you desire.
 */
public interface ConnectionPropertiesTransform {
    /**
     * The driver will call this method if the user has loaded your
     * implementation of this interface by specifying the 'propertiesTransform'
     * property in their URL.
     * 
     * @param props
     *            the properties as passed by the driver (never null)
     * 
     * @return the same properties with any transformations that your
     *         implementation has made
     * 
     */
    Properties transformProperties(Properties props);
}
