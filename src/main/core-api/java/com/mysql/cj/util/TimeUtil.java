/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.util;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.InvalidConnectionAttributeException;
import com.mysql.cj.exceptions.WrongArgumentException;

/**
 * Timezone conversion routines and other time related methods
 */
public class TimeUtil {
    static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT");

    // Mappings from TimeZone identifications (prefixed by type: Windows, TZ name, MetaZone, TZ alias, ...), to standard TimeZone Ids
    private static final String TIME_ZONE_MAPPINGS_RESOURCE = "/com/mysql/cj/util/TimeZoneMapping.properties";

    private static Properties timeZoneMappings = null;

    protected final static Method systemNanoTimeMethod;

    static {
        Method aMethod;

        try {
            aMethod = System.class.getMethod("nanoTime", (Class[]) null);
        } catch (SecurityException e) {
            aMethod = null;
        } catch (NoSuchMethodException e) {
            aMethod = null;
        }

        systemNanoTimeMethod = aMethod;
    }

    public static boolean nanoTimeAvailable() {
        return systemNanoTimeMethod != null;
    }

    public static long getCurrentTimeNanosOrMillis() {
        if (systemNanoTimeMethod != null) {
            try {
                return ((Long) systemNanoTimeMethod.invoke(null, (Object[]) null)).longValue();
            } catch (IllegalArgumentException e) {
                // ignore - fall through to currentTimeMillis()
            } catch (IllegalAccessException e) {
                // ignore - fall through to currentTimeMillis()
            } catch (InvocationTargetException e) {
                // ignore - fall through to currentTimeMillis()
            }
        }

        return System.currentTimeMillis();
    }

    /**
     * Returns the 'official' Java timezone name for the given timezone
     * 
     * @param timezoneStr
     *            the 'common' timezone name
     * @param exceptionInterceptor
     *            exception interceptor
     * 
     * @return the Java timezone name for the given timezone
     */
    public static String getCanonicalTimezone(String timezoneStr, ExceptionInterceptor exceptionInterceptor) {
        if (timezoneStr == null) {
            return null;
        }

        timezoneStr = timezoneStr.trim();

        // handle '+/-hh:mm' form ...
        if (timezoneStr.length() > 2) {
            if ((timezoneStr.charAt(0) == '+' || timezoneStr.charAt(0) == '-') && Character.isDigit(timezoneStr.charAt(1))) {
                return "GMT" + timezoneStr;
            }
        }

        synchronized (TimeUtil.class) {
            if (timeZoneMappings == null) {
                loadTimeZoneMappings(exceptionInterceptor);
            }
        }

        String canonicalTz;
        if ((canonicalTz = timeZoneMappings.getProperty(timezoneStr)) != null) {
            return canonicalTz;
        }

        throw ExceptionFactory.createException(InvalidConnectionAttributeException.class,
                Messages.getString("TimeUtil.UnrecognizedTimezoneId", new Object[] { timezoneStr }), exceptionInterceptor);
    }

    /**
     * Return a new Timestamp object which value is adjusted according to known DATE, DATETIME or TIMESTAMP field precision.
     * 
     * @param ts
     *            an original Timestamp object, not modified by this method
     * @param fsp
     *            value in the range from 0 to 6 specifying fractional seconds precision
     * @param serverRoundFracSecs
     *            Flag indicating whether rounding or truncation occurs on server when inserting a TIME, DATE, or TIMESTAMP value with a fractional seconds part
     *            into a column having the same type but fewer fractional digits: true means rounding, false means truncation. The proper value should be
     *            detected by analyzing sql_mode server variable for TIME_TRUNCATE_FRACTIONAL presence.
     * @return A new Timestamp object cloned from original ones and then rounded or truncated according to required fsp value
     */
    public static Timestamp adjustTimestampNanosPrecision(Timestamp ts, int fsp, boolean serverRoundFracSecs) {
        if (fsp < 0 || fsp > 6) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "fsp value must be in 0 to 6 range.");
        }

        Timestamp res = (Timestamp) ts.clone();
        int nanos = res.getNanos();
        double tail = Math.pow(10, 9 - fsp);

        if (serverRoundFracSecs) {
            nanos = (int) Math.round(nanos / tail) * (int) tail;
            if (nanos > 999999999) {
                nanos %= 1000000000; // get only last 9 digits
                res.setTime(res.getTime() + 1000); // increment seconds
            }
        } else {
            nanos = (int) (nanos / tail) * (int) tail;
        }
        res.setNanos(nanos);

        return res;
    }

    /**
     * Return a string representation of a fractional seconds part. This method assumes that all Timestamp adjustments are already done before,
     * thus no rounding is needed, only a proper "0" padding to be done.
     * 
     * @param nanos
     *            fractional seconds value
     * @param fsp
     *            required fractional part length
     * @return fractional seconds part as a string
     */
    public static String formatNanos(int nanos, int fsp) {
        return formatNanos(nanos, fsp, true);
    }

    /**
     * Return a string representation of a fractional seconds part. This method assumes that all Timestamp adjustments are already done before,
     * thus no rounding is needed, only a proper "0" padding to be done.
     * 
     * @param nanos
     *            fractional seconds value
     * @param fsp
     *            required fractional part length
     * @param truncateTrailingZeros
     *            whether to remove trailing zero characters in a fractional part after formatting
     * @return fractional seconds part as a string
     */
    public static String formatNanos(int nanos, int fsp, boolean truncateTrailingZeros) {
        if (nanos < 0 || nanos > 999999999) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "nanos value must be in 0 to 999999999 range but was " + nanos);
        }
        if (fsp < 0 || fsp > 6) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "fsp value must be in 0 to 6 range but was " + fsp);
        }

        if (fsp == 0 || nanos == 0) {
            return "0";
        }

        // just truncate because we expect the rounding was done before
        nanos = (int) (nanos / Math.pow(10, 9 - fsp));
        if (nanos == 0) {
            return "0";
        }

        String nanosString = Integer.toString(nanos);
        final String zeroPadding = "000000000";

        nanosString = zeroPadding.substring(0, fsp - nanosString.length()) + nanosString;

        if (truncateTrailingZeros) {
            int pos = fsp - 1; // the end, we're padded to the end by the code above
            while (nanosString.charAt(pos) == '0') {
                pos--;
            }
            nanosString = nanosString.substring(0, pos + 1);
        }
        return nanosString;
    }

    /**
     * Loads a properties file that contains all kinds of time zone mappings.
     * 
     * @param exceptionInterceptor
     *            exception interceptor
     */
    private static void loadTimeZoneMappings(ExceptionInterceptor exceptionInterceptor) {
        timeZoneMappings = new Properties();
        try {
            timeZoneMappings.load(TimeUtil.class.getResourceAsStream(TIME_ZONE_MAPPINGS_RESOURCE));
        } catch (IOException e) {
            throw ExceptionFactory.createException(Messages.getString("TimeUtil.LoadTimeZoneMappingError"), exceptionInterceptor);
        }
        // bridge all Time Zone ids known by Java
        for (String tz : TimeZone.getAvailableIDs()) {
            if (!timeZoneMappings.containsKey(tz)) {
                timeZoneMappings.put(tz, tz);
            }
        }
    }

    public static Timestamp truncateFractionalSeconds(Timestamp timestamp) {
        Timestamp truncatedTimestamp = new Timestamp(timestamp.getTime());
        truncatedTimestamp.setNanos(0);
        return truncatedTimestamp;
    }

    /**
     * Get SimpleDateFormat with a default Calendar which TimeZone is replaced with the provided one.
     * <p>
     * Note: The SimpleDateFormat object returned by this method contains a default Calendar with an altered TimeZone. It's safe to cache it between this method
     * calls because the Calendar object itself is not altered.
     * 
     * @param cachedSimpleDateFormat
     *            existing SimpleDateFormat to use instead of creating a new one
     * @param pattern
     *            format pattern
     * @param tz
     *            {@link TimeZone} object replacing the default one
     * @return {@link SimpleDateFormat} object
     */
    public static SimpleDateFormat getSimpleDateFormat(SimpleDateFormat cachedSimpleDateFormat, String pattern, TimeZone tz) {
        SimpleDateFormat sdf = cachedSimpleDateFormat != null ? cachedSimpleDateFormat : new SimpleDateFormat(pattern, Locale.US);
        if (tz != null) {
            sdf.setTimeZone(tz);
        }
        return sdf;
    }

    /**
     * Get SimpleDateFormat where a default Calendar is replaced with the provided one.
     * <p>
     * Note: Don't cache the SimpleDateFormat object returned by this method. Other methods could rely on assumption that the cached SimpleDateFormat has a
     * default Calendar and that it is safe to change only it's time zone (see {@link #getSimpleDateFormat(SimpleDateFormat, String, TimeZone)}.
     * 
     * @param pattern
     *            format pattern
     * @param cal
     *            {@link Calendar} object replacing the default one
     * @return {@link SimpleDateFormat} object
     */
    public static SimpleDateFormat getSimpleDateFormat(String pattern, Calendar cal) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern, Locale.US);
        if (cal != null) {
            sdf.setCalendar(cal);
        }
        return sdf;
    }

    /**
     * Used in prepared statements
     * 
     * @param dt
     *            DateTime string
     * @param toTime
     *            true if get Time pattern
     * @return pattern
     * @throws IOException
     *             if an error occurs
     */
    public static final String getDateTimePattern(String dt, boolean toTime) throws IOException {
        //
        // Special case
        //
        int dtLength = (dt != null) ? dt.length() : 0;

        if ((dtLength >= 8) && (dtLength <= 10)) {
            int dashCount = 0;
            boolean isDateOnly = true;

            for (int i = 0; i < dtLength; i++) {
                char c = dt.charAt(i);

                if (!Character.isDigit(c) && (c != '-')) {
                    isDateOnly = false;

                    break;
                }

                if (c == '-') {
                    dashCount++;
                }
            }

            if (isDateOnly && (dashCount == 2)) {
                return "yyyy-MM-dd";
            }
        }

        //
        // Special case - time-only
        //
        boolean colonsOnly = true;

        for (int i = 0; i < dtLength; i++) {
            char c = dt.charAt(i);

            if (!Character.isDigit(c) && (c != ':')) {
                colonsOnly = false;

                break;
            }
        }

        if (colonsOnly) {
            return "HH:mm:ss";
        }

        int n;
        int z;
        int count;
        int maxvecs;
        char c;
        char separator;
        StringReader reader = new StringReader(dt + " ");
        ArrayList<Object[]> vec = new ArrayList<>();
        ArrayList<Object[]> vecRemovelist = new ArrayList<>();
        Object[] nv = new Object[3];
        Object[] v;
        nv[0] = Character.valueOf('y');
        nv[1] = new StringBuilder();
        nv[2] = Integer.valueOf(0);
        vec.add(nv);

        if (toTime) {
            nv = new Object[3];
            nv[0] = Character.valueOf('h');
            nv[1] = new StringBuilder();
            nv[2] = Integer.valueOf(0);
            vec.add(nv);
        }

        while ((z = reader.read()) != -1) {
            separator = (char) z;
            maxvecs = vec.size();

            for (count = 0; count < maxvecs; count++) {
                v = vec.get(count);
                n = ((Integer) v[2]).intValue();
                c = getSuccessor(((Character) v[0]).charValue(), n);

                if (!Character.isLetterOrDigit(separator)) {
                    if ((c == ((Character) v[0]).charValue()) && (c != 'S')) {
                        vecRemovelist.add(v);
                    } else {
                        ((StringBuilder) v[1]).append(separator);

                        if ((c == 'X') || (c == 'Y')) {
                            v[2] = Integer.valueOf(4);
                        }
                    }
                } else {
                    if (c == 'X') {
                        c = 'y';
                        nv = new Object[3];
                        nv[1] = (new StringBuilder(((StringBuilder) v[1]).toString())).append('M');
                        nv[0] = Character.valueOf('M');
                        nv[2] = Integer.valueOf(1);
                        vec.add(nv);
                    } else if (c == 'Y') {
                        c = 'M';
                        nv = new Object[3];
                        nv[1] = (new StringBuilder(((StringBuilder) v[1]).toString())).append('d');
                        nv[0] = Character.valueOf('d');
                        nv[2] = Integer.valueOf(1);
                        vec.add(nv);
                    }

                    ((StringBuilder) v[1]).append(c);

                    if (c == ((Character) v[0]).charValue()) {
                        v[2] = Integer.valueOf(n + 1);
                    } else {
                        v[0] = Character.valueOf(c);
                        v[2] = Integer.valueOf(1);
                    }
                }
            }

            int size = vecRemovelist.size();

            for (int i = 0; i < size; i++) {
                v = vecRemovelist.get(i);
                vec.remove(v);
            }

            vecRemovelist.clear();
        }

        int size = vec.size();

        for (int i = 0; i < size; i++) {
            v = vec.get(i);
            c = ((Character) v[0]).charValue();
            n = ((Integer) v[2]).intValue();

            boolean bk = getSuccessor(c, n) != c;
            boolean atEnd = (((c == 's') || (c == 'm') || ((c == 'h') && toTime)) && bk);
            boolean finishesAtDate = (bk && (c == 'd') && !toTime);
            boolean containsEnd = (((StringBuilder) v[1]).toString().indexOf('W') != -1);

            if ((!atEnd && !finishesAtDate) || (containsEnd)) {
                vecRemovelist.add(v);
            }
        }

        size = vecRemovelist.size();

        for (int i = 0; i < size; i++) {
            vec.remove(vecRemovelist.get(i));
        }

        vecRemovelist.clear();
        v = vec.get(0); // might throw exception

        StringBuilder format = (StringBuilder) v[1];
        format.setLength(format.length() - 1);

        return format.toString();
    }

    private static final char getSuccessor(char c, int n) {
        return ((c == 'y') && (n == 2)) ? 'X'
                : (((c == 'y') && (n < 4)) ? 'y'
                        : ((c == 'y') ? 'M'
                                : (((c == 'M') && (n == 2)) ? 'Y'
                                        : (((c == 'M') && (n < 3)) ? 'M'
                                                : ((c == 'M') ? 'd'
                                                        : (((c == 'd') && (n < 2)) ? 'd'
                                                                : ((c == 'd') ? 'H'
                                                                        : (((c == 'H') && (n < 2)) ? 'H'
                                                                                : ((c == 'H') ? 'm'
                                                                                        : (((c == 'm') && (n < 2)) ? 'm'
                                                                                                : ((c == 'm') ? 's'
                                                                                                        : (((c == 's') && (n < 2)) ? 's' : 'W'))))))))))));
    }
}
