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

package com.mysql.cj;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Mapping between MySQL charset names and Java charset names. I've investigated placing these in a .properties file, but unfortunately under most appservers
 * this complicates configuration because the security policy needs to be changed by the user to allow the driver to read them :(
 */
public class CharsetMapping {

    public static final int MAP_SIZE = 2048; // Size of static maps
    public static final String[] COLLATION_INDEX_TO_COLLATION_NAME;
    public static final MysqlCharset[] COLLATION_INDEX_TO_CHARSET;

    public static final Map<String, MysqlCharset> CHARSET_NAME_TO_CHARSET;
    public static final Map<String, Integer> CHARSET_NAME_TO_COLLATION_INDEX;

    private static final Map<String, List<MysqlCharset>> JAVA_ENCODING_UC_TO_MYSQL_CHARSET;

    private static final Set<String> MULTIBYTE_ENCODINGS;

    public static final Set<Integer> UTF8MB4_INDEXES;

    private static final String MYSQL_CHARSET_NAME_armscii8 = "armscii8";
    private static final String MYSQL_CHARSET_NAME_ascii = "ascii";
    private static final String MYSQL_CHARSET_NAME_big5 = "big5";
    private static final String MYSQL_CHARSET_NAME_binary = "binary";
    private static final String MYSQL_CHARSET_NAME_cp1250 = "cp1250";
    private static final String MYSQL_CHARSET_NAME_cp1251 = "cp1251";
    private static final String MYSQL_CHARSET_NAME_cp1256 = "cp1256";
    private static final String MYSQL_CHARSET_NAME_cp1257 = "cp1257";
    private static final String MYSQL_CHARSET_NAME_cp850 = "cp850";
    private static final String MYSQL_CHARSET_NAME_cp852 = "cp852";
    private static final String MYSQL_CHARSET_NAME_cp866 = "cp866";
    private static final String MYSQL_CHARSET_NAME_cp932 = "cp932";
    private static final String MYSQL_CHARSET_NAME_dec8 = "dec8";
    private static final String MYSQL_CHARSET_NAME_eucjpms = "eucjpms";
    private static final String MYSQL_CHARSET_NAME_euckr = "euckr";
    private static final String MYSQL_CHARSET_NAME_gb18030 = "gb18030";
    private static final String MYSQL_CHARSET_NAME_gb2312 = "gb2312";
    private static final String MYSQL_CHARSET_NAME_gbk = "gbk";
    private static final String MYSQL_CHARSET_NAME_geostd8 = "geostd8";
    private static final String MYSQL_CHARSET_NAME_greek = "greek";
    private static final String MYSQL_CHARSET_NAME_hebrew = "hebrew";
    private static final String MYSQL_CHARSET_NAME_hp8 = "hp8";
    private static final String MYSQL_CHARSET_NAME_keybcs2 = "keybcs2";
    private static final String MYSQL_CHARSET_NAME_koi8r = "koi8r";
    private static final String MYSQL_CHARSET_NAME_koi8u = "koi8u";
    private static final String MYSQL_CHARSET_NAME_latin1 = "latin1";
    private static final String MYSQL_CHARSET_NAME_latin2 = "latin2";
    private static final String MYSQL_CHARSET_NAME_latin5 = "latin5";
    private static final String MYSQL_CHARSET_NAME_latin7 = "latin7";
    private static final String MYSQL_CHARSET_NAME_macce = "macce";
    private static final String MYSQL_CHARSET_NAME_macroman = "macroman";
    private static final String MYSQL_CHARSET_NAME_sjis = "sjis";
    private static final String MYSQL_CHARSET_NAME_swe7 = "swe7";
    private static final String MYSQL_CHARSET_NAME_tis620 = "tis620";
    private static final String MYSQL_CHARSET_NAME_ucs2 = "ucs2";
    private static final String MYSQL_CHARSET_NAME_ujis = "ujis";
    private static final String MYSQL_CHARSET_NAME_utf16 = "utf16";
    private static final String MYSQL_CHARSET_NAME_utf16le = "utf16le";
    private static final String MYSQL_CHARSET_NAME_utf32 = "utf32";
    private static final String MYSQL_CHARSET_NAME_utf8 = "utf8";
    private static final String MYSQL_CHARSET_NAME_utf8mb4 = "utf8mb4";

    public static final String NOT_USED = MYSQL_CHARSET_NAME_latin1; // punting for not-used character sets
    public static final String COLLATION_NOT_DEFINED = "none";

    public static final int MYSQL_COLLATION_INDEX_utf8 = 33;
    public static final int MYSQL_COLLATION_INDEX_binary = 63;

    private static int numberOfEncodingsConfigured = 0;

    static {
        // complete list of mysql character sets and their corresponding java encoding names
        MysqlCharset[] charset = new MysqlCharset[] { new MysqlCharset(MYSQL_CHARSET_NAME_ascii, 1, 0, new String[] { "US-ASCII", "ASCII" }),

                new MysqlCharset(MYSQL_CHARSET_NAME_big5, 2, 0, new String[] { "Big5" }),
                new MysqlCharset(MYSQL_CHARSET_NAME_gbk, 2, 0, new String[] { "GBK" }),

                new MysqlCharset(MYSQL_CHARSET_NAME_sjis, 2, 0, new String[] { "SHIFT_JIS", "Cp943", "WINDOWS-31J" }),	// SJIS is alias for SHIFT_JIS, Cp943 is rather a cp932 but we map it to sjis for years
                new MysqlCharset(MYSQL_CHARSET_NAME_cp932, 2, 1, new String[] { "WINDOWS-31J" }),		// MS932 is alias for WINDOWS-31J

                new MysqlCharset(MYSQL_CHARSET_NAME_gb2312, 2, 0, new String[] { "GB2312" }),
                new MysqlCharset(MYSQL_CHARSET_NAME_ujis, 3, 0, new String[] { "EUC_JP" }),
                new MysqlCharset(MYSQL_CHARSET_NAME_eucjpms, 3, 0, new String[] { "EUC_JP_Solaris" }, new ServerVersion(5, 0, 3)),	// "EUC_JP_Solaris = 	>5.0.3 eucjpms,"

                new MysqlCharset(MYSQL_CHARSET_NAME_gb18030, 4, 0, new String[] { "GB18030" }, new ServerVersion(5, 7, 4)),

                new MysqlCharset(MYSQL_CHARSET_NAME_euckr, 2, 0, new String[] { "EUC-KR" }),

                new MysqlCharset(MYSQL_CHARSET_NAME_latin1, 1, 1, new String[] { "Cp1252", "ISO8859_1" }),
                new MysqlCharset(MYSQL_CHARSET_NAME_swe7, 1, 0, new String[] { "Cp1252" }),			// new mapping, Cp1252 ?
                new MysqlCharset(MYSQL_CHARSET_NAME_hp8, 1, 0, new String[] { "Cp1252" }),			// new mapping, Cp1252 ?
                new MysqlCharset(MYSQL_CHARSET_NAME_dec8, 1, 0, new String[] { "Cp1252" }),			// new mapping, Cp1252 ?
                new MysqlCharset(MYSQL_CHARSET_NAME_armscii8, 1, 0, new String[] { "Cp1252" }),			// new mapping, Cp1252 ?
                new MysqlCharset(MYSQL_CHARSET_NAME_geostd8, 1, 0, new String[] { "Cp1252" }),			// new mapping, Cp1252 ?

                new MysqlCharset(MYSQL_CHARSET_NAME_latin2, 1, 0, new String[] { "ISO8859_2" }),		// latin2 is an alias

                new MysqlCharset(MYSQL_CHARSET_NAME_greek, 1, 0, new String[] { "ISO8859_7", "greek" }),
                new MysqlCharset(MYSQL_CHARSET_NAME_latin7, 1, 0, new String[] { "ISO-8859-13" }),	// was ISO8859_7, that's incorrect; also + "LATIN7 =		latin7," is wrong java encoding name

                new MysqlCharset(MYSQL_CHARSET_NAME_hebrew, 1, 0, new String[] { "ISO8859_8" }),		// hebrew is an alias
                new MysqlCharset(MYSQL_CHARSET_NAME_latin5, 1, 0, new String[] { "ISO8859_9" }),		// LATIN5 is an alias

                new MysqlCharset(MYSQL_CHARSET_NAME_cp850, 1, 0, new String[] { "Cp850", "Cp437" }),

                new MysqlCharset(MYSQL_CHARSET_NAME_cp852, 1, 0, new String[] { "Cp852" }),
                new MysqlCharset(MYSQL_CHARSET_NAME_keybcs2, 1, 0, new String[] { "Cp852" }),	// new, Kamenicky encoding usually known as Cp895 but there is no official cp895 specification; close to Cp852, see http://ftp.muni.cz/pub/localization/charsets/cs-encodings-faq

                new MysqlCharset(MYSQL_CHARSET_NAME_cp866, 1, 0, new String[] { "Cp866" }),

                new MysqlCharset(MYSQL_CHARSET_NAME_koi8r, 1, 1, new String[] { "KOI8_R" }),
                new MysqlCharset(MYSQL_CHARSET_NAME_koi8u, 1, 0, new String[] { "KOI8_R" }),

                new MysqlCharset(MYSQL_CHARSET_NAME_tis620, 1, 0, new String[] { "TIS620" }),

                new MysqlCharset(MYSQL_CHARSET_NAME_cp1250, 1, 0, new String[] { "Cp1250" }),

                new MysqlCharset(MYSQL_CHARSET_NAME_cp1251, 1, 1, new String[] { "Cp1251" }),

                new MysqlCharset(MYSQL_CHARSET_NAME_cp1256, 1, 0, new String[] { "Cp1256" }),
                new MysqlCharset(MYSQL_CHARSET_NAME_cp1257, 1, 0, new String[] { "Cp1257" }),
                new MysqlCharset(MYSQL_CHARSET_NAME_macroman, 1, 0, new String[] { "MacRoman" }),
                new MysqlCharset(MYSQL_CHARSET_NAME_macce, 1, 0, new String[] { "MacCentralEurope" }),

                new MysqlCharset(MYSQL_CHARSET_NAME_utf8, 3, 1, new String[] { "UTF-8" }),
                new MysqlCharset(MYSQL_CHARSET_NAME_utf8mb4, 4, 0, new String[] { "UTF-8" }),			// "UTF-8 =				*> 5.5.2 utf8mb4,"

                new MysqlCharset(MYSQL_CHARSET_NAME_ucs2, 2, 0, new String[] { "UnicodeBig" }),

                new MysqlCharset(MYSQL_CHARSET_NAME_binary, 1, 1, new String[] { "ISO8859_1" }),	// US-ASCII ?

                new MysqlCharset(MYSQL_CHARSET_NAME_utf16, 4, 0, new String[] { "UTF-16" }),
                new MysqlCharset(MYSQL_CHARSET_NAME_utf16le, 4, 0, new String[] { "UTF-16LE" }),
                new MysqlCharset(MYSQL_CHARSET_NAME_utf32, 4, 0, new String[] { "UTF-32" })

        };
        HashMap<String, MysqlCharset> charsetNameToMysqlCharsetMap = new HashMap<>();
        HashMap<String, List<MysqlCharset>> javaUcToMysqlCharsetMap = new HashMap<>();
        Set<String> tempMultibyteEncodings = new HashSet<>(); // Character sets that we can't convert ourselves.
        for (int i = 0; i < charset.length; i++) {
            String charsetName = charset[i].charsetName;

            charsetNameToMysqlCharsetMap.put(charsetName, charset[i]);

            numberOfEncodingsConfigured += charset[i].javaEncodingsUc.size();

            for (String encUC : charset[i].javaEncodingsUc) {

                // fill javaUcToMysqlCharsetMap
                List<MysqlCharset> charsets = javaUcToMysqlCharsetMap.get(encUC);
                if (charsets == null) {
                    charsets = new ArrayList<>();
                    javaUcToMysqlCharsetMap.put(encUC, charsets);
                }
                charsets.add(charset[i]);

                // fill multi-byte charsets
                if (charset[i].mblen > 1) {
                    tempMultibyteEncodings.add(encUC);
                }

            }

        }
        CHARSET_NAME_TO_CHARSET = Collections.unmodifiableMap(charsetNameToMysqlCharsetMap);
        JAVA_ENCODING_UC_TO_MYSQL_CHARSET = Collections.unmodifiableMap(javaUcToMysqlCharsetMap);
        MULTIBYTE_ENCODINGS = Collections.unmodifiableSet(tempMultibyteEncodings);

        // complete list of mysql collations and their corresponding character sets each element of collation[1]..collation[MAP_SIZE-1] must not be null
        Collation[] collation = new Collation[MAP_SIZE];
        collation[1] = new Collation(1, "big5_chinese_ci", 1, MYSQL_CHARSET_NAME_big5);
        collation[2] = new Collation(2, "latin2_czech_cs", 0, MYSQL_CHARSET_NAME_latin2);
        collation[3] = new Collation(3, "dec8_swedish_ci", 0, MYSQL_CHARSET_NAME_dec8);
        collation[4] = new Collation(4, "cp850_general_ci", 1, MYSQL_CHARSET_NAME_cp850);
        collation[5] = new Collation(5, "latin1_german1_ci", 0, MYSQL_CHARSET_NAME_latin1);
        collation[6] = new Collation(6, "hp8_english_ci", 0, MYSQL_CHARSET_NAME_hp8);
        collation[7] = new Collation(7, "koi8r_general_ci", 0, MYSQL_CHARSET_NAME_koi8r);
        collation[8] = new Collation(8, "latin1_swedish_ci", 1, MYSQL_CHARSET_NAME_latin1);
        collation[9] = new Collation(9, "latin2_general_ci", 1, MYSQL_CHARSET_NAME_latin2);
        collation[10] = new Collation(10, "swe7_swedish_ci", 0, MYSQL_CHARSET_NAME_swe7);
        collation[11] = new Collation(11, "ascii_general_ci", 0, MYSQL_CHARSET_NAME_ascii);
        collation[12] = new Collation(12, "ujis_japanese_ci", 0, MYSQL_CHARSET_NAME_ujis);
        collation[13] = new Collation(13, "sjis_japanese_ci", 0, MYSQL_CHARSET_NAME_sjis);
        collation[14] = new Collation(14, "cp1251_bulgarian_ci", 0, MYSQL_CHARSET_NAME_cp1251);
        collation[15] = new Collation(15, "latin1_danish_ci", 0, MYSQL_CHARSET_NAME_latin1);
        collation[16] = new Collation(16, "hebrew_general_ci", 0, MYSQL_CHARSET_NAME_hebrew);

        collation[18] = new Collation(18, "tis620_thai_ci", 0, MYSQL_CHARSET_NAME_tis620);
        collation[19] = new Collation(19, "euckr_korean_ci", 0, MYSQL_CHARSET_NAME_euckr);
        collation[20] = new Collation(20, "latin7_estonian_cs", 0, MYSQL_CHARSET_NAME_latin7);
        collation[21] = new Collation(21, "latin2_hungarian_ci", 0, MYSQL_CHARSET_NAME_latin2);
        collation[22] = new Collation(22, "koi8u_general_ci", 0, MYSQL_CHARSET_NAME_koi8u);
        collation[23] = new Collation(23, "cp1251_ukrainian_ci", 0, MYSQL_CHARSET_NAME_cp1251);
        collation[24] = new Collation(24, "gb2312_chinese_ci", 0, MYSQL_CHARSET_NAME_gb2312);
        collation[25] = new Collation(25, "greek_general_ci", 0, MYSQL_CHARSET_NAME_greek);
        collation[26] = new Collation(26, "cp1250_general_ci", 1, MYSQL_CHARSET_NAME_cp1250);
        collation[27] = new Collation(27, "latin2_croatian_ci", 0, MYSQL_CHARSET_NAME_latin2);
        collation[28] = new Collation(28, "gbk_chinese_ci", 1, MYSQL_CHARSET_NAME_gbk);
        collation[29] = new Collation(29, "cp1257_lithuanian_ci", 0, MYSQL_CHARSET_NAME_cp1257);
        collation[30] = new Collation(30, "latin5_turkish_ci", 1, MYSQL_CHARSET_NAME_latin5);
        collation[31] = new Collation(31, "latin1_german2_ci", 0, MYSQL_CHARSET_NAME_latin1);
        collation[32] = new Collation(32, "armscii8_general_ci", 0, MYSQL_CHARSET_NAME_armscii8);
        collation[33] = new Collation(33, "utf8_general_ci", 1, MYSQL_CHARSET_NAME_utf8);
        collation[34] = new Collation(34, "cp1250_czech_cs", 0, MYSQL_CHARSET_NAME_cp1250);
        collation[35] = new Collation(35, "ucs2_general_ci", 1, MYSQL_CHARSET_NAME_ucs2);
        collation[36] = new Collation(36, "cp866_general_ci", 1, MYSQL_CHARSET_NAME_cp866);
        collation[37] = new Collation(37, "keybcs2_general_ci", 1, MYSQL_CHARSET_NAME_keybcs2);
        collation[38] = new Collation(38, "macce_general_ci", 1, MYSQL_CHARSET_NAME_macce);
        collation[39] = new Collation(39, "macroman_general_ci", 1, MYSQL_CHARSET_NAME_macroman);
        collation[40] = new Collation(40, "cp852_general_ci", 1, MYSQL_CHARSET_NAME_cp852);
        collation[41] = new Collation(41, "latin7_general_ci", 1, MYSQL_CHARSET_NAME_latin7);
        collation[42] = new Collation(42, "latin7_general_cs", 0, MYSQL_CHARSET_NAME_latin7);
        collation[43] = new Collation(43, "macce_bin", 0, MYSQL_CHARSET_NAME_macce);
        collation[44] = new Collation(44, "cp1250_croatian_ci", 0, MYSQL_CHARSET_NAME_cp1250);
        collation[45] = new Collation(45, "utf8mb4_general_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[46] = new Collation(46, "utf8mb4_bin", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[47] = new Collation(47, "latin1_bin", 0, MYSQL_CHARSET_NAME_latin1);
        collation[48] = new Collation(48, "latin1_general_ci", 0, MYSQL_CHARSET_NAME_latin1);
        collation[49] = new Collation(49, "latin1_general_cs", 0, MYSQL_CHARSET_NAME_latin1);
        collation[50] = new Collation(50, "cp1251_bin", 0, MYSQL_CHARSET_NAME_cp1251);
        collation[51] = new Collation(51, "cp1251_general_ci", 1, MYSQL_CHARSET_NAME_cp1251);
        collation[52] = new Collation(52, "cp1251_general_cs", 0, MYSQL_CHARSET_NAME_cp1251);
        collation[53] = new Collation(53, "macroman_bin", 0, MYSQL_CHARSET_NAME_macroman);
        collation[54] = new Collation(54, "utf16_general_ci", 1, MYSQL_CHARSET_NAME_utf16);
        collation[55] = new Collation(55, "utf16_bin", 0, MYSQL_CHARSET_NAME_utf16);
        collation[56] = new Collation(56, "utf16le_general_ci", 1, MYSQL_CHARSET_NAME_utf16le);
        collation[57] = new Collation(57, "cp1256_general_ci", 1, MYSQL_CHARSET_NAME_cp1256);
        collation[58] = new Collation(58, "cp1257_bin", 0, MYSQL_CHARSET_NAME_cp1257);
        collation[59] = new Collation(59, "cp1257_general_ci", 1, MYSQL_CHARSET_NAME_cp1257);
        collation[60] = new Collation(60, "utf32_general_ci", 1, MYSQL_CHARSET_NAME_utf32);
        collation[61] = new Collation(61, "utf32_bin", 0, MYSQL_CHARSET_NAME_utf32);
        collation[62] = new Collation(62, "utf16le_bin", 0, MYSQL_CHARSET_NAME_utf16le);
        collation[63] = new Collation(63, "binary", 1, MYSQL_CHARSET_NAME_binary);
        collation[64] = new Collation(64, "armscii8_bin", 0, MYSQL_CHARSET_NAME_armscii8);
        collation[65] = new Collation(65, "ascii_bin", 0, MYSQL_CHARSET_NAME_ascii);
        collation[66] = new Collation(66, "cp1250_bin", 0, MYSQL_CHARSET_NAME_cp1250);
        collation[67] = new Collation(67, "cp1256_bin", 0, MYSQL_CHARSET_NAME_cp1256);
        collation[68] = new Collation(68, "cp866_bin", 0, MYSQL_CHARSET_NAME_cp866);
        collation[69] = new Collation(69, "dec8_bin", 0, MYSQL_CHARSET_NAME_dec8);
        collation[70] = new Collation(70, "greek_bin", 0, MYSQL_CHARSET_NAME_greek);
        collation[71] = new Collation(71, "hebrew_bin", 0, MYSQL_CHARSET_NAME_hebrew);
        collation[72] = new Collation(72, "hp8_bin", 0, MYSQL_CHARSET_NAME_hp8);
        collation[73] = new Collation(73, "keybcs2_bin", 0, MYSQL_CHARSET_NAME_keybcs2);
        collation[74] = new Collation(74, "koi8r_bin", 0, MYSQL_CHARSET_NAME_koi8r);
        collation[75] = new Collation(75, "koi8u_bin", 0, MYSQL_CHARSET_NAME_koi8u);
        collation[76] = new Collation(76, "utf8_tolower_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[77] = new Collation(77, "latin2_bin", 0, MYSQL_CHARSET_NAME_latin2);
        collation[78] = new Collation(78, "latin5_bin", 0, MYSQL_CHARSET_NAME_latin5);
        collation[79] = new Collation(79, "latin7_bin", 0, MYSQL_CHARSET_NAME_latin7);
        collation[80] = new Collation(80, "cp850_bin", 0, MYSQL_CHARSET_NAME_cp850);
        collation[81] = new Collation(81, "cp852_bin", 0, MYSQL_CHARSET_NAME_cp852);
        collation[82] = new Collation(82, "swe7_bin", 0, MYSQL_CHARSET_NAME_swe7);
        collation[83] = new Collation(83, "utf8_bin", 0, MYSQL_CHARSET_NAME_utf8);
        collation[84] = new Collation(84, "big5_bin", 0, MYSQL_CHARSET_NAME_big5);
        collation[85] = new Collation(85, "euckr_bin", 0, MYSQL_CHARSET_NAME_euckr);
        collation[86] = new Collation(86, "gb2312_bin", 0, MYSQL_CHARSET_NAME_gb2312);
        collation[87] = new Collation(87, "gbk_bin", 0, MYSQL_CHARSET_NAME_gbk);
        collation[88] = new Collation(88, "sjis_bin", 0, MYSQL_CHARSET_NAME_sjis);
        collation[89] = new Collation(89, "tis620_bin", 0, MYSQL_CHARSET_NAME_tis620);
        collation[90] = new Collation(90, "ucs2_bin", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[91] = new Collation(91, "ujis_bin", 0, MYSQL_CHARSET_NAME_ujis);
        collation[92] = new Collation(92, "geostd8_general_ci", 0, MYSQL_CHARSET_NAME_geostd8);
        collation[93] = new Collation(93, "geostd8_bin", 0, MYSQL_CHARSET_NAME_geostd8);
        collation[94] = new Collation(94, "latin1_spanish_ci", 0, MYSQL_CHARSET_NAME_latin1);
        collation[95] = new Collation(95, "cp932_japanese_ci", 1, MYSQL_CHARSET_NAME_cp932);
        collation[96] = new Collation(96, "cp932_bin", 0, MYSQL_CHARSET_NAME_cp932);
        collation[97] = new Collation(97, "eucjpms_japanese_ci", 1, MYSQL_CHARSET_NAME_eucjpms);
        collation[98] = new Collation(98, "eucjpms_bin", 0, MYSQL_CHARSET_NAME_eucjpms);
        collation[99] = new Collation(99, "cp1250_polish_ci", 0, MYSQL_CHARSET_NAME_cp1250);

        collation[101] = new Collation(101, "utf16_unicode_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[102] = new Collation(102, "utf16_icelandic_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[103] = new Collation(103, "utf16_latvian_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[104] = new Collation(104, "utf16_romanian_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[105] = new Collation(105, "utf16_slovenian_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[106] = new Collation(106, "utf16_polish_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[107] = new Collation(107, "utf16_estonian_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[108] = new Collation(108, "utf16_spanish_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[109] = new Collation(109, "utf16_swedish_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[110] = new Collation(110, "utf16_turkish_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[111] = new Collation(111, "utf16_czech_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[112] = new Collation(112, "utf16_danish_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[113] = new Collation(113, "utf16_lithuanian_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[114] = new Collation(114, "utf16_slovak_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[115] = new Collation(115, "utf16_spanish2_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[116] = new Collation(116, "utf16_roman_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[117] = new Collation(117, "utf16_persian_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[118] = new Collation(118, "utf16_esperanto_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[119] = new Collation(119, "utf16_hungarian_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[120] = new Collation(120, "utf16_sinhala_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[121] = new Collation(121, "utf16_german2_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[122] = new Collation(122, "utf16_croatian_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[123] = new Collation(123, "utf16_unicode_520_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[124] = new Collation(124, "utf16_vietnamese_ci", 0, MYSQL_CHARSET_NAME_utf16);

        collation[128] = new Collation(128, "ucs2_unicode_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[129] = new Collation(129, "ucs2_icelandic_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[130] = new Collation(130, "ucs2_latvian_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[131] = new Collation(131, "ucs2_romanian_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[132] = new Collation(132, "ucs2_slovenian_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[133] = new Collation(133, "ucs2_polish_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[134] = new Collation(134, "ucs2_estonian_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[135] = new Collation(135, "ucs2_spanish_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[136] = new Collation(136, "ucs2_swedish_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[137] = new Collation(137, "ucs2_turkish_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[138] = new Collation(138, "ucs2_czech_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[139] = new Collation(139, "ucs2_danish_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[140] = new Collation(140, "ucs2_lithuanian_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[141] = new Collation(141, "ucs2_slovak_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[142] = new Collation(142, "ucs2_spanish2_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[143] = new Collation(143, "ucs2_roman_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[144] = new Collation(144, "ucs2_persian_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[145] = new Collation(145, "ucs2_esperanto_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[146] = new Collation(146, "ucs2_hungarian_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[147] = new Collation(147, "ucs2_sinhala_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[148] = new Collation(148, "ucs2_german2_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[149] = new Collation(149, "ucs2_croatian_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[150] = new Collation(150, "ucs2_unicode_520_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[151] = new Collation(151, "ucs2_vietnamese_ci", 0, MYSQL_CHARSET_NAME_ucs2);

        collation[159] = new Collation(159, "ucs2_general_mysql500_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[160] = new Collation(160, "utf32_unicode_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[161] = new Collation(161, "utf32_icelandic_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[162] = new Collation(162, "utf32_latvian_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[163] = new Collation(163, "utf32_romanian_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[164] = new Collation(164, "utf32_slovenian_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[165] = new Collation(165, "utf32_polish_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[166] = new Collation(166, "utf32_estonian_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[167] = new Collation(167, "utf32_spanish_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[168] = new Collation(168, "utf32_swedish_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[169] = new Collation(169, "utf32_turkish_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[170] = new Collation(170, "utf32_czech_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[171] = new Collation(171, "utf32_danish_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[172] = new Collation(172, "utf32_lithuanian_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[173] = new Collation(173, "utf32_slovak_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[174] = new Collation(174, "utf32_spanish2_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[175] = new Collation(175, "utf32_roman_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[176] = new Collation(176, "utf32_persian_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[177] = new Collation(177, "utf32_esperanto_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[178] = new Collation(178, "utf32_hungarian_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[179] = new Collation(179, "utf32_sinhala_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[180] = new Collation(180, "utf32_german2_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[181] = new Collation(181, "utf32_croatian_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[182] = new Collation(182, "utf32_unicode_520_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[183] = new Collation(183, "utf32_vietnamese_ci", 0, MYSQL_CHARSET_NAME_utf32);

        collation[192] = new Collation(192, "utf8_unicode_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[193] = new Collation(193, "utf8_icelandic_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[194] = new Collation(194, "utf8_latvian_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[195] = new Collation(195, "utf8_romanian_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[196] = new Collation(196, "utf8_slovenian_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[197] = new Collation(197, "utf8_polish_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[198] = new Collation(198, "utf8_estonian_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[199] = new Collation(199, "utf8_spanish_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[200] = new Collation(200, "utf8_swedish_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[201] = new Collation(201, "utf8_turkish_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[202] = new Collation(202, "utf8_czech_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[203] = new Collation(203, "utf8_danish_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[204] = new Collation(204, "utf8_lithuanian_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[205] = new Collation(205, "utf8_slovak_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[206] = new Collation(206, "utf8_spanish2_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[207] = new Collation(207, "utf8_roman_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[208] = new Collation(208, "utf8_persian_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[209] = new Collation(209, "utf8_esperanto_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[210] = new Collation(210, "utf8_hungarian_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[211] = new Collation(211, "utf8_sinhala_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[212] = new Collation(212, "utf8_german2_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[213] = new Collation(213, "utf8_croatian_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[214] = new Collation(214, "utf8_unicode_520_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[215] = new Collation(215, "utf8_vietnamese_ci", 0, MYSQL_CHARSET_NAME_utf8);

        collation[223] = new Collation(223, "utf8_general_mysql500_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[224] = new Collation(224, "utf8mb4_unicode_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[225] = new Collation(225, "utf8mb4_icelandic_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[226] = new Collation(226, "utf8mb4_latvian_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[227] = new Collation(227, "utf8mb4_romanian_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[228] = new Collation(228, "utf8mb4_slovenian_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[229] = new Collation(229, "utf8mb4_polish_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[230] = new Collation(230, "utf8mb4_estonian_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[231] = new Collation(231, "utf8mb4_spanish_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[232] = new Collation(232, "utf8mb4_swedish_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[233] = new Collation(233, "utf8mb4_turkish_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[234] = new Collation(234, "utf8mb4_czech_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[235] = new Collation(235, "utf8mb4_danish_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[236] = new Collation(236, "utf8mb4_lithuanian_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[237] = new Collation(237, "utf8mb4_slovak_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[238] = new Collation(238, "utf8mb4_spanish2_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[239] = new Collation(239, "utf8mb4_roman_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[240] = new Collation(240, "utf8mb4_persian_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[241] = new Collation(241, "utf8mb4_esperanto_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[242] = new Collation(242, "utf8mb4_hungarian_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[243] = new Collation(243, "utf8mb4_sinhala_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[244] = new Collation(244, "utf8mb4_german2_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[245] = new Collation(245, "utf8mb4_croatian_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[246] = new Collation(246, "utf8mb4_unicode_520_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[247] = new Collation(247, "utf8mb4_vietnamese_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[248] = new Collation(248, "gb18030_chinese_ci", 1, MYSQL_CHARSET_NAME_gb18030);
        collation[249] = new Collation(249, "gb18030_bin", 0, MYSQL_CHARSET_NAME_gb18030);
        collation[250] = new Collation(250, "gb18030_unicode_520_ci", 0, MYSQL_CHARSET_NAME_gb18030);

        collation[255] = new Collation(255, "utf8mb4_0900_ai_ci", 1, MYSQL_CHARSET_NAME_utf8mb4);
        collation[256] = new Collation(256, "utf8mb4_de_pb_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[257] = new Collation(257, "utf8mb4_is_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[258] = new Collation(258, "utf8mb4_lv_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[259] = new Collation(259, "utf8mb4_ro_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[260] = new Collation(260, "utf8mb4_sl_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[261] = new Collation(261, "utf8mb4_pl_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[262] = new Collation(262, "utf8mb4_et_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[263] = new Collation(263, "utf8mb4_es_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[264] = new Collation(264, "utf8mb4_sv_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[265] = new Collation(265, "utf8mb4_tr_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[266] = new Collation(266, "utf8mb4_cs_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[267] = new Collation(267, "utf8mb4_da_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[268] = new Collation(268, "utf8mb4_lt_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[269] = new Collation(269, "utf8mb4_sk_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[270] = new Collation(270, "utf8mb4_es_trad_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[271] = new Collation(271, "utf8mb4_la_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);

        collation[273] = new Collation(273, "utf8mb4_eo_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[274] = new Collation(274, "utf8mb4_hu_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[275] = new Collation(275, "utf8mb4_hr_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);

        collation[277] = new Collation(277, "utf8mb4_vi_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);

        collation[278] = new Collation(278, "utf8mb4_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[279] = new Collation(279, "utf8mb4_de_pb_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[280] = new Collation(280, "utf8mb4_is_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[281] = new Collation(281, "utf8mb4_lv_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[282] = new Collation(282, "utf8mb4_ro_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[283] = new Collation(283, "utf8mb4_sl_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[284] = new Collation(284, "utf8mb4_pl_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[285] = new Collation(285, "utf8mb4_et_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[286] = new Collation(286, "utf8mb4_es_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[287] = new Collation(287, "utf8mb4_sv_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[288] = new Collation(288, "utf8mb4_tr_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[289] = new Collation(289, "utf8mb4_cs_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[290] = new Collation(290, "utf8mb4_da_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[291] = new Collation(291, "utf8mb4_lt_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[292] = new Collation(292, "utf8mb4_sk_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[293] = new Collation(293, "utf8mb4_es_trad_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[294] = new Collation(294, "utf8mb4_la_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);

        collation[296] = new Collation(296, "utf8mb4_eo_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[297] = new Collation(297, "utf8mb4_hu_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[298] = new Collation(298, "utf8mb4_hr_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);

        collation[300] = new Collation(300, "utf8mb4_vi_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);

        collation[303] = new Collation(303, "utf8mb4_ja_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[304] = new Collation(304, "utf8mb4_ja_0900_as_cs_ks", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[305] = new Collation(305, "utf8mb4_0900_as_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[306] = new Collation(306, "utf8mb4_ru_0900_ai_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[307] = new Collation(307, "utf8mb4_ru_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[308] = new Collation(308, "utf8mb4_zh_0900_as_cs", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[309] = new Collation(309, "utf8mb4_0900_bin", 0, MYSQL_CHARSET_NAME_utf8mb4);

        collation[326] = new Collation(326, "utf8mb4_test_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);
        collation[327] = new Collation(327, "utf16_test_ci", 0, MYSQL_CHARSET_NAME_utf16);
        collation[328] = new Collation(328, "utf8mb4_test_400_ci", 0, MYSQL_CHARSET_NAME_utf8mb4);

        collation[336] = new Collation(336, "utf8_bengali_standard_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[337] = new Collation(337, "utf8_bengali_traditional_ci", 0, MYSQL_CHARSET_NAME_utf8);

        collation[352] = new Collation(352, "utf8_phone_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[353] = new Collation(353, "utf8_test_ci", 0, MYSQL_CHARSET_NAME_utf8);
        collation[354] = new Collation(354, "utf8_5624_1", 0, MYSQL_CHARSET_NAME_utf8);
        collation[355] = new Collation(355, "utf8_5624_2", 0, MYSQL_CHARSET_NAME_utf8);
        collation[356] = new Collation(356, "utf8_5624_3", 0, MYSQL_CHARSET_NAME_utf8);
        collation[357] = new Collation(357, "utf8_5624_4", 0, MYSQL_CHARSET_NAME_utf8);
        collation[358] = new Collation(358, "ucs2_test_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[359] = new Collation(359, "ucs2_vn_ci", 0, MYSQL_CHARSET_NAME_ucs2);
        collation[360] = new Collation(360, "ucs2_5624_1", 0, MYSQL_CHARSET_NAME_ucs2);

        collation[368] = new Collation(368, "utf8_5624_5", 0, MYSQL_CHARSET_NAME_utf8);
        collation[391] = new Collation(391, "utf32_test_ci", 0, MYSQL_CHARSET_NAME_utf32);
        collation[2047] = new Collation(2047, "utf8_maxuserid_ci", 0, MYSQL_CHARSET_NAME_utf8);

        COLLATION_INDEX_TO_COLLATION_NAME = new String[MAP_SIZE];
        COLLATION_INDEX_TO_CHARSET = new MysqlCharset[MAP_SIZE];
        Map<String, Integer> charsetNameToCollationIndexMap = new TreeMap<>();
        Map<String, Integer> charsetNameToCollationPriorityMap = new TreeMap<>();
        Set<Integer> tempUTF8MB4Indexes = new HashSet<>();

        Collation notUsedCollation = new Collation(0, COLLATION_NOT_DEFINED, 0, NOT_USED);
        for (int i = 1; i < MAP_SIZE; i++) {
            Collation coll = collation[i] != null ? collation[i] : notUsedCollation;
            COLLATION_INDEX_TO_COLLATION_NAME[i] = coll.collationName;
            COLLATION_INDEX_TO_CHARSET[i] = coll.mysqlCharset;
            String charsetName = coll.mysqlCharset.charsetName;

            if (!charsetNameToCollationIndexMap.containsKey(charsetName) || charsetNameToCollationPriorityMap.get(charsetName) < coll.priority) {
                charsetNameToCollationIndexMap.put(charsetName, i);
                charsetNameToCollationPriorityMap.put(charsetName, coll.priority);
            }

            // Filling indexes of utf8mb4 collations
            if (charsetName.equals(MYSQL_CHARSET_NAME_utf8mb4)) {
                tempUTF8MB4Indexes.add(i);
            }
        }

        CHARSET_NAME_TO_COLLATION_INDEX = Collections.unmodifiableMap(charsetNameToCollationIndexMap);
        UTF8MB4_INDEXES = Collections.unmodifiableSet(tempUTF8MB4Indexes);

        collation = null;
    }

    public final static String getMysqlCharsetForJavaEncoding(String javaEncoding, ServerVersion version) {

        List<MysqlCharset> mysqlCharsets = CharsetMapping.JAVA_ENCODING_UC_TO_MYSQL_CHARSET.get(javaEncoding.toUpperCase(Locale.ENGLISH));

        if (mysqlCharsets != null) {
            Iterator<MysqlCharset> iter = mysqlCharsets.iterator();

            MysqlCharset currentChoice = null;

            while (iter.hasNext()) {
                MysqlCharset charset = iter.next();

                if (version == null) {
                    // Take the first one we get

                    return charset.charsetName;
                }

                if (currentChoice == null || currentChoice.minimumVersion.compareTo(charset.minimumVersion) < 0
                        || currentChoice.priority < charset.priority && currentChoice.minimumVersion.compareTo(charset.minimumVersion) == 0) {
                    if (charset.isOkayForVersion(version)) {
                        currentChoice = charset;
                    }
                }
            }

            if (currentChoice != null) {
                return currentChoice.charsetName;
            }
        }

        return null;
    }

    public static int getCollationIndexForJavaEncoding(String javaEncoding, ServerVersion version) {
        String charsetName = getMysqlCharsetForJavaEncoding(javaEncoding, version);
        if (charsetName != null) {
            Integer ci = CHARSET_NAME_TO_COLLATION_INDEX.get(charsetName);
            if (ci != null) {
                return ci.intValue();
            }
        }
        return 0;
    }

    public static String getMysqlCharsetNameForCollationIndex(Integer collationIndex) {
        if (collationIndex != null && collationIndex > 0 && collationIndex < MAP_SIZE) {
            return COLLATION_INDEX_TO_CHARSET[collationIndex].charsetName;
        }
        return null;
    }

    /**
     * MySQL charset could map to several Java encodings.
     * So here we choose the one according to next rules:
     * <ul>
     * <li>if there is no static mapping for this charset then return javaEncoding value as is because this
     * could be a custom charset for example
     * <li>if static mapping exists and javaEncoding equals to one of Java encoding canonical names or aliases available
     * for this mapping then javaEncoding value as is; this is required when result should match to connection encoding, for example if connection encoding is
     * Cp943 we must avoid getting SHIFT_JIS for sjis mysql charset
     * <li>if static mapping exists and javaEncoding doesn't match any Java encoding canonical
     * names or aliases available for this mapping then return default Java encoding (the first in mapping list)
     * </ul>
     * 
     * @param mysqlCharsetName
     *            MySQL charset name
     * @param javaEncoding
     *            fall-back java encoding name
     * @return java encoding name
     */
    public static String getJavaEncodingForMysqlCharset(String mysqlCharsetName, String javaEncoding) {
        String res = javaEncoding;
        MysqlCharset cs = CHARSET_NAME_TO_CHARSET.get(mysqlCharsetName);
        if (cs != null) {
            res = cs.getMatchingJavaEncoding(javaEncoding);
        }
        return res;
    }

    public static String getJavaEncodingForMysqlCharset(String mysqlCharsetName) {
        return getJavaEncodingForMysqlCharset(mysqlCharsetName, null);
    }

    public static String getJavaEncodingForCollationIndex(Integer collationIndex, String javaEncoding) {
        if (collationIndex != null && collationIndex > 0 && collationIndex < MAP_SIZE) {
            MysqlCharset cs = COLLATION_INDEX_TO_CHARSET[collationIndex];
            return cs.getMatchingJavaEncoding(javaEncoding);
        }
        return null;
    }

    public static String getJavaEncodingForCollationIndex(Integer collationIndex) {
        return getJavaEncodingForCollationIndex(collationIndex, null);
    }

    public final static int getNumberOfCharsetsConfigured() {
        return numberOfEncodingsConfigured;
    }

    /**
     * Does the character set contain multi-byte encoded characters.
     * 
     * @param javaEncodingName
     *            java encoding name
     * @return true if the character set contains multi-byte encoded characters.
     */
    final public static boolean isMultibyteCharset(String javaEncodingName) {
        return MULTIBYTE_ENCODINGS.contains(javaEncodingName.toUpperCase(Locale.ENGLISH));
    }

    public static int getMblen(String charsetName) {
        if (charsetName != null) {
            MysqlCharset cs = CHARSET_NAME_TO_CHARSET.get(charsetName);
            if (cs != null) {
                return cs.mblen;
            }
        }
        return 0;
    }
}

class MysqlCharset {
    public final String charsetName;
    public final int mblen;
    public final int priority;
    public final List<String> javaEncodingsUc = new ArrayList<>();

    public final ServerVersion minimumVersion;

    /**
     * Constructs MysqlCharset object
     * 
     * @param charsetName
     *            MySQL charset name
     * @param mblen
     *            Max number of bytes per character
     * @param priority
     *            MysqlCharset with highest value of this param will be used for Java encoding --&gt; Mysql charsets conversion.
     * @param javaEncodings
     *            List of Java encodings corresponding to this MySQL charset; the first name in list is the default for mysql --&gt; java data conversion
     */
    public MysqlCharset(String charsetName, int mblen, int priority, String[] javaEncodings) {
        this(charsetName, mblen, priority, javaEncodings, new ServerVersion(0, 0, 0));
    }

    private void addEncodingMapping(String encoding) {
        String encodingUc = encoding.toUpperCase(Locale.ENGLISH);

        if (!this.javaEncodingsUc.contains(encodingUc)) {
            this.javaEncodingsUc.add(encodingUc);
        }
    }

    public MysqlCharset(String charsetName, int mblen, int priority, String[] javaEncodings, ServerVersion minimumVersion) {
        this.charsetName = charsetName;
        this.mblen = mblen;
        this.priority = priority;

        for (int i = 0; i < javaEncodings.length; i++) {
            String encoding = javaEncodings[i];
            try {
                Charset cs = Charset.forName(encoding);
                addEncodingMapping(cs.name());

                Set<String> als = cs.aliases();
                Iterator<String> ali = als.iterator();
                while (ali.hasNext()) {
                    addEncodingMapping(ali.next());
                }
            } catch (Exception e) {
                // if there is no support of this charset in JVM it's still possible to use our converter for 1-byte charsets
                if (mblen == 1) {
                    addEncodingMapping(encoding);
                }
            }
        }

        if (this.javaEncodingsUc.size() == 0) {
            if (mblen > 1) {
                addEncodingMapping("UTF-8");
            } else {
                addEncodingMapping("Cp1252");
            }
        }

        this.minimumVersion = minimumVersion;
    }

    @Override
    public String toString() {
        StringBuilder asString = new StringBuilder();
        asString.append("[");
        asString.append("charsetName=");
        asString.append(this.charsetName);
        asString.append(",mblen=");
        asString.append(this.mblen);
        // asString.append(",javaEncoding=");
        // asString.append(this.javaEncodings.toString());
        asString.append("]");
        return asString.toString();
    }

    boolean isOkayForVersion(ServerVersion version) {
        return version.meetsMinimum(this.minimumVersion);
    }

    /**
     * If javaEncoding parameter value is one of available java encodings for this charset
     * then returns javaEncoding value as is. Otherwise returns first available java encoding name.
     * 
     * @param javaEncoding
     *            java encoding name
     * @return java encoding name
     */
    String getMatchingJavaEncoding(String javaEncoding) {
        if (javaEncoding != null && this.javaEncodingsUc.contains(javaEncoding.toUpperCase(Locale.ENGLISH))) {
            return javaEncoding;
        }
        return this.javaEncodingsUc.get(0);
    }
}

class Collation {
    public final int index;
    public final String collationName;
    public final int priority;
    public final MysqlCharset mysqlCharset;

    public Collation(int index, String collationName, int priority, String charsetName) {
        this.index = index;
        this.collationName = collationName;
        this.priority = priority;
        this.mysqlCharset = CharsetMapping.CHARSET_NAME_TO_CHARSET.get(charsetName);
    }

    @Override
    public String toString() {
        StringBuilder asString = new StringBuilder();
        asString.append("[");
        asString.append("index=");
        asString.append(this.index);
        asString.append(",collationName=");
        asString.append(this.collationName);
        asString.append(",charsetName=");
        asString.append(this.mysqlCharset.charsetName);
        asString.append(",javaCharsetName=");
        asString.append(this.mysqlCharset.getMatchingJavaEncoding(null));
        asString.append("]");
        return asString.toString();
    }
}
