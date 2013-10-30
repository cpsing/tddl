package com.taobao.tddl.atom;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.atom.config.TAtomConfConstants;

/**
 * TAtom数据源的常量设置类
 * 
 * @author qihao
 */
public class TAtomConstants extends TAtomConfConstants {

    public final static String        DEFAULT_DIAMOND_GROUP                = null;

    public final static String        DEFAULT_MYSQL_CHAR_SET               = "gbk";

    // public final static String ORACLE_DBTYPE_STR = "ORACLE";

    // public final static String MYSQL_DBTYPE_STR = "MYSQL";

    public final static String        DEFAULT_ORACLE_CON_TYPE              = "oci";

    public final static String        DB_STATUS_R                          = "R";

    public final static String        DB_STATUS_W                          = "W";

    public final static String        DB_STATUS_RW                         = "RW";

    public final static String        DB_STATUS_NA                         = "NA";

    public static Map<String, String> DEFAULT_ORACLE_CONNECTION_PROPERTIES = new HashMap<String, String>(2);
    static {
        TAtomConstants.DEFAULT_ORACLE_CONNECTION_PROPERTIES.put("SetBigStringTryClob", "true");
        TAtomConstants.DEFAULT_ORACLE_CONNECTION_PROPERTIES.put("defaultRowPrefetch", "50");
    }

    public static Map<String, String> DEFAULT_MYSQL_CONNECTION_PROPERTIES  = new HashMap<String, String>(1);
    static {
        TAtomConstants.DEFAULT_MYSQL_CONNECTION_PROPERTIES.put("characterEncoding", "gbk");
    }

    public final static String        DEFAULT_ORACLE_DRIVER_CLASS          = "oracle.jdbc.driver.OracleDriver";

    public final static String        DEFAULT_MYSQL_DRIVER_CLASS           = "com.mysql.jdbc.Driver";

    public final static String        DEFAULT_ORACLE_SORTER_CLASS          = "com.taobao.datasource.resource.adapter.jdbc.vendor.OracleExceptionSorter";

    public final static String        DEFAULT_MYSQL_SORTER_CLASS           = "com.taobao.datasource.resource.adapter.jdbc.vendor.MySQLExceptionSorter";

    public final static String        DEFAULT_MYSQL_VALID_CHECKERCLASS     = "com.taobao.datasource.resource.adapter.jdbc.vendor.MySQLValidConnectionChecker";

    public final static String        MYSQL_INTEGRATION_SORTER_CLASS       = "com.mysql.jdbc.integration.jboss.ExtendedMysqlExceptionSorter";

    public final static String        MYSQL_VALID_CONNECTION_CHECKERCLASS  = "com.mysql.jdbc.integration.jboss.MysqlValidConnectionChecker";

}
