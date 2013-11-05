package com.taobao.tddl.atom.config;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.taobao.tddl.atom.securety.PasswordCoder;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * TAtom数据源的推送配置解析类
 * 
 * @author qihao
 */
public class TAtomConfHelper {

    private static Logger      logger                                = LoggerFactory.getLogger(TAtomConfHelper.class);

    public static final String GLOBA_IP_KEY                          = "ip";
    public static final String GLOBA_PORT_KEY                        = "port";
    public static final String GLOBA_DB_NAME_KEY                     = "dbName";
    public static final String GLOBA_DB_TYPE_KEY                     = "dbType";
    public static final String GLOBA_DB_STATUS_KEY                   = "dbStatus";
    public static final String APP_USER_NAME_KEY                     = "userName";
    public static final String APP_MIN_POOL_SIZE_KEY                 = "minPoolSize";
    public static final String APP_MAX_POOL_SIZE_KEY                 = "maxPoolSize";
    public static final String APP_IDLE_TIMEOUT_KEY                  = "idleTimeout";
    public static final String APP_BLOCKING_TIMEOUT_KEY              = "blockingTimeout";
    public static final String APP_PREPARED_STATEMENT_CACHE_SIZE_KEY = "preparedStatementCacheSize";
    public static final String APP_ORACLE_CON_TYPE_KEY               = "oracleConType";
    public static final String APP_CON_PROP_KEY                      = "connectionProperties";
    public static final String PASSWD_ENC_PASSWD_KEY                 = "encPasswd";
    public static final String PASSWD_ENC_KEY_KEY                    = "encKey";

    public static final String APP_DRIVER_CLASS_KEY                  = "driverClass";
    /**
     * 写，次数限制
     */
    public static final String APP_WRITE_RESTRICT_TIMES              = "writeRestrictTimes";
    /**
     * 读，次数限制
     */
    public static final String APP_READ_RESTRICT_TIMES               = "readRestrictTimes";
    /**
     * thread count 次数限制
     */
    public static final String APP_THREAD_COUNT_RESTRICT             = "threadCountRestrict";

    public static final String APP_TIME_SLICE_IN_MILLS               = "timeSliceInMillis";

    /**
     * 应用连接限制: 限制某个应用键值的并发连接数。
     */
    public static final String APP_CONN_RESTRICT                     = "connRestrict";

    public static Map<String, String> parserConPropStr2Map(String conPropStr) {
        Map<String, String> connectionProperties = null;
        if (TStringUtil.isNotBlank(conPropStr)) {
            String[] keyValues = TStringUtil.split(conPropStr, ";");
            if (null != keyValues && keyValues.length > 0) {
                connectionProperties = new HashMap<String, String>(keyValues.length);
                for (String keyValue : keyValues) {
                    String key = TStringUtil.substringBefore(keyValue, "=");
                    String value = TStringUtil.substringAfter(keyValue, "=");
                    if (TStringUtil.isNotBlank(key) && TStringUtil.isNotBlank(value)) {
                        connectionProperties.put(key, value);
                    }
                }
            }
        }
        return connectionProperties;
    }

    public static String parserPasswd(String passwdStr) {
        String passwd = null;
        Properties passwdProp = TAtomConfHelper.parserConfStr2Properties(passwdStr);
        String encPasswd = passwdProp.getProperty(TAtomConfHelper.PASSWD_ENC_PASSWD_KEY);
        if (TStringUtil.isNotBlank(encPasswd)) {
            String encKey = passwdProp.getProperty(TAtomConfHelper.PASSWD_ENC_KEY_KEY);
            try {
                passwd = new PasswordCoder().decode(encKey, encPasswd);
            } catch (Exception e) {
                logger.error("[parserPasswd Error] decode dbPasswdError!may jdk version error!", e);
            }
        }
        return passwd;
    }

    public static Properties parserConfStr2Properties(String data) {
        Properties prop = new Properties();
        if (TStringUtil.isNotBlank(data)) {
            ByteArrayInputStream byteArrayInputStream = null;
            try {
                byteArrayInputStream = new ByteArrayInputStream((data).getBytes());
                prop.load(byteArrayInputStream);
            } catch (IOException e) {
                logger.error("parserConfStr2Properties Error", e);
            } finally {
                if (byteArrayInputStream != null) {
                    try {
                        byteArrayInputStream.close();
                    } catch (IOException e) {
                        logger.error("parserConfStr2Properties Error", e);
                    }
                }
            }
        }
        return prop;
    }
}
