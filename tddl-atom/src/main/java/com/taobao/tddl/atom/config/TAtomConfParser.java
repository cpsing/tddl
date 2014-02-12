package com.taobao.tddl.atom.config;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.BooleanUtils;

import com.taobao.tddl.atom.securety.impl.PasswordCoder;
import com.taobao.tddl.atom.utils.ConnRestrictEntry;
import com.taobao.tddl.common.utils.TStringUtil;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * TAtom数据源的推送配置解析类
 * 
 * @author qihao
 */
public class TAtomConfParser {

    private static Logger      logger                                = LoggerFactory.getLogger(TAtomConfParser.class);

    public static final String GLOBA_IP_KEY                          = "ip";
    public static final String GLOBA_PORT_KEY                        = "port";
    public static final String GLOBA_DB_NAME_KEY                     = "dbName";
    public static final String GLOBA_DB_TYPE_KEY                     = "dbType";
    public static final String GLOBA_DB_STATUS_KEY                   = "dbStatus";
    public static final String APP_USER_NAME_KEY                     = "userName";
    public static final String APP_INIT_POOL_SIZE_KEY                = "initPoolSize";
    public static final String APP_PREFILL                           = "prefill";
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

    public static TAtomDsConfDO parserTAtomDsConfDO(String globaConfStr, String appConfStr) {
        TAtomDsConfDO pasObj = new TAtomDsConfDO();
        if (TStringUtil.isNotBlank(globaConfStr)) {
            Properties globaProp = TAtomConfParser.parserConfStr2Properties(globaConfStr);
            if (!globaProp.isEmpty()) {
                String ip = TStringUtil.trim(globaProp.getProperty(TAtomConfParser.GLOBA_IP_KEY));
                if (TStringUtil.isNotBlank(ip)) {
                    pasObj.setIp(ip);
                }
                String port = TStringUtil.trim(globaProp.getProperty(TAtomConfParser.GLOBA_PORT_KEY));
                if (TStringUtil.isNotBlank(port)) {
                    pasObj.setPort(port);
                }
                String dbName = TStringUtil.trim(globaProp.getProperty(TAtomConfParser.GLOBA_DB_NAME_KEY));
                if (TStringUtil.isNotBlank(dbName)) {
                    pasObj.setDbName(dbName);
                }
                String dbType = TStringUtil.trim(globaProp.getProperty(TAtomConfParser.GLOBA_DB_TYPE_KEY));
                if (TStringUtil.isNotBlank(dbType)) {
                    pasObj.setDbType(dbType);
                }
                String dbStatus = TStringUtil.trim(globaProp.getProperty(TAtomConfParser.GLOBA_DB_STATUS_KEY));
                if (TStringUtil.isNotBlank(dbStatus)) {
                    pasObj.setDbStatus(dbStatus);
                }
            }
        }
        if (TStringUtil.isNotBlank(appConfStr)) {
            Properties appProp = TAtomConfParser.parserConfStr2Properties(appConfStr);
            if (!appProp.isEmpty()) {
                String userName = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_USER_NAME_KEY));
                if (TStringUtil.isNotBlank(userName)) {
                    pasObj.setUserName(userName);
                }
                String oracleConType = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_ORACLE_CON_TYPE_KEY));
                if (TStringUtil.isNotBlank(oracleConType)) {
                    pasObj.setOracleConType(oracleConType);
                }
                String initPoolSize = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_INIT_POOL_SIZE_KEY));
                if (TStringUtil.isNotBlank(initPoolSize) && TStringUtil.isNumeric(initPoolSize)) {
                    pasObj.setInitPoolSize(Integer.valueOf(initPoolSize));
                }
                String minPoolSize = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_MIN_POOL_SIZE_KEY));
                if (TStringUtil.isNotBlank(minPoolSize) && TStringUtil.isNumeric(minPoolSize)) {
                    pasObj.setMinPoolSize(Integer.valueOf(minPoolSize));
                }
                String maxPoolSize = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_MAX_POOL_SIZE_KEY));
                if (TStringUtil.isNotBlank(maxPoolSize) && TStringUtil.isNumeric(maxPoolSize)) {
                    pasObj.setMaxPoolSize(Integer.valueOf(maxPoolSize));
                }
                String idleTimeout = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_IDLE_TIMEOUT_KEY));
                if (TStringUtil.isNotBlank(idleTimeout) && TStringUtil.isNumeric(idleTimeout)) {
                    pasObj.setIdleTimeout(Long.valueOf(idleTimeout));
                }
                String blockingTimeout = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_BLOCKING_TIMEOUT_KEY));
                if (TStringUtil.isNotBlank(blockingTimeout) && TStringUtil.isNumeric(blockingTimeout)) {
                    pasObj.setBlockingTimeout(Integer.valueOf(blockingTimeout));
                }
                String preparedStatementCacheSize = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_PREPARED_STATEMENT_CACHE_SIZE_KEY));
                if (TStringUtil.isNotBlank(preparedStatementCacheSize)
                    && TStringUtil.isNumeric(preparedStatementCacheSize)) {
                    pasObj.setPreparedStatementCacheSize(Integer.valueOf(preparedStatementCacheSize));
                }

                String writeRestrictTimes = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_WRITE_RESTRICT_TIMES));
                if (TStringUtil.isNotBlank(writeRestrictTimes) && TStringUtil.isNumeric(writeRestrictTimes)) {
                    pasObj.setWriteRestrictTimes(Integer.valueOf(writeRestrictTimes));
                }

                String readRestrictTimes = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_READ_RESTRICT_TIMES));
                if (TStringUtil.isNotBlank(readRestrictTimes) && TStringUtil.isNumeric(readRestrictTimes)) {
                    pasObj.setReadRestrictTimes(Integer.valueOf(readRestrictTimes));
                }
                String threadCountRestrict = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_THREAD_COUNT_RESTRICT));
                if (TStringUtil.isNotBlank(threadCountRestrict) && TStringUtil.isNumeric(threadCountRestrict)) {
                    pasObj.setThreadCountRestrict(Integer.valueOf(threadCountRestrict));
                }
                String timeSliceInMillis = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_TIME_SLICE_IN_MILLS));
                if (TStringUtil.isNotBlank(timeSliceInMillis) && TStringUtil.isNumeric(timeSliceInMillis)) {
                    pasObj.setTimeSliceInMillis(Integer.valueOf(timeSliceInMillis));
                }

                String conPropStr = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_CON_PROP_KEY));
                Map<String, String> connectionProperties = parserConPropStr2Map(conPropStr);
                if (null != connectionProperties && !connectionProperties.isEmpty()) {
                    pasObj.setConnectionProperties(connectionProperties);
                    String driverClass = connectionProperties.get(TAtomConfParser.APP_DRIVER_CLASS_KEY);
                    if (!TStringUtil.isBlank(driverClass)) {
                        pasObj.setDriverClass(driverClass);
                    }

                    if (connectionProperties.containsKey(APP_PREFILL)) {
                        // add by agapple, 简单处理支持下初始化链接
                        String prefill = connectionProperties.get(APP_PREFILL);
                        if (BooleanUtils.toBoolean(prefill)
                            && pasObj.getInitPoolSize() == TAtomDsConfDO.defaultInitPoolSize) {
                            pasObj.setInitPoolSize(pasObj.getMinPoolSize());
                        }
                    }
                }

                // 解析应用连接限制, 参看下面的文档
                String connRestrictStr = TStringUtil.trim(appProp.getProperty(TAtomConfParser.APP_CONN_RESTRICT));
                List<ConnRestrictEntry> connRestrictEntries = parseConnRestrictEntries(connRestrictStr,
                    pasObj.getMaxPoolSize());
                if (null != connRestrictEntries && !connRestrictEntries.isEmpty()) {
                    pasObj.setConnRestrictEntries(connRestrictEntries);
                }
            }
        }
        return pasObj;
    }

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
        Properties passwdProp = TAtomConfParser.parserConfStr2Properties(passwdStr);
        String encPasswd = passwdProp.getProperty(TAtomConfParser.PASSWD_ENC_PASSWD_KEY);
        if (TStringUtil.isNotBlank(encPasswd)) {
            String encKey = passwdProp.getProperty(TAtomConfParser.PASSWD_ENC_KEY_KEY);
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
                try {
                    byteArrayInputStream.close();
                } catch (IOException e) {
                    logger.error("parserConfStr2Properties Error", e);
                }
            }
        }
        return prop;
    }

    /**
     * HASH 策略的最大槽数量限制。
     */
    public static final int MAX_HASH_RESTRICT_SLOT = 32;

    /**
     * 解析应用连接限制, 完整格式是:
     * "K1,K2,K3,K4:80%; K5,K6,K7,K8:80%; K9,K10,K11,K12:80%; *:16,80%; ~:80%;"
     * 这样可以兼容 HASH: "*:16,80%", 也可以兼容 LIST:
     * "K1:80%; K2:80%; K3:80%; K4:80%; ~:80%;" 配置可以是连接数, 也可以是百分比。
     */
    public static List<ConnRestrictEntry> parseConnRestrictEntries(String connRestrictStr, int maxPoolSize) {
        List<ConnRestrictEntry> connRestrictEntries = null;
        if (TStringUtil.isNotBlank(connRestrictStr)) {
            // Split "K1:number1; K2:number2; ...; *:count,number3; ~:number4"
            String[] entries = TStringUtil.split(connRestrictStr, ";");
            if (null != entries && entries.length > 0) {
                HashMap<String, String> existKeys = new HashMap<String, String>();
                connRestrictEntries = new ArrayList<ConnRestrictEntry>(entries.length);
                for (String entry : entries) {
                    // Parse "K1,K2,K3:number | *:count,number | ~:number"
                    int find = entry.indexOf(':');
                    if (find >= 1 && find < (entry.length() - 1)) {
                        String key = entry.substring(0, find).trim();
                        String value = entry.substring(find + 1).trim();
                        // "K1,K2,K3:number | *:count,number | ~:number"
                        ConnRestrictEntry connRestrictEntry = ConnRestrictEntry.parseEntry(key, value, maxPoolSize);
                        if (connRestrictEntry == null) {
                            logger.error("[connRestrict Error] parse entry error: " + entry);
                        } else {
                            // Remark entry config problem
                            if (0 >= connRestrictEntry.getLimits()) {
                                logger.error("[connRestrict Error] connection limit is 0: " + entry);
                                connRestrictEntry.setLimits(/* 至少允许一个连接 */1);
                            }
                            if (ConnRestrictEntry.MAX_HASH_RESTRICT_SLOT < connRestrictEntry.getHashSize()) {
                                logger.error("[connRestrict Error] hash size exceed maximum: " + entry);
                                connRestrictEntry.setHashSize(ConnRestrictEntry.MAX_HASH_RESTRICT_SLOT);
                            }
                            // Remark Key config confliction
                            for (String slotKey : connRestrictEntry.getKeys()) {
                                if (!existKeys.containsKey(slotKey)) {
                                    existKeys.put(slotKey, entry);
                                } else if (ConnRestrictEntry.isWildcard(slotKey)) {
                                    logger.error("[connRestrict Error] hash config [" + entry + "] conflict with ["
                                                 + existKeys.get(slotKey) + "]");
                                } else if (ConnRestrictEntry.isNullKey(slotKey)) {
                                    logger.error("[connRestrict Error] null-key config [" + entry + "] conflict with ["
                                                 + existKeys.get(slotKey) + "]");
                                } else {
                                    logger.error("[connRestrict Error] " + slotKey + " config [" + entry
                                                 + "] conflict with [" + existKeys.get(slotKey) + "]");
                                }
                            }
                            connRestrictEntries.add(connRestrictEntry);
                        }
                    } else {
                        logger.error("[connRestrict Error] unknown entry: " + entry);
                    }
                }
            }
        }
        return connRestrictEntries;
    }
}
