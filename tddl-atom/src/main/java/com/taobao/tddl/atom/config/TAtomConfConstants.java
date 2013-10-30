package com.taobao.tddl.atom.config;

import java.text.MessageFormat;

/**
 * atom ds里面跟获取配置相关的key
 * 
 * @author JIECHEN
 */
public class TAtomConfConstants {

    /**
     * 全局配置dataId模板
     */
    private static final MessageFormat GLOBAL_FORMAT  = new MessageFormat("com.taobao.tddl.atom.global.{0}");

    /**
     * 应用配置dataId模板
     */
    private static final MessageFormat APP_FORMAT     = new MessageFormat("com.taobao.tddl.atom.app.{0}.{1}");

    private static final MessageFormat PASSWD_FORMAT  = new MessageFormat("com.taobao.tddl.atom.passwd.{0}.{1}.{2}");

    /**
     * dbName模板
     */
    private static final MessageFormat DB_NAME_FORMAT = new MessageFormat("atom.dbkey.{0}^{1}");

    /**
     * 根据dbKey获取全局配置dataId
     * 
     * @param dbKey 数据库名KEY
     * @return
     */
    public static String getGlobalDataId(String dbKey) {
        return GLOBAL_FORMAT.format(new Object[] { dbKey });
    }

    /**
     * 根据应用名和dbKey获取指定的应用配置dataId
     * 
     * @param appName
     * @param dbKey
     * @return
     */
    public static String getAppDataId(String appName, String dbKey) {
        return APP_FORMAT.format(new Object[] { appName, dbKey });
    }

    /**
     * 根据dbKey和userName获得对应的passwd的dataId
     * 
     * @param dbKey
     * @param userName
     * @return
     */
    public static String getPasswdDataId(String dbName, String dbType, String userName) {
        return PASSWD_FORMAT.format(new Object[] { dbName, dbType, userName });
    }

    /**
     * @param appName
     * @param dbkey
     * @return
     */
    public static String getDbNameStr(String appName, String dbkey) {
        return DB_NAME_FORMAT.format(new Object[] { appName, dbkey });
    }

}
