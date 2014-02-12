package com.taobao.tddl.atom.config;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;
import com.taobao.tddl.atom.TAtomDbStatusEnum;
import com.taobao.tddl.atom.TAtomDbTypeEnum;
import com.taobao.tddl.atom.common.TAtomConURLTools;
import com.taobao.tddl.atom.common.TAtomConstants;
import com.taobao.tddl.atom.config.listener.AtomDbStatusListener;
import com.taobao.tddl.atom.exception.AtomAlreadyInitException;
import com.taobao.tddl.atom.exception.AtomIllegalException;
import com.taobao.tddl.atom.exception.AtomInitialException;
import com.taobao.tddl.atom.jdbc.TDataSourceWrapper;
import com.taobao.tddl.atom.utils.ConnRestrictEntry;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.monitor.Monitor;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 数据库动态切换的Handle类，所有数据库的动态切换 都是由这个类完成
 * 
 * @author qihao
 */
public class TAtomDsConfHandle {

    private static Logger                       logger      = LoggerFactory.getLogger(TAtomDsConfHandle.class);

    private String                              appName;

    private String                              dbKey;

    private String                              unitName;

    /**
     * 运行时配置
     */
    private volatile TAtomDsConfDO              runTimeConf = new TAtomDsConfDO();

    /**
     * 本地配置，优先于推送的动态配置
     */
    private TAtomDsConfDO                       localConf   = new TAtomDsConfDO();

    /**
     * 全局配置，应用配置订阅管理
     */
    private DbConfManager                       dbConfManager;

    /**
     * 密码配置订阅管理
     */
    private DbPasswdManager                     dbPasswdManager;

    /**
     * druid数据源通过init初始化
     */
    private volatile DruidDataSource            druidDataSource;

    /**
     * 数据库状态改变回调
     */
    private volatile List<AtomDbStatusListener> dbStatusListeners;

    /**
     * 初始化标记为一但初始化过，所有本地的配置禁止改动
     */
    private volatile boolean                    initFalg;

    /**
     * 数据源操作锁，当需要对数据源进行重建或者刷新时需要先获得该锁
     */
    private final ReentrantLock                 lock        = new ReentrantLock();

    // public static final int druidStatMaxKeySize = 5000;
    // public static final int druidFlushIntervalMill = 300*1000;

    /**
     * 初始化方法，创建对应的数据源，只能被调用一次
     * 
     * @throws Exception
     */
    public void init() throws Exception {
        if (initFalg) {
            throw new AtomAlreadyInitException("[AlreadyInit] double call Init !");
        }
        // 1.初始化参数检查
        if (TStringUtil.isBlank(this.appName) || TStringUtil.isBlank(this.dbKey)) {
            String errorMsg = "[attributeError] TAtomDatasource of appName Or dbKey is Empty !";
            logger.error(errorMsg);
            throw new AtomIllegalException(errorMsg);
        }
        // 2.配置dbConfManager
        AtomConfigManager defaultDbConfManager = new AtomConfigManager();
        defaultDbConfManager.setGlobalConfigDataId(TAtomConstants.getGlobalDataId(this.dbKey));
        defaultDbConfManager.setAppConfigDataId(TAtomConstants.getAppDataId(this.appName, this.dbKey));
        defaultDbConfManager.setUnitName(unitName);
        // 初始化dbConfManager
        defaultDbConfManager.init(appName);
        dbConfManager = defaultDbConfManager;
        // 3.获取全局配置
        String globaConfStr = dbConfManager.getGlobalDbConf();
        // 注册全局配置监听
        registerGlobaDbConfListener(defaultDbConfManager);
        if (TStringUtil.isBlank(globaConfStr)) {
            String errorMsg = "[ConfError] read globalConfig is Empty !";
            logger.error(errorMsg);
            throw new AtomInitialException(errorMsg);
        }
        // 4.获取应用配置
        String appConfStr = dbConfManager.getAppDbDbConf();
        // 注册应用配置监听
        registerAppDbConfListener(defaultDbConfManager);
        if (TStringUtil.isBlank(appConfStr)) {
            String errorMsg = "[ConfError] read appConfig is Empty !";
            logger.error(errorMsg);
            throw new AtomInitialException(errorMsg);
        }
        lock.lock();
        try {
            // 5.解析配置string成TAtomDsConfDO
            runTimeConf = TAtomConfParser.parserTAtomDsConfDO(globaConfStr, appConfStr);
            // 6.处理本地优先配置
            overConfByLocal(localConf, runTimeConf);
            // 7.如果没有设置本地密码，则用订的密码，初始化passwdManager
            if (TStringUtil.isBlank(this.runTimeConf.getPasswd())) {
                // 检查dbKey和对应的userName是否为空
                if (TStringUtil.isBlank(runTimeConf.getUserName())) {
                    String errorMsg = "[attributeError] TAtomDatasource of UserName is Empty !";
                    logger.error(errorMsg);
                    throw new AtomIllegalException(errorMsg);
                }
                AtomPasswdManager diamondDbPasswdManager = new AtomPasswdManager();
                diamondDbPasswdManager.setPasswdConfDataId(TAtomConstants.getPasswdDataId(runTimeConf.getDbName(),
                    runTimeConf.getDbType(),
                    runTimeConf.getUserName()));
                diamondDbPasswdManager.setUnitName(unitName);
                diamondDbPasswdManager.init(appName);
                dbPasswdManager = diamondDbPasswdManager;
                // 获取密码
                String passwd = dbPasswdManager.getPasswd();
                registerPasswdConfListener(diamondDbPasswdManager);
                if (TStringUtil.isBlank(passwd)) {
                    String errorMsg = "[PasswdError] read passwd is Empty !";
                    logger.error(errorMsg);
                    throw new AtomInitialException(errorMsg);
                }
                runTimeConf.setPasswd(passwd);
            }
            // 8.转换tAtomDsConfDO
            DruidDataSource druidDataSource = convertTAtomDsConf2DruidConf(TAtomDsConfHandle.this.dbKey,
                this.runTimeConf,
                TAtomConstants.getDbNameStr(this.unitName, this.appName, this.dbKey));
            // 9.参数检查如果参数不正确直接抛出异常
            if (!checkLocalTxDataSourceDO(druidDataSource)) {
                String errorMsg = "[ConfigError]init dataSource Prams Error! config is : " + druidDataSource.toString();
                logger.error(errorMsg);
                throw new AtomInitialException(errorMsg);
            }
            // 10.创建数据源
            // druidDataSource.setUseJmx(false);
            // LocalTxDataSource localTxDataSource = TaobaoDataSourceFactory
            // .createLocalTxDataSource(localTxDataSourceDO);
            // 11.将创建好的数据源是指到TAtomDatasource中
            druidDataSource.init();

            // druidDataSource.getDataSourceStat().setMaxSqlSize(DruidDsConfHandle.druidStatMaxKeySize);
            this.druidDataSource = druidDataSource;
            clearDataSourceWrapper();
            initFalg = true;
        } finally {
            lock.unlock();
        }
    }

    private void clearDataSourceWrapper() {
        Monitor.removeSnapshotValuesCallback(wrapDataSource);
        wrapDataSource = null;
    }

    /**
     * 注册密码变化监听器
     * 
     * @param dbPasswdManager
     */
    private void registerPasswdConfListener(DbPasswdManager dbPasswdManager) {
        dbPasswdManager.registerPasswdConfListener(new ConfigDataListener() {

            public void onDataRecieved(String dataId, String data) {
                logger.warn("[Passwd HandleData] dataId : " + dataId + " data: " + data);
                if (null == data || TStringUtil.isBlank(data)) {
                    return;
                }
                lock.lock();
                try {
                    String localPasswd = TAtomDsConfHandle.this.localConf.getPasswd();
                    if (TStringUtil.isNotBlank(localPasswd)) {
                        // 如果本地配置了passwd直接返回不支持动态修改
                        return;
                    }
                    String newPasswd = TAtomConfParser.parserPasswd(data);
                    String runPasswd = TAtomDsConfHandle.this.runTimeConf.getPasswd();
                    if (!TStringUtil.equals(runPasswd, newPasswd)) {
                        try {
                            // modify by junyu 2013-06-14:dynamic change
                            // passwd,not recreate it!
                            // DruidDataSource newDruidDataSource =
                            // DruidDsConfHandle.this.druidDataSource.cloneDruidDataSource();
                            // newDruidDataSource.setPassword(newPasswd);
                            // newDruidDataSource.init();
                            // DruidDataSource tempDataSource =
                            // DruidDsConfHandle.this.druidDataSource;
                            // DruidDsConfHandle.this.druidDataSource =
                            // newDruidDataSource;
                            // tempDataSource.close();
                            // logger.warn("[DRUID CHANGE PASSWORD] ReCreate DataSource !");
                            // 是用新的配置覆盖运行时的配置
                            // clearDataSourceWrapper();
                            TAtomDsConfHandle.this.druidDataSource.setPassword(newPasswd);
                            logger.warn("[DRUID CHANGE PASSWORD] already reset the new passwd!");
                            TAtomDsConfHandle.this.runTimeConf.setPasswd(newPasswd);
                        } catch (Exception e) {
                            logger.error("[DRUID CHANGE PASSWORD] reset new passwd error!", e);
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }
        });
    }

    /**
     * 全局配置监听,全局配置发生变化， 需要重新FLUSH数据源
     * 
     * @param defaultDbConfManager
     */
    private void registerGlobaDbConfListener(DbConfManager dbConfManager) {
        dbConfManager.registerGlobaDbConfListener(new ConfigDataListener() {

            public void onDataRecieved(String dataId, String data) {
                logger.warn("[DRUID GlobaConf HandleData] dataId : " + dataId + " data: " + data);
                if (null == data || TStringUtil.isBlank(data)) {
                    return;
                }
                lock.lock();
                try {
                    String globaConfStr = data;
                    // 如果是全局配置发生变化，可能是IP,PORT,DBNAME,DBTYPE,STATUS
                    TAtomDsConfDO tmpConf = TAtomConfParser.parserTAtomDsConfDO(globaConfStr, null);
                    TAtomDsConfDO newConf = TAtomDsConfHandle.this.runTimeConf.clone();
                    // 是用推送的配置，覆盖当前的配置
                    newConf.setIp(tmpConf.getIp());
                    newConf.setPort(tmpConf.getPort());
                    newConf.setDbName(tmpConf.getDbName());
                    newConf.setDbType(tmpConf.getDbType());
                    newConf.setDbStatus(tmpConf.getDbStatus());
                    // 处理本地优先配置
                    overConfByLocal(TAtomDsConfHandle.this.localConf, newConf);
                    // 如果推送过来的数据库状态是 RW/R->NA,直接销毁掉数据源，以下业务逻辑不做处理
                    if (TAtomDbStatusEnum.NA_STATUS != TAtomDsConfHandle.this.runTimeConf.getDbStautsEnum()
                        && TAtomDbStatusEnum.NA_STATUS == tmpConf.getDbStautsEnum()) {
                        try {
                            TAtomDsConfHandle.this.druidDataSource.close();
                            logger.warn("[DRUID NA STATUS PUSH] destroy DataSource !");
                        } catch (Exception e) {
                            logger.error("[DRUID NA STATUS PUSH] destroy DataSource  Error!", e);
                        }
                    } else {
                        // 转换tAtomDsConfDO
                        DruidDataSource druidDataSource;
                        try {
                            druidDataSource = convertTAtomDsConf2DruidConf(TAtomDsConfHandle.this.dbKey,
                                newConf,
                                TAtomConstants.getDbNameStr(TAtomDsConfHandle.this.unitName,
                                    TAtomDsConfHandle.this.appName,
                                    TAtomDsConfHandle.this.dbKey));
                        } catch (Exception e1) {
                            logger.error("[DRUID GlobaConfError] convertTAtomDsConf2DruidConf Error! dataId : "
                                         + dataId + " config : " + data);
                            return;
                        }
                        // 检查转换后结果是否正确
                        if (!checkLocalTxDataSourceDO(druidDataSource)) {
                            logger.error("[DRUID GlobaConfError] dataSource Prams Error! dataId : " + dataId
                                         + " config : " + data);
                            return;
                        }
                        // 如果推送的状态时 NA->RW/R 时需要重新创建数据源，无需再刷新
                        if (TAtomDsConfHandle.this.runTimeConf.getDbStautsEnum() == TAtomDbStatusEnum.NA_STATUS
                            && (newConf.getDbStautsEnum() == TAtomDbStatusEnum.RW_STATUS
                                || newConf.getDbStautsEnum() == TAtomDbStatusEnum.R_STATUS || newConf.getDbStautsEnum() == TAtomDbStatusEnum.W_STATUS)) {
                            // 创建数据源
                            try {
                                // 关闭TB-DATASOURCE的JMX注册
                                // localTxDataSourceDO.setUseJmx(false);
                                // LocalTxDataSource localTxDataSource =
                                // TaobaoDataSourceFactory
                                // .createLocalTxDataSource(localTxDataSourceDO);
                                druidDataSource.init();
                                // druidDataSource.getDataSourceStat().setMaxSqlSize(DruidDsConfHandle.druidStatMaxKeySize);
                                DruidDataSource tempDataSource = TAtomDsConfHandle.this.druidDataSource;
                                TAtomDsConfHandle.this.druidDataSource = druidDataSource;
                                tempDataSource.close();
                                logger.warn("[DRUID NA->RW/R STATUS PUSH] ReCreate DataSource !");
                            } catch (Exception e) {
                                logger.error("[DRUID NA->RW/R STATUS PUSH] ReCreate DataSource Error!", e);
                            }
                        } else {
                            boolean needCreate = isGlobalChangeNeedReCreate(TAtomDsConfHandle.this.runTimeConf, newConf);
                            // 如果发生的配置变化是否需要重建数据源
                            // druid 没有flush方法，只能重建数据源 jiechen.qzm
                            if (needCreate) {
                                try {
                                    // 更新数据源
                                    druidDataSource.init();
                                    // druidDataSource.getDataSourceStat().setMaxSqlSize(druidStatMaxKeySize);
                                    DruidDataSource tempDataSource = TAtomDsConfHandle.this.druidDataSource;
                                    TAtomDsConfHandle.this.druidDataSource = druidDataSource;
                                    tempDataSource.close();
                                    logger.warn("[DRUID CONFIG CHANGE STATUS] Always ReCreate DataSource !");
                                } catch (Exception e) {
                                    logger.error("[DRUID Create GlobaConf Error]  Always ReCreate DataSource Error !",
                                        e);
                                }
                            } else {
                                logger.warn("[DRUID Create GlobaConf Error]  global config is same!nothing will be done! the global config is:"
                                            + globaConfStr);
                            }
                        }
                    }
                    // 处理数据库状态监听器
                    processDbStatusListener(TAtomDsConfHandle.this.runTimeConf.getDbStautsEnum(),
                        newConf.getDbStautsEnum());
                    // 是用新的配置覆盖运行时的配置
                    TAtomDsConfHandle.this.runTimeConf = newConf;
                    clearDataSourceWrapper();
                } finally {
                    lock.unlock();
                }
            }

            private boolean isGlobalChangeNeedReCreate(TAtomDsConfDO runConf, TAtomDsConfDO newConf) {
                boolean needReCreate = false;
                if (!TStringUtil.equals(runConf.getIp(), newConf.getIp())) {
                    needReCreate = true;
                    return needReCreate;
                }
                if (!TStringUtil.equals(runConf.getPort(), newConf.getPort())) {
                    needReCreate = true;
                    return needReCreate;
                }
                if (!TStringUtil.equals(runConf.getDbName(), newConf.getDbName())) {
                    needReCreate = true;
                    return needReCreate;
                }
                if (runConf.getDbTypeEnum() != newConf.getDbTypeEnum()) {
                    needReCreate = true;
                    return needReCreate;
                }

                return needReCreate;
            }
        });
    }

    /**
     * 应用配置监听，当应用配置发生变化时，区分发生 变化的配置，来决定具体是flush还是reCreate
     * 
     * @param defaultDbConfManager
     */
    private void registerAppDbConfListener(DbConfManager dbConfManager) {
        dbConfManager.registerAppDbConfListener(new ConfigDataListener() {

            public void onDataRecieved(String dataId, String data) {
                logger.warn("[DRUID AppConf HandleData] dataId : " + dataId + " data: " + data);
                if (null == data || TStringUtil.isBlank(data)) {
                    return;
                }
                lock.lock();
                try {
                    String appConfStr = data;
                    TAtomDsConfDO tmpConf = TAtomConfParser.parserTAtomDsConfDO(null, appConfStr);
                    TAtomDsConfDO newConf = TAtomDsConfHandle.this.runTimeConf.clone();
                    // 有些既有配置不能变更，所以克隆老的配置，然后将新的set进去
                    newConf.setUserName(tmpConf.getUserName());
                    newConf.setMinPoolSize(tmpConf.getMinPoolSize());
                    newConf.setMaxPoolSize(tmpConf.getMaxPoolSize());
                    newConf.setInitPoolSize(tmpConf.getInitPoolSize());
                    newConf.setIdleTimeout(tmpConf.getIdleTimeout());
                    newConf.setBlockingTimeout(tmpConf.getBlockingTimeout());
                    newConf.setPreparedStatementCacheSize(tmpConf.getPreparedStatementCacheSize());
                    newConf.setConnectionProperties(tmpConf.getConnectionProperties());
                    newConf.setOracleConType(tmpConf.getOracleConType());
                    // 增加3个具体的实现
                    newConf.setWriteRestrictTimes(tmpConf.getWriteRestrictTimes());
                    newConf.setReadRestrictTimes(tmpConf.getReadRestrictTimes());
                    newConf.setThreadCountRestrict(tmpConf.getThreadCountRestrict());
                    newConf.setTimeSliceInMillis(tmpConf.getTimeSliceInMillis());
                    newConf.setDriverClass(tmpConf.getDriverClass());
                    // 处理本地优先配置
                    overConfByLocal(TAtomDsConfHandle.this.localConf, newConf);

                    boolean isNeedReCreate = isAppChangeNeedReCreate(TAtomDsConfHandle.this.runTimeConf, newConf);
                    if (isNeedReCreate) {
                        // 转换tAtomDsConfDO
                        DruidDataSource druidDataSource;
                        try {
                            druidDataSource = convertTAtomDsConf2DruidConf(TAtomDsConfHandle.this.dbKey,
                                newConf,
                                TAtomConstants.getDbNameStr(TAtomDsConfHandle.this.unitName,
                                    TAtomDsConfHandle.this.appName,
                                    TAtomDsConfHandle.this.dbKey));
                        } catch (Exception e1) {
                            logger.error("[DRUID GlobaConfError] convertTAtomDsConf2DruidConf Error! dataId : "
                                         + dataId + " config : " + data);
                            return;
                        }
                        // 检查转换后结果是否正确
                        if (!checkLocalTxDataSourceDO(druidDataSource)) {
                            logger.error("[DRUID GlobaConfError] dataSource Prams Error! dataId : " + dataId
                                         + " config : " + data);
                            return;
                        }

                        try {
                            // 这个必须在最前面，否则下次推送可能无法比较出不同而不创建新数据
                            TAtomDsConfHandle.this.runTimeConf = newConf;
                            TAtomDsConfHandle.this.druidDataSource.close();
                            logger.warn("[DRUID destroy OldDataSource] dataId : " + dataId);
                            druidDataSource.init();
                            // druidDataSource.getDataSourceStat().setMaxSqlSize(DruidDsConfHandle.druidStatMaxKeySize);
                            logger.warn("[DRUID create newDataSource] dataId : " + dataId);
                            TAtomDsConfHandle.this.druidDataSource = druidDataSource;
                            clearDataSourceWrapper();
                        } catch (Exception e) {
                            logger.error("[DRUID Create GlobaConf Error]  Always ReCreate DataSource Error ! dataId: "
                                         + dataId, e);
                        }
                    } else {
                        boolean isNeedFlush = isAppChangeNeedFlush(TAtomDsConfHandle.this.runTimeConf, newConf);
                        /**
                         * 阀值变化无需刷新持有的数据源，只要更新runTimeConf，并且清空wrapDataSource
                         */
                        boolean isRestrictChange = isRestrictChange(TAtomDsConfHandle.this.runTimeConf, newConf);

                        if (isNeedFlush) {
                            try {
                                TAtomDsConfHandle.this.runTimeConf = newConf;
                                Properties prop = new Properties();
                                prop.putAll(newConf.getConnectionProperties());
                                TAtomDsConfHandle.this.druidDataSource.setConnectProperties(prop);
                                TAtomDsConfHandle.this.druidDataSource.setMinIdle(newConf.getMinPoolSize());
                                TAtomDsConfHandle.this.druidDataSource.setMaxActive(newConf.getMaxPoolSize());
                                if (newConf.getPreparedStatementCacheSize() > 0
                                    && TAtomDbTypeEnum.MYSQL != newConf.getDbTypeEnum()) {
                                    TAtomDsConfHandle.this.druidDataSource.setPoolPreparedStatements(true);
                                    TAtomDsConfHandle.this.druidDataSource.setMaxPoolPreparedStatementPerConnectionSize(newConf.getPreparedStatementCacheSize());
                                }
                                if (newConf.getIdleTimeout() > 0) {
                                    TAtomDsConfHandle.this.druidDataSource.setTimeBetweenEvictionRunsMillis(newConf.getIdleTimeout() * 60 * 1000);
                                    TAtomDsConfHandle.this.druidDataSource.setMinEvictableIdleTimeMillis(newConf.getIdleTimeout() * 60 * 1000);
                                }
                                if (newConf.getBlockingTimeout() > 0) {
                                    TAtomDsConfHandle.this.druidDataSource.setMaxWait(newConf.getBlockingTimeout());
                                }
                                logger.info("[TDDL DRUID] flush ds success,dataId : " + dataId);
                                clearDataSourceWrapper();
                            } catch (Exception e) {
                                logger.error("[TDDL DRUID] flush DataSource Error ! dataId:" + dataId + ",data:"
                                             + appConfStr, e);
                            }
                        } else if (isRestrictChange) {
                            TAtomDsConfHandle.this.runTimeConf = newConf;
                            clearDataSourceWrapper();
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }

            private boolean isAppChangeNeedReCreate(TAtomDsConfDO runConf, TAtomDsConfDO newConf) {
                if (!newConf.getDriverClass().equals(runConf.getDriverClass())) {
                    return true;
                }

                if (TAtomDbTypeEnum.ORACLE == newConf.getDbTypeEnum()) {
                    Map<String, String> newProp = newConf.getConnectionProperties();
                    Map<String, String> runProp = runConf.getConnectionProperties();
                    // oracle的连接参数变化会导致
                    if (!runProp.equals(newProp)) {
                        return true;
                    }
                }

                if (!TStringUtil.equals(runConf.getUserName(), newConf.getUserName())) {
                    return true;
                }

                if (runConf.getOracleConType() != newConf.getOracleConType()) {
                    return true;
                }

                if (runConf.getDbTypeEnum() != newConf.getDbTypeEnum()) {
                    return true;
                }

                return false;
            }

            private boolean isAppChangeNeedFlush(TAtomDsConfDO runConf, TAtomDsConfDO newConf) {
                if (TAtomDbTypeEnum.MYSQL == newConf.getDbTypeEnum()) {
                    Map<String, String> newProp = newConf.getConnectionProperties();
                    Map<String, String> runProp = runConf.getConnectionProperties();
                    if (!runProp.equals(newProp)) {
                        return true;
                    }
                }

                if (!TStringUtil.equals(runConf.getPasswd(), newConf.getPasswd())) {
                    return true;
                }

                if (runConf.getMinPoolSize() != newConf.getMinPoolSize()) {
                    return true;
                }

                if (runConf.getMaxPoolSize() != newConf.getMaxPoolSize()) {
                    return true;
                }

                if (runConf.getIdleTimeout() != newConf.getIdleTimeout()) {
                    return true;
                }

                if (runConf.getBlockingTimeout() != newConf.getBlockingTimeout()) {
                    return true;
                }

                if (runConf.getPreparedStatementCacheSize() != newConf.getPreparedStatementCacheSize()) {
                    return true;
                }

                return false;
            }

            private boolean isRestrictChange(TAtomDsConfDO runConf, TAtomDsConfDO newConf) {
                if (runConf.getReadRestrictTimes() != newConf.getReadRestrictTimes()) {
                    return true;
                }

                if (runConf.getWriteRestrictTimes() != newConf.getWriteRestrictTimes()) {
                    return true;
                }

                if (runConf.getThreadCountRestrict() != newConf.getThreadCountRestrict()) {
                    return true;
                }

                if (runConf.getTimeSliceInMillis() != newConf.getTimeSliceInMillis()) {
                    return true;
                }

                List<ConnRestrictEntry> runEntries = runConf.getConnRestrictEntries();
                List<ConnRestrictEntry> newEntries = newConf.getConnRestrictEntries();
                if (runEntries != newEntries) {
                    if (runEntries == null || newEntries == null || !newEntries.equals(runEntries)) {
                        return true;
                    }
                }
                return false;
            }
        });
    }

    /**
     * druid特殊开关，针对特殊编码转换，目前是B2B中文站专用
     * 
     * @param connectionProperties
     * @param druidDataSource
     * @throws SQLException
     */
    public static void fillDruidFilters(Map<String, String> connectionProperties, DruidDataSource druidDataSource)
                                                                                                                  throws SQLException {
        if (connectionProperties.containsKey("clientEncoding") || connectionProperties.containsKey("serverEncoding")) {
            druidDataSource.setFilters(TDDL_DRUID_ENCODING_FILTER);
        }
    }

    private static final String TDDL_DRUID_ENCODING_FILTER = "encoding";

    // private static final String DEFAULT_TDDL_DRUID_FILTERS="mergeStat";

    /**
     * 将TAtomDsConfDO转换成LocalTxDataSourceDO
     * 
     * @param tAtomDsConfDO
     * @return
     */
    @SuppressWarnings("rawtypes")
    public static DruidDataSource convertTAtomDsConf2DruidConf(String dbKey, TAtomDsConfDO tAtomDsConfDO, String dbName)
                                                                                                                        throws Exception {
        DruidDataSource localDruidDataSource = new DruidDataSource();
        // 一下三个是druid监控需要的特殊配置
        localDruidDataSource.setName(dbKey);
        localDruidDataSource.setTestOnBorrow(false);
        localDruidDataSource.setTestWhileIdle(true);

        // localDruidDataSource.setFilters(DEFAULT_TDDL_DRUID_FILTERS);
        localDruidDataSource.setUsername(tAtomDsConfDO.getUserName());
        localDruidDataSource.setPassword(tAtomDsConfDO.getPasswd());
        localDruidDataSource.setDriverClassName(tAtomDsConfDO.getDriverClass());
        localDruidDataSource.setExceptionSorterClassName(tAtomDsConfDO.getSorterClass());
        // 根据数据库类型设置conURL和setConnectionProperties
        if (TAtomDbTypeEnum.ORACLE == tAtomDsConfDO.getDbTypeEnum()) {
            String conUlr = TAtomConURLTools.getOracleConURL(tAtomDsConfDO.getIp(),
                tAtomDsConfDO.getPort(),
                tAtomDsConfDO.getDbName(),
                tAtomDsConfDO.getOracleConType());
            localDruidDataSource.setUrl(conUlr);
            // 如果是oracle没有设置ConnectionProperties则给以个默认的
            Properties connectionProperties = new Properties();
            if (!tAtomDsConfDO.getConnectionProperties().isEmpty()) {
                connectionProperties.putAll(tAtomDsConfDO.getConnectionProperties());
                fillDruidFilters(tAtomDsConfDO.getConnectionProperties(), localDruidDataSource);
            } else {
                connectionProperties.putAll(TAtomConstants.DEFAULT_ORACLE_CONNECTION_PROPERTIES);
            }
            localDruidDataSource.setConnectProperties(connectionProperties);
            localDruidDataSource.setValidationQuery(TAtomConstants.DEFAULT_DRUID_ORACLE_VALIDATION_QUERY);
        } else if (TAtomDbTypeEnum.MYSQL == tAtomDsConfDO.getDbTypeEnum()) {
            String conUlr = TAtomConURLTools.getMySqlConURL(tAtomDsConfDO.getIp(),
                tAtomDsConfDO.getPort(),
                tAtomDsConfDO.getDbName(),
                tAtomDsConfDO.getConnectionProperties());
            localDruidDataSource.setUrl(conUlr);
            // 如果可以找到mysqlDriver中的Valid就使用，否则不设置valid
            String validConnnectionCheckerClassName = TAtomConstants.DEFAULT_DRUID_MYSQL_VALID_CONNECTION_CHECKERCLASS;
            try {
                Class.forName(validConnnectionCheckerClassName);
                localDruidDataSource.setValidConnectionCheckerClassName(validConnnectionCheckerClassName);
            } catch (ClassNotFoundException e) {
                logger.warn("MYSQL Driver is Not Suport " + validConnnectionCheckerClassName);
            } catch (NoClassDefFoundError e) {
                logger.warn("MYSQL Driver is Not Suport " + validConnnectionCheckerClassName);
            }

            // 如果可以找到mysqlDriver中的integrationSorter就使用否则使用默认的
            String integrationSorterCalssName = TAtomConstants.DRUID_MYSQL_INTEGRATION_SORTER_CLASS;
            String defaultIntegrationSorterCalssName = TAtomConstants.DEFAULT_DRUID_MYSQL_SORTER_CLASS;
            try {
                Class integrationSorterCalss = Class.forName(integrationSorterCalssName);
                if (null != integrationSorterCalss) {
                    localDruidDataSource.setExceptionSorterClassName(integrationSorterCalssName);
                } else {
                    localDruidDataSource.setExceptionSorterClassName(defaultIntegrationSorterCalssName);
                    logger.warn("MYSQL Driver is Not Suport " + integrationSorterCalssName + " use default sorter "
                                + defaultIntegrationSorterCalssName);
                }
            } catch (ClassNotFoundException e) {
                logger.warn("MYSQL Driver is Not Suport " + integrationSorterCalssName + " use default sorter "
                            + defaultIntegrationSorterCalssName);
                localDruidDataSource.setExceptionSorterClassName(defaultIntegrationSorterCalssName);
            } catch (NoClassDefFoundError e) {
                logger.warn("MYSQL Driver is Not Suport " + integrationSorterCalssName + " use default sorter "
                            + defaultIntegrationSorterCalssName);
                localDruidDataSource.setExceptionSorterClassName(defaultIntegrationSorterCalssName);
            }
            localDruidDataSource.setValidationQuery(TAtomConstants.DEFAULT_DRUID_MYSQL_VALIDATION_QUERY);
        }
        // lazy init 先设置为0 后续真正执行时才创建连接
        localDruidDataSource.setInitialSize(tAtomDsConfDO.getInitPoolSize());
        localDruidDataSource.setMinIdle(tAtomDsConfDO.getMinPoolSize());
        localDruidDataSource.setMaxActive(tAtomDsConfDO.getMaxPoolSize());
        if (tAtomDsConfDO.getPreparedStatementCacheSize() > 0 && TAtomDbTypeEnum.MYSQL != tAtomDsConfDO.getDbTypeEnum()) {
            localDruidDataSource.setPoolPreparedStatements(true);
            localDruidDataSource.setMaxPoolPreparedStatementPerConnectionSize(tAtomDsConfDO.getPreparedStatementCacheSize());
        }
        if (tAtomDsConfDO.getIdleTimeout() > 0) {
            localDruidDataSource.setTimeBetweenEvictionRunsMillis(tAtomDsConfDO.getIdleTimeout() * 60 * 1000);
            localDruidDataSource.setMinEvictableIdleTimeMillis(tAtomDsConfDO.getIdleTimeout() * 60 * 1000);
        }
        if (tAtomDsConfDO.getBlockingTimeout() > 0) {
            localDruidDataSource.setMaxWait(tAtomDsConfDO.getBlockingTimeout());
        }

        // 添加druid日志输出
        // DruidDataSourceStatLogger
        // logger=localDruidDataSource.getStatLogger();
        // logger.setLogger(new Log4jImpl(LoggerInit.TDDL_Atom_Statistic_LOG));
        // localDruidDataSource.setTimeBetweenLogStatsMillis(DruidDsConfHandle.druidFlushIntervalMill);

        return localDruidDataSource;
    }

    public static boolean checkLocalTxDataSourceDO(DruidDataSource druidDataSource) {
        if (null == druidDataSource) {
            return false;
        }

        if (TStringUtil.isBlank(druidDataSource.getUrl())) {
            logger.error("[DsConfig Check] URL is Empty !");
            return false;
        }

        if (TStringUtil.isBlank(druidDataSource.getUsername())) {
            logger.error("[DsConfig Check] Username is Empty !");
            return false;
        }

        if (TStringUtil.isBlank(druidDataSource.getPassword())) {
            logger.error("[DsConfig Check] Password is Empty !");
            return false;
        }

        if (TStringUtil.isBlank(druidDataSource.getDriverClassName())) {
            logger.error("[DsConfig Check] DriverClassName is Empty !");
            return false;
        }

        if (druidDataSource.getMinIdle() < 1) {
            logger.error("[DsConfig Check] MinIdle Error size is:" + druidDataSource.getMinIdle());
            return false;
        }

        if (druidDataSource.getMaxActive() < 1) {
            logger.error("[DsConfig Check] MaxActive Error size is:" + druidDataSource.getMaxActive());
            return false;
        }

        if (druidDataSource.getMinIdle() > druidDataSource.getMaxActive()) {
            logger.error("[DsConfig Check] MinPoolSize Over MaxPoolSize Minsize is:" + druidDataSource.getMinIdle()
                         + "MaxSize is :" + druidDataSource.getMaxActive());
            return false;
        }
        return true;
    }

    /**
     * 是用本地配置覆盖传入的TAtomDsConfDO的属性
     * 
     * @param tAtomDsConfDO
     */
    private void overConfByLocal(TAtomDsConfDO localDsConfDO, TAtomDsConfDO newDsConfDO) {
        if (null == newDsConfDO || null == localDsConfDO) {
            return;
        }
        // 允许设置driverClass
        // if (StringUtil.isNotBlank(localDsConfDO.getDriverClass())) {
        // newDsConfDO.setDriverClass(localDsConfDO.getDriverClass());
        // }
        if (TStringUtil.isNotBlank(localDsConfDO.getSorterClass())) {
            newDsConfDO.setSorterClass(localDsConfDO.getSorterClass());
        }
        if (TStringUtil.isNotBlank(localDsConfDO.getPasswd())) {
            newDsConfDO.setPasswd(localDsConfDO.getPasswd());
        }
        if (null != localDsConfDO.getConnectionProperties() && !localDsConfDO.getConnectionProperties().isEmpty()) {
            newDsConfDO.setConnectionProperties(localDsConfDO.getConnectionProperties());
        }
    }

    /**
     * Datasource 的包装类
     */
    private volatile TDataSourceWrapper wrapDataSource = null;

    public DataSource getDataSource() throws SQLException {
        if (wrapDataSource == null) {
            lock.lock();
            try {
                if (wrapDataSource != null) {
                    // 双检查锁
                    return wrapDataSource;
                }
                String errorMsg = "";
                if (null == druidDataSource) {
                    errorMsg = "[InitError] TAtomDsConfHandle maybe forget init !";
                    logger.error(errorMsg);
                    throw new SQLException(errorMsg);
                }
                DataSource dataSource = druidDataSource;
                if (null == dataSource) {
                    errorMsg = "[InitError] TAtomDsConfHandle maybe init fail !";
                    logger.error(errorMsg);
                    throw new SQLException(errorMsg);
                }
                // 如果数据库状态不可用直接抛出异常
                if (null == this.getStatus()) {
                    errorMsg = "[DB Stats Error] DbStatus is Null: " + this.getDbKey();
                    logger.error(errorMsg);
                    throw new SQLException(errorMsg);
                }
                TDataSourceWrapper tDataSourceWrapper = new TDataSourceWrapper(dataSource, runTimeConf);
                tDataSourceWrapper.setDatasourceName(dbKey);
                tDataSourceWrapper.setDatasourceIp(runTimeConf.getIp());
                tDataSourceWrapper.setDatasourcePort(runTimeConf.getPort());
                tDataSourceWrapper.setDatasourceRealDbName(runTimeConf.getDbName());
                tDataSourceWrapper.setDbStatus(getStatus());
                logger.warn("set datasource key: " + dbKey);
                tDataSourceWrapper.init();
                wrapDataSource = tDataSourceWrapper;

                return wrapDataSource;

            } finally {
                lock.unlock();
            }
        } else {
            return wrapDataSource;
        }
    }

    public void flushDataSource() {
        // 暂时不支持flush 抛错
        logger.error("DRUID DATASOURCE DO NOT SUPPORT FLUSH.");
        throw new RuntimeException("DRUID DATASOURCE DO NOT SUPPORT FLUSH.");
    }

    public void destroyDataSource() throws Exception {
        if (null != this.druidDataSource) {
            logger.warn("[DataSource Stop] Start!");
            this.druidDataSource.close();
            if (null != this.dbConfManager) {
                this.dbConfManager.stopDbConfManager();
            }
            if (null != this.dbPasswdManager) {
                this.dbPasswdManager.stopDbPasswdManager();
            }
            logger.warn("[DataSource Stop] End!");
        }

    }

    public void setSingleInGroup(boolean isSingleInGroup) {
        this.runTimeConf.setSingleInGroup(isSingleInGroup);
    }

    public void setAppName(String appName) throws AtomAlreadyInitException {
        if (initFalg) {
            throw new AtomAlreadyInitException("[AlreadyInit] couldn't Reset appName !");
        }
        this.appName = appName;
    }

    public void setDbKey(String dbKey) throws AtomAlreadyInitException {
        if (initFalg) {
            throw new AtomAlreadyInitException("[AlreadyInit] couldn't Reset dbKey !");
        }
        this.dbKey = dbKey;
    }

    public void setLocalPasswd(String passwd) throws AtomAlreadyInitException {
        if (initFalg) {
            throw new AtomAlreadyInitException("[AlreadyInit] couldn't Reset passwd !");
        }
        this.localConf.setPasswd(passwd);
    }

    public void setLocalConnectionProperties(Map<String, String> map) throws AtomAlreadyInitException {
        if (initFalg) {
            throw new AtomAlreadyInitException("[AlreadyInit] couldn't Reset connectionProperties !");
        }
        this.localConf.setConnectionProperties(map);
        String driverClass = map.get(TAtomConfParser.APP_DRIVER_CLASS_KEY);
        if (!TStringUtil.isBlank(driverClass)) {
            this.localConf.setDriverClass(driverClass);
        }
    }

    public void setLocalDriverClass(String driverClass) throws AtomAlreadyInitException {
        if (initFalg) {
            throw new AtomAlreadyInitException("[AlreadyInit] couldn't Reset driverClass !");
        }
        this.localConf.setDriverClass(driverClass);
    }

    public void setLocalSorterClass(String sorterClass) throws AtomAlreadyInitException {
        if (initFalg) {
            throw new AtomAlreadyInitException("[AlreadyInit] couldn't Reset sorterClass !");
        }
        this.localConf.setSorterClass(sorterClass);
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public String getAppName() {
        return appName;
    }

    public String getDbKey() {
        return dbKey;
    }

    public TAtomDbStatusEnum getStatus() {
        return this.runTimeConf.getDbStautsEnum();
    }

    public TAtomDbTypeEnum getDbType() {
        return this.runTimeConf.getDbTypeEnum();
    }

    public void setDbStatusListeners(List<AtomDbStatusListener> dbStatusListeners) {
        this.dbStatusListeners = dbStatusListeners;
    }

    private void processDbStatusListener(TAtomDbStatusEnum oldStatus, TAtomDbStatusEnum newStatus) {
        if (null != oldStatus && oldStatus != newStatus) {
            if (null != dbStatusListeners) {
                for (AtomDbStatusListener statusListener : dbStatusListeners) {
                    try {
                        statusListener.handleData(oldStatus, newStatus);
                    } catch (Exception e) {
                        logger.error("[call StatusListenner Error] !", e);
                        continue;
                    }
                }
            }
        }
    }
}
