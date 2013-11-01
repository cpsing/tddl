package com.taobao.tddl.atom;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import com.taobao.datasource.LocalTxDataSourceDO;
import com.taobao.datasource.TaobaoDataSourceFactory;
import com.taobao.datasource.resource.adapter.jdbc.local.LocalTxDataSource;
import com.taobao.tddl.atom.config.DbConfManager;
import com.taobao.tddl.atom.config.DbPasswdManager;
import com.taobao.tddl.atom.config.DiamondDbConfManager;
import com.taobao.tddl.atom.config.DiamondDbPasswdManager;
import com.taobao.tddl.atom.config.TAtomConfParser;
import com.taobao.tddl.atom.config.TAtomDsConfDO;
import com.taobao.tddl.atom.config.listener.TAtomDbStatusListener;
import com.taobao.tddl.atom.exception.AtomAlreadyInitException;
import com.taobao.tddl.atom.exception.AtomIllegalException;
import com.taobao.tddl.atom.exception.AtomInitialException;
import com.taobao.tddl.atom.jdbc.ConnRestrictEntry;
import com.taobao.tddl.atom.jdbc.TDataSourceWrapper;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.monitor.Monitor;

/**
 * 数据库动态切换的Handle类，所有数据库的动态切换 都是由这个类完成
 * 
 * @author qihao
 */
class TAtomDsConfHandle {

    private static Logger                        logger      = LoggerFactory.getLogger(TAtomDsConfHandle.class);

    private String                               appName;

    private String                               dbKey;

    private String                               unitName;

    /**
     * 运行时配置
     */
    private volatile TAtomDsConfDO               runTimeConf = new TAtomDsConfDO();

    /**
     * 本地配置，优先于推送的动态配置
     */
    private TAtomDsConfDO                        localConf   = new TAtomDsConfDO();

    /**
     * 全局配置，应用配置订阅管理
     */
    private DbConfManager                        dbConfManager;

    /**
     * 密码配置订阅管理
     */
    private DbPasswdManager                      dbPasswdManager;

    /**
     * Jboss数据源通过init初始化
     */
    private volatile LocalTxDataSource           jbossDataSource;

    /**
     * 数据库状态改变回调
     */
    private volatile List<TAtomDbStatusListener> dbStatusListeners;

    /**
     * 初始化标记为一但初始化过，所有本地的配置禁止改动
     */
    private volatile boolean                     initFalg;

    /**
     * 数据源操作锁，当需要对数据源进行重建或者刷新时需要先获得该锁
     */
    private final ReentrantLock                  lock        = new ReentrantLock();

    /**
     * 初始化方法，创建对应的数据源，只能被调用一次
     * 
     * @throws Exception
     */
    protected void init() throws Exception {
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
        DiamondDbConfManager defaultDbConfManager = new DiamondDbConfManager();
        defaultDbConfManager.setGlobalConfigDataId(TAtomConstants.getGlobalDataId(this.dbKey));
        defaultDbConfManager.setAppConfigDataId(TAtomConstants.getAppDataId(this.appName, this.dbKey));
        // 初始化dbConfManager
        defaultDbConfManager.init(appName);
        defaultDbConfManager.setUnitName(unitName);
        dbConfManager = defaultDbConfManager;
        // 3.获取全局配置
        String globaConfStr = dbConfManager.getGlobalDbConf();
        // 注册全局配置监听
        registerGlobaDbConfListener(defaultDbConfManager);
        if (TStringUtil.isBlank(globaConfStr)) {

            String errorMsg = "[ConfError] read globalConfig is Empty ! dataId: "
                              + TAtomConstants.getGlobalDataId(this.dbKey);
            logger.error(errorMsg);
            throw new AtomInitialException(errorMsg);
        }
        // 4.获取应用配置
        String appConfStr = dbConfManager.getAppDbDbConf();
        // 注册应用配置监听
        registerAppDbConfListener(defaultDbConfManager);
        if (TStringUtil.isBlank(appConfStr)) {

            String errorMsg = "[ConfError] read appConfig is Empty ! dataId: "
                              + TAtomConstants.getAppDataId(this.appName, this.dbKey);
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
                DiamondDbPasswdManager diamondDbPasswdManager = new DiamondDbPasswdManager();
                diamondDbPasswdManager.setPasswdConfDataId(TAtomConstants.getPasswdDataId(runTimeConf.getDbName(),
                    runTimeConf.getDbType(),
                    runTimeConf.getUserName()));

                diamondDbPasswdManager.init(appName);
                diamondDbPasswdManager.setUnitName(unitName);
                dbPasswdManager = diamondDbPasswdManager;
                // 获取密码
                String passwd = dbPasswdManager.getPasswd();
                registerPasswdConfListener(diamondDbPasswdManager);
                if (TStringUtil.isBlank(passwd)) {
                    StringBuilder errorMsgBuilder = new StringBuilder();
                    errorMsgBuilder.append("[PasswdError] read passwd is Empty ! dataId: ");
                    errorMsgBuilder.append(TAtomConstants.getPasswdDataId(runTimeConf.getDbName(),
                        runTimeConf.getDbType(),
                        runTimeConf.getUserName()));
                    errorMsgBuilder.append(", maybe passwd parse error, please grep '[parserPasswd Error]' ");

                    logger.error(errorMsgBuilder.toString());
                    throw new AtomInitialException(errorMsgBuilder.toString());
                }
                runTimeConf.setPasswd(passwd);
            }
            // 8.转换tAtomDsConfDO
            LocalTxDataSourceDO localTxDataSourceDO = convertTAtomDsConf2JbossConf(this.runTimeConf,
                TAtomConstants.getDbNameStr(this.appName, this.dbKey));
            // 9.参数检查如果参数不正确直接抛出异常
            if (!checkLocalTxDataSourceDO(localTxDataSourceDO)) {
                String errorMsg = "[ConfigError]init dataSource Prams Error! config is : "
                                  + localTxDataSourceDO.toString();
                logger.error(errorMsg);
                throw new AtomInitialException(errorMsg);
            }
            // 10.创建数据源
            // 关闭TB-DATASOURCE的JMX注册
            localTxDataSourceDO.setUseJmx(false);
            LocalTxDataSource localTxDataSource = TaobaoDataSourceFactory.createLocalTxDataSource(localTxDataSourceDO);
            // 11.将创建好的数据源是指到TAtomDatasource中
            this.jbossDataSource = localTxDataSource;
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
                logger.warn("[Passwd HandleData] handle received password data, dataId : " + dataId + " data: " + data);
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
                        TAtomDsConfHandle.this.jbossDataSource.setPassword(newPasswd);
                        try {
                            // 更新数据源
                            TAtomDsConfHandle.this.flushDataSource();
                            // 是用新的配置覆盖运行时的配置
                            TAtomDsConfHandle.this.runTimeConf.setPasswd(newPasswd);
                        } catch (Exception e) {
                            logger.error("[Flsh Passwd Error] flush dataSource Error !", e);
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
                logger.warn("[GlobaConf HandleData] handle received global data, dataId : " + dataId + " data: " + data);
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
                    if (AtomDbStatusEnum.NA_STATUS != TAtomDsConfHandle.this.runTimeConf.getDbStautsEnum()
                        && AtomDbStatusEnum.NA_STATUS == tmpConf.getDbStautsEnum()) {
                        try {
                            TAtomDsConfHandle.this.jbossDataSource.destroy();
                            logger.warn("[NA STATUS PUSH] destroy DataSource !");
                        } catch (Exception e) {
                            logger.error("[NA STATUS PUSH] destroy DataSource  Error!", e);
                        }
                    } else {
                        // 转换tAtomDsConfDO
                        LocalTxDataSourceDO localTxDataSourceDO = convertTAtomDsConf2JbossConf(newConf,
                            TAtomConstants.getDbNameStr(TAtomDsConfHandle.this.appName, TAtomDsConfHandle.this.dbKey));
                        // 检查转换后结果是否正确
                        if (!checkLocalTxDataSourceDO(localTxDataSourceDO)) {
                            logger.error("[GlobaConfError] dataSource Prams Error! dataId : " + dataId + " config : "
                                         + data);
                            return;
                        }
                        // 如果推送的状态时 NA->RW/R 时需要重新创建数据源，无需再刷新
                        if (TAtomDsConfHandle.this.runTimeConf.getDbStautsEnum() == AtomDbStatusEnum.NA_STATUS
                            && (newConf.getDbStautsEnum() == AtomDbStatusEnum.RW_STATUS
                                || newConf.getDbStautsEnum() == AtomDbStatusEnum.R_STATUS || newConf.getDbStautsEnum() == AtomDbStatusEnum.W_STATUS)) {
                            // 创建数据源
                            try {
                                // 关闭TB-DATASOURCE的JMX注册
                                localTxDataSourceDO.setUseJmx(false);
                                LocalTxDataSource localTxDataSource = TaobaoDataSourceFactory.createLocalTxDataSource(localTxDataSourceDO);
                                TAtomDsConfHandle.this.jbossDataSource = localTxDataSource;
                                logger.warn("[NA->RW/R STATUS PUSH] ReCreate DataSource !");
                            } catch (Exception e) {
                                logger.error("[NA->RW/R STATUS PUSH] ReCreate DataSource Error!", e);
                            }
                        } else {
                            boolean needFlush = checkGlobaConfChange(TAtomDsConfHandle.this.runTimeConf, newConf);
                            // 如果发生的配置变化是否需要重建数据源
                            if (needFlush) {
                                TAtomDsConfHandle.this.jbossDataSource.setConnectionURL(localTxDataSourceDO.getConnectionURL());
                                TAtomDsConfHandle.this.jbossDataSource.setDriverClass(localTxDataSourceDO.getDriverClass());
                                TAtomDsConfHandle.this.jbossDataSource.setExceptionSorterClassName(localTxDataSourceDO.getExceptionSorterClassName());
                                try {
                                    // 更新数据源
                                    TAtomDsConfHandle.this.flushDataSource();
                                } catch (Exception e) {
                                    logger.error("[Flsh GlobaConf Error] flush dataSource Error !", e);
                                }
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

            private boolean checkGlobaConfChange(TAtomDsConfDO runConf, TAtomDsConfDO newConf) {
                boolean needFlush = false;
                if (!TStringUtil.equals(runConf.getIp(), newConf.getIp())) {
                    needFlush = true;
                    return needFlush;
                }
                if (!TStringUtil.equals(runConf.getPort(), newConf.getPort())) {
                    needFlush = true;
                    return needFlush;
                }
                if (!TStringUtil.equals(runConf.getDbName(), newConf.getDbName())) {
                    needFlush = true;
                    return needFlush;
                }
                if (runConf.getDbTypeEnum() != newConf.getDbTypeEnum()) {
                    needFlush = true;
                    return needFlush;
                }
                return needFlush;
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
                logger.warn("[AppConf HandleData] handle received app data, dataId : " + dataId + " data: " + data);
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
                    // 应用连接限制配置
                    newConf.setConnRestrictEntries(tmpConf.getConnRestrictEntries());
                    // 处理本地优先配置
                    overConfByLocal(TAtomDsConfHandle.this.localConf, newConf);
                    // 转换tAtomDsConfDO
                    LocalTxDataSourceDO localTxDataSourceDO = convertTAtomDsConf2JbossConf(newConf,
                        TAtomConstants.getDbNameStr(TAtomDsConfHandle.this.appName, TAtomDsConfHandle.this.dbKey));
                    // 检查转换后结果是否正确
                    if (!checkLocalTxDataSourceDO(localTxDataSourceDO)) {
                        logger.error("[GlobaConfError] dataSource Prams Error! dataId : " + dataId + " config : "
                                     + data);
                        return;
                    }
                    boolean isNeedReCreate = isNeedReCreate(TAtomDsConfHandle.this.runTimeConf, newConf);
                    if (isNeedReCreate) {
                        try {
                            // 这个必须在最前面，否则下次推送可能无法比较出不同而不创建新数据源
                            TAtomDsConfHandle.this.runTimeConf = newConf;
                            TAtomDsConfHandle.this.jbossDataSource.destroy();
                            logger.warn("[destroy OldDataSource] dataId : " + dataId);
                            LocalTxDataSource localTxDataSource = TaobaoDataSourceFactory.createLocalTxDataSource(localTxDataSourceDO);
                            logger.warn("[create newDataSource] dataId : " + dataId);
                            TAtomDsConfHandle.this.jbossDataSource = localTxDataSource;
                            clearDataSourceWrapper();
                        } catch (Exception e) {
                            logger.error("[Flsh AppConf Error] reCreate dataSource Error ! dataId: " + dataId, e);
                        }
                    } else {
                        boolean isNeedFlush = isNeedFlush(TAtomDsConfHandle.this.runTimeConf, newConf);
                        /**
                         * 阀值变化无需刷新持有的数据源，只要更新runTimeConf，并且清空wrapDataSource
                         */
                        boolean isRestrictChange = isRestrictChange(TAtomDsConfHandle.this.runTimeConf, newConf);
                        if (isNeedFlush) {
                            TAtomDsConfHandle.this.jbossDataSource.setConnectionURL(localTxDataSourceDO.getConnectionURL());
                            TAtomDsConfHandle.this.jbossDataSource.setUserName(localTxDataSourceDO.getUserName());
                            try {
                                // 是用新的配置覆盖运行时的配置
                                TAtomDsConfHandle.this.runTimeConf = newConf;
                                // 更新数据源
                                TAtomDsConfHandle.this.flushDataSource();
                                clearDataSourceWrapper();
                            } catch (Exception e) {
                                logger.error("[Flash GlobaConf Error] flush dataSource Error !", e);
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

            private boolean isNeedReCreate(TAtomDsConfDO runConf, TAtomDsConfDO newConf) {
                boolean needReCreate = false;
                if (!newConf.getDriverClass().equals(runConf.getDriverClass())) {
                    return true;
                }

                if (AtomDbTypeEnum.ORACLE == newConf.getDbTypeEnum()) {
                    Map<String, String> newProp = newConf.getConnectionProperties();
                    Map<String, String> runProp = runConf.getConnectionProperties();
                    if (!runProp.equals(newProp)) {
                        return true;
                    }
                }
                if (runConf.getMinPoolSize() != newConf.getMinPoolSize()) {
                    return true;
                }
                if (runConf.getMaxPoolSize() != newConf.getMaxPoolSize()) {
                    return true;
                }
                if (runConf.getBlockingTimeout() != newConf.getBlockingTimeout()) {
                    return true;
                }
                if (runConf.getIdleTimeout() != newConf.getIdleTimeout()) {
                    return true;
                }
                if (runConf.getPreparedStatementCacheSize() != newConf.getPreparedStatementCacheSize()) {
                    return true;
                }
                return needReCreate;
            }

            private boolean isNeedFlush(TAtomDsConfDO runConf, TAtomDsConfDO newConf) {
                boolean needFlush = false;
                if (AtomDbTypeEnum.MYSQL == newConf.getDbTypeEnum()) {
                    Map<String, String> newProp = newConf.getConnectionProperties();
                    Map<String, String> runProp = runConf.getConnectionProperties();
                    if (!runProp.equals(newProp)) {
                        return true;
                    }
                }
                if (!TStringUtil.equals(runConf.getUserName(), newConf.getUserName())) {
                    return true;
                }
                if (!TStringUtil.equals(runConf.getPasswd(), newConf.getPasswd())) {
                    return true;
                }
                return needFlush;
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

    protected static Class<?> findClass(String className) {
        Class<?> foundClass = null;
        try {
            foundClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            if (logger.isInfoEnabled()) {
                logger.info("Class not found: " + className);
            }
        } catch (NoClassDefFoundError e) {
            if (logger.isInfoEnabled()) {
                logger.info("No class def: " + className);
            }
        }
        return foundClass;
    }

    /**
     * 将TAtomDsConfDO转换成LocalTxDataSourceDO
     * 
     * @param tAtomDsConfDO
     * @return
     */
    @SuppressWarnings("rawtypes")
    protected static LocalTxDataSourceDO convertTAtomDsConf2JbossConf(TAtomDsConfDO tAtomDsConfDO, String dbName) {
        LocalTxDataSourceDO localTxDataSourceDO = new LocalTxDataSourceDO();
        if (TStringUtil.isNotBlank(dbName)) {
            localTxDataSourceDO.setJndiName(dbName);
        }
        localTxDataSourceDO.setUserName(tAtomDsConfDO.getUserName());
        localTxDataSourceDO.setPassword(tAtomDsConfDO.getPasswd());
        localTxDataSourceDO.setDriverClass(tAtomDsConfDO.getDriverClass());
        localTxDataSourceDO.setExceptionSorterClassName(tAtomDsConfDO.getSorterClass());
        // 根据数据库类型设置conURL和setConnectionProperties
        if (AtomDbTypeEnum.ORACLE == tAtomDsConfDO.getDbTypeEnum()) {
            String conUlr = TAtomConURLTools.getOracleConURL(tAtomDsConfDO.getIp(),
                tAtomDsConfDO.getPort(),
                tAtomDsConfDO.getDbName(),
                tAtomDsConfDO.getOracleConType());
            localTxDataSourceDO.setConnectionURL(conUlr);
            // 如果是oracle没有设置ConnectionProperties则给以个默认的
            if (!tAtomDsConfDO.getConnectionProperties().isEmpty()) {
                localTxDataSourceDO.setConnectionProperties(tAtomDsConfDO.getConnectionProperties());
            } else {
                localTxDataSourceDO.setConnectionProperties(TAtomConstants.DEFAULT_ORACLE_CONNECTION_PROPERTIES);
            }
        } else if (AtomDbTypeEnum.MYSQL == tAtomDsConfDO.getDbTypeEnum()) {
            String conUlr = TAtomConURLTools.getMySqlConURL(tAtomDsConfDO.getIp(),
                tAtomDsConfDO.getPort(),
                tAtomDsConfDO.getDbName(),
                tAtomDsConfDO.getConnectionProperties());
            localTxDataSourceDO.setConnectionURL(conUlr);
            // 如果可以找到mysqlDriver中的Valid就使用，否则设置默认的
            Class validConnectionClass = findClass(TAtomConstants.MYSQL_VALID_CONNECTION_CHECKERCLASS);
            if (null == validConnectionClass) {
                logger.warn("MYSQL Driver is NOT support valid connection checker "
                            + TAtomConstants.MYSQL_VALID_CONNECTION_CHECKERCLASS + ", use default "
                            + TAtomConstants.DEFAULT_MYSQL_VALID_CHECKERCLASS);
                validConnectionClass = findClass(TAtomConstants.DEFAULT_MYSQL_VALID_CHECKERCLASS);
            }
            if (null != validConnectionClass) {
                localTxDataSourceDO.setValidConnectionCheckerClassName(validConnectionClass.getName());
            } else {
                logger.error("NOT found default valid connection checker "
                             + TAtomConstants.DEFAULT_MYSQL_VALID_CHECKERCLASS);
            }
            // 如果可以找到mysqlDriver中的integrationSorter就使用否则使用默认的
            Class integrationSorterClass = findClass(TAtomConstants.MYSQL_INTEGRATION_SORTER_CLASS);
            if (null == integrationSorterClass) {
                logger.warn("MYSQL Driver is NOT support integration sorter "
                            + TAtomConstants.MYSQL_INTEGRATION_SORTER_CLASS + " use default sorter "
                            + TAtomConstants.DEFAULT_MYSQL_SORTER_CLASS);
                integrationSorterClass = findClass(TAtomConstants.DEFAULT_MYSQL_SORTER_CLASS);
            }
            if (null != integrationSorterClass) {
                localTxDataSourceDO.setExceptionSorterClassName(integrationSorterClass.getName());
            } else {
                logger.error("NOT found default integration sorter " + TAtomConstants.DEFAULT_MYSQL_SORTER_CLASS);
            }
        }
        localTxDataSourceDO.setMinPoolSize(tAtomDsConfDO.getMinPoolSize());
        localTxDataSourceDO.setMaxPoolSize(tAtomDsConfDO.getMaxPoolSize());
        localTxDataSourceDO.setPreparedStatementCacheSize(tAtomDsConfDO.getPreparedStatementCacheSize());
        if (tAtomDsConfDO.getIdleTimeout() > 0) {
            localTxDataSourceDO.setIdleTimeoutMinutes(tAtomDsConfDO.getIdleTimeout());
        }
        if (tAtomDsConfDO.getBlockingTimeout() > 0) {
            localTxDataSourceDO.setBlockingTimeoutMillis(tAtomDsConfDO.getBlockingTimeout());
        }
        return localTxDataSourceDO;
    }

    protected static boolean checkLocalTxDataSourceDO(LocalTxDataSourceDO localTxDataSourceDO) {
        if (null == localTxDataSourceDO) {
            return false;
        }

        if (TStringUtil.isBlank(localTxDataSourceDO.getConnectionURL())) {
            logger.error("[DsConfig Check] ConnectionURL is Empty !");
            return false;
        }

        if (TStringUtil.isBlank(localTxDataSourceDO.getUserName())) {
            logger.error("[DsConfig Check] UserName is Empty !");
            return false;
        }

        if (TStringUtil.isBlank(localTxDataSourceDO.getPassword())) {
            logger.error("[DsConfig Check] Password is Empty !");
            return false;
        }

        if (TStringUtil.isBlank(localTxDataSourceDO.getDriverClass())) {
            logger.error("[DsConfig Check] DriverClass is Empty !");
            return false;
        }

        if (localTxDataSourceDO.getMinPoolSize() < 1) {
            logger.error("[DsConfig Check] MinPoolSize Error size is:" + localTxDataSourceDO.getMinPoolSize());
            return false;
        }

        if (localTxDataSourceDO.getMaxPoolSize() < 1) {
            logger.error("[DsConfig Check] MaxPoolSize Error size is:" + localTxDataSourceDO.getMaxPoolSize());
            return false;
        }

        if (localTxDataSourceDO.getMinPoolSize() > localTxDataSourceDO.getMaxPoolSize()) {
            logger.error("[DsConfig Check] MinPoolSize Over MaxPoolSize Minsize is:"
                         + localTxDataSourceDO.getMinPoolSize() + "MaxSize is :" + localTxDataSourceDO.getMaxPoolSize());
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
        // 允许设置driver
        // if (TStringUtil.isNotBlank(localDsConfDO.getDriverClass())) {
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
                if (null == jbossDataSource) {
                    errorMsg = "[InitError] TAtomDsConfHandle maybe forget init !";
                    logger.error(errorMsg);
                    throw new SQLException(errorMsg);
                }
                DataSource dataSource = jbossDataSource.getDatasource();
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
        if (null != this.jbossDataSource) {
            logger.warn("[DataSource Flush] Start!");
            this.jbossDataSource.flush();
            logger.warn("[DataSource Flush] End!");
        }
    }

    protected void destroyDataSource() throws Exception {
        if (null != this.jbossDataSource) {
            logger.warn("[DataSource Stop] Start!");
            this.jbossDataSource.destroy();
            if (null != this.dbConfManager) {
                this.dbConfManager.stopDbConfManager();
            }
            if (null != this.dbPasswdManager) {
                this.dbPasswdManager.stopDbPasswdManager();
            }
            logger.warn("[DataSource Stop] End!");
        }

    }

    void setSingleInGroup(boolean isSingleInGroup) {
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

    public AtomDbStatusEnum getStatus() {
        return this.runTimeConf.getDbStautsEnum();
    }

    public AtomDbTypeEnum getDbType() {
        return this.runTimeConf.getDbTypeEnum();
    }

    public void setDbStatusListeners(List<TAtomDbStatusListener> dbStatusListeners) {
        this.dbStatusListeners = dbStatusListeners;
    }

    private void processDbStatusListener(AtomDbStatusEnum oldStatus, AtomDbStatusEnum newStatus) {
        if (null != oldStatus && oldStatus != newStatus) {
            if (null != dbStatusListeners) {
                for (TAtomDbStatusListener statusListener : dbStatusListeners) {
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
