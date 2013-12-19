package com.taobao.tddl.repo.bdb.spi;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.stereotype.Repository;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.utils.ExceptionErrorCodeUtils;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.common.TransactionConfig;
import com.taobao.tddl.executor.spi.ICommandHandlerFactory;
import com.taobao.tddl.executor.spi.ICursorFactory;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITHLog;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.Relationship;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

//import com.taobao.ustore.thl.ITHLog2Applier;

/**
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class JE_Repository implements IRepository {

    private final static Logger logger = LoggerFactory.getLogger(JE_Repository.class);
    BDBConfig                   config;
    Environment                 env;
    Map<String, ITable>         tables = new ConcurrentHashMap<String, ITable>();
    ICursorFactory              cursorFactoryBDBImp;
    Environment                 env_tmp;
    Durability                  durability;
    Random                      r      = new Random();

    public JE_Repository(){
    }

    public void commonConfig(EnvironmentConfig envConfig, BDBConfig config) {
        System.setProperty("JEMonitor", "true");
        envConfig.setConfigParam(EnvironmentConfig.NODE_MAX_ENTRIES, "256");
        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_EVICT_BYTES, (1024 * 1024 * 2) + "");
        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_NODES_PER_SCAN, "10");
        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_LRU_ONLY, "false");
        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_FORCED_YIELD, "true");
        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_CORE_THREADS, Runtime.getRuntime().availableProcessors()
                                                                         + "");
        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_MAX_THREADS, Runtime.getRuntime().availableProcessors() + "");
        envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, 1024 * 1024 * 200 + "");

        envConfig.setConfigParam(EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE, 1024 * 8 + "");
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_READ_SIZE, 1024 * 1024 + "");
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_MAX_BATCH_FILES, 3 + "");
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, 1024 * 1024 * 200 + "");
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_CACHE_SIZE, "1024");
        envConfig.setConfigParam(EnvironmentConfig.LOG_USE_WRITE_QUEUE, "true");
        envConfig.setConfigParam(EnvironmentConfig.LOG_WRITE_QUEUE_SIZE, 1024 * 1024 * 2 + "");
        // envConfig.setConfigParam(EnvironmentConfig.HALT_ON_COMMIT_AFTER_CHECKSUMEXCEPTION,
        // "true");
        envConfig.setConfigParam(EnvironmentConfig.LOG_ITERATOR_READ_SIZE, 1024 * 8 + "");
        envConfig.setConfigParam(EnvironmentConfig.LOCK_TIMEOUT, 2000 + "\tMILLISECONDS");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RECOVERY_FORCE_CHECKPOINT, "true");

        envConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION, (config.getCleaner_min_utilization()));
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_LAZY_MIGRATION, config.getCleanerLazyMigration());
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS, config.getCleanerThreadCount() + "");
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_MAX_BATCH_FILES, config.getCleanerBatchFileCount() + "");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, config.getAutoClean() ? "true" : "false");

    }

    @Override
    public BDBConfig getRepoConfig() {
        return config;
    }

    @Override
    public Map<String, ITable> getTables() {
        return tables;
    }

    @Override
    public boolean isWriteAble() {
        return true;
    }

    @Override
    public ICursorFactory getCursorFactory() {
        return cursorFactoryBDBImp;
    }

    protected final AtomicReference<ITHLog> historyLog = new AtomicReference<ITHLog>();
    // private static Log logger = LogFactory.getLog(JE_Repository.class);

    protected ICommandHandlerFactory        cef        = null;

    @Override
    public void init() {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        commonConfig(envConfig);
        envConfig.setCachePercent(config.getCachePercent());
        envConfig.setAllowCreate(true);
        if (config.isTransactional()) {
            envConfig.setCachePercent(config.getCachePercent());
            envConfig.setTransactional(config.isTransactional());
            envConfig.setTxnTimeout(config.getTxnTimeout(), TimeUnit.SECONDS);
            this.durability = config.isCommitSync() ? Durability.COMMIT_SYNC : Durability.COMMIT_NO_SYNC;
            envConfig.setDurability(this.durability);
        }
        File repo_dir = new File(config.getRepoDir());
        if (!repo_dir.exists()) {
            repo_dir.mkdirs();
        }
        this.env = new Environment(repo_dir, envConfig);
        cef = new CommandHandlerFactoryBDBImpl();
        cursorFactoryBDBImp = new CursorFactoryBDBImp();

    }

    public void commonConfig(EnvironmentConfig envConfig) {
        System.setProperty("JEMonitor", "true");
        envConfig.setConfigParam(EnvironmentConfig.NODE_MAX_ENTRIES, "256");
        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_EVICT_BYTES, (1024 * 1024 * 2) + "");
        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_NODES_PER_SCAN, "10");
        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_LRU_ONLY, "false");
        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_FORCED_YIELD, "true");
        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_CORE_THREADS, Runtime.getRuntime().availableProcessors()
                                                                         + "");
        envConfig.setConfigParam(EnvironmentConfig.EVICTOR_MAX_THREADS, Runtime.getRuntime().availableProcessors() + "");
        envConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, 1024 * 1024 * 200 + "");
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_LAZY_MIGRATION, "true");
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS, Runtime.getRuntime().availableProcessors() + "");
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE, 1024 * 8 + "");
        envConfig.setConfigParam(EnvironmentConfig.CLEANER_READ_SIZE, 1024 * 1024 + "");
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, 1024 * 1024 * 200 + "");
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_CACHE_SIZE, "1024");
        envConfig.setConfigParam(EnvironmentConfig.LOG_USE_WRITE_QUEUE, "true");
        envConfig.setConfigParam(EnvironmentConfig.LOG_WRITE_QUEUE_SIZE, 1024 * 1024 * 2 + "");
        // envConfig.setConfigParam(EnvironmentConfig.HALT_ON_COMMIT_AFTER_CHECKSUMEXCEPTION,
        // "true");
        envConfig.setConfigParam(EnvironmentConfig.LOG_ITERATOR_READ_SIZE, 1024 * 8 + "");
        envConfig.setConfigParam(EnvironmentConfig.LOCK_TIMEOUT, 2000 + "\tMILLISECONDS");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RECOVERY_FORCE_CHECKPOINT, "true");
    }

    @Override
    public ITable getTable(TableMeta table_schema, String groupNode) throws TddlException {
        ITable table = tables.get(table_schema.getTableName());
        if (table == null) {
            synchronized (this) {
                table = tables.get(table_schema.getTableName());
                if (table == null) {
                    try {
                        table = initTable(table_schema);
                    } catch (ReplicaWriteException ex) {
                        throw new TddlException(ExceptionErrorCodeUtils.Read_only, ex);
                    }
                    if (!table_schema.isTmp()) {
                        tables.put(table_schema.getTableName(), table);
                    }
                }

            }
        }
        return table;
    }

    public ITable initTable(TableMeta table_schema) throws TddlException {

        return new JE_Table(table_schema, this);
    }

    public Database getDatabase(String name, boolean isTmp, boolean isSortedDuplicates) {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        Environment _env = env;
        if (isTmp) {
            dbConfig.setTemporary(true);
            dbConfig.setSortedDuplicates(isSortedDuplicates);
            _env = (Environment) getTmpEnv();
        } else {
            if (!config.isTransactional()) {
                dbConfig.setDeferredWrite(config.isCommitSync());
            } else {
                dbConfig.setTransactional(true);
            }
        }

        Database database = buildPrimaryIndex(dbConfig, _env, name);
        return database;
    }

    public Environment getEnv() {
        return env;
    }

    @Override
    public void close() throws TddlException {
        for (Entry<String, ITable> t : tables.entrySet()) {
            t.getValue().close();
        }
        env.close();
        if (env_tmp != null) {
            env_tmp.close();
        }
    }

    @Override
    public ITransaction beginTransaction(TransactionConfig conf) throws TddlException {
        try {
            com.sleepycat.je.TransactionConfig _conf = new com.sleepycat.je.TransactionConfig();
            _conf.setReadCommitted(conf.getReadCommitted());
            _conf.setDurability(durability);
            return new JE_Transaction(env.beginTransaction(null, _conf), _conf, historyLog);
        } catch (ReplicaWriteException ex) {
            throw new TddlException(ExceptionErrorCodeUtils.Read_only, ex);
        }
    }

    public synchronized Object getTmpEnv() {
        if (env_tmp != null) {
            return env_tmp;
        } else {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setCachePercent(20);
            envConfig.setAllowCreate(true);
            File repo_dir = new File(System.getProperty("user.dir") + "/bdbtmp/" + System.currentTimeMillis()
                                     + r.nextInt() + "requestID." + genRequestID() + "/");
            if (!repo_dir.exists()) {
                repo_dir.mkdirs();
            }
            env_tmp = new Environment(repo_dir, envConfig);
            return env_tmp;
        }
    }

    @Override
    public boolean isEnhanceExecutionModel(String groupKey) {
        return true;
    }

    private Database buildPrimaryIndex(DatabaseConfig dbConfig, Environment _env, String dbName) {
        Database database = _env.openDatabase(null, dbName, dbConfig);
        return database;
    }

    private void buildSecondaryIndex(TableMeta table_schema, Map<String, Database> databases, Environment _env,
                                     Database database, IndexMeta secondaryIndexMeta, String dbName) {
        SecondaryConfig sc = new SecondaryConfig();
        sc.setAllowCreate(true);
        if (table_schema.isTmp()) {
            sc.setTemporary(table_schema.isTmp());
        } else {
            if (!config.isTransactional()) {
                sc.setDeferredWrite(config.isCommitSync());
            } else {
                sc.setTransactional(true);
            }
        }
        sc.setSortedDuplicates(true);
        sc.setKeyPrefixing(true);
        if (secondaryIndexMeta.getRelationship() == Relationship.MANY_TO_MANY
            || secondaryIndexMeta.getRelationship() == Relationship.MANY_TO_ONE) {
            sc.setMultiKeyCreator(new IndexKeyCreator(table_schema.getPrimaryIndex(), secondaryIndexMeta));
        } else {
            sc.setKeyCreator(new IndexKeyCreator(table_schema.getPrimaryIndex(), secondaryIndexMeta));
        }
        SecondaryDatabase secondaryIndex = _env.openSecondaryDatabase(null, dbName, database, sc);
        databases.put(dbName, secondaryIndex);
    }

    private static boolean hasSuffixString(String targetIndexName) {
        int size = TStringUtil.lastIndexOf(targetIndexName, "_");

        if (size == -1) {
            return false;
        }

        return true;
    }

    private static String getSuffixString(String targetIndexName) {

        Pattern pattern = Pattern.compile("\\d+$");
        Matcher matcher = pattern.matcher(targetIndexName);
        if (matcher.find()) {
            return (matcher.group());
        } else {
            throw new IllegalArgumentException(targetIndexName + " has no _ . pattern mismatch. recheck ");
        }

    }

    public void setCursorFactoryBDBImp(ICursorFactory cursorFactoryBDBImp) {
        this.cursorFactoryBDBImp = cursorFactoryBDBImp;
    }

    /**
     * 建立连接
     * 
     * @param createTxn
     * @param command
     * @param context
     * @param repo
     * @param executionContext
     * @throws Exception
     */
    public Transaction createTransaction(boolean createTxn, IDataNodeExecutor command, Map<String, Comparable> context,
                                         Repository repo) throws Exception {

        return null;
    }

    // @Override
    public void cleanTempTable() {
        if (env_tmp != null) {
            env_tmp.cleanLog();
        }
    }

    @Override
    public ICommandHandlerFactory getCommandExecutorFactory() {
        return cef;
    }

    public int cleanLog() {
        return env.cleanLog();
    }

    @Override
    public IGroupExecutor buildGroupExecutor(Group group) {
        throw new NotSupportException();
    }

    public void setConfig(BDBConfig config) {
        this.config = config;

    }

    public ITable getTempTable(TableMeta meta) throws TddlException {
        return initTable(meta);
    }

    static AtomicLong currentID = new AtomicLong(0L);

    public static long genRequestID() {
        return currentID.addAndGet(1L);
    }

}
