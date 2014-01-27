package com.taobao.tddl.repo.bdb.spi;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.repo.RepositoryConfig;

/**
 * @author mengshi.sunmengshi 2013-12-19 下午3:08:18
 * @since 5.0.0
 */
public class BDBConfig extends RepositoryConfig {

    public static final String BDB_REPO_CONFIG_FILE_PATH = "BDB_REPO_CONFIG_FILE_PATH";
    // 项目根目录
    private String             root_dir;
    // 序列化/编码
    private String             codec_name;
    // 数据目录
    private String             repo_dir                  = ".";

    // 提交时同步刷到文件系统
    private boolean            commit_sync               = true;

    // 用作缓存的内存的百分比
    private int                cache_percent             = 60;

    // 表定义文件
    // public String schema_file;
    // 机器拓扑
    // public String machineTopology;

    // tddl-rule的配置文件
    // 执行引擎线程数
    private int                execThreadCount           = 50;
    private int                maxThreadCount            = execThreadCount;
    private int                keepAliveTime             = 5000;
    // 是否使用bdb ha
    private boolean            ha                        = false;
    // ha group name
    private String             group_name;
    // ha group node name
    private String             node_name;

    // ha leader选举优先级
    private int                priority                  = 1;
    // group中的所有节点
    private String             group_nodes;
    // rpc 连接权限
    private String             userName;
    private String             password;
    // 持久化策略
    private String[]           durability;

    private int                transactionDelayTime;

    private int                resultDelayTime;
    // 走mysql
    private boolean            useMysql;
    private String             mysqlDB;
    // 精卫的topic

    private String             metaTopic;
    // 精卫映射表
    private String             jingweiMap;

    /**
     * 清理县城count
     */
    public int                 cleaner_thread_count;

    /**
     * 清理者
     */
    public int                 cleaner_batch_file_count;

    public String              cleaner_min_utilization   = "50";
    public boolean             auto_clean;

    public String              cleanerLazyMigration      = "FALSE";

    public String getJingweiMap() {
        return jingweiMap;
    }

    public void setJingweiMap(String jingweiMap) {
        this.jingweiMap = jingweiMap;
    }

    public String[] getDurability() {
        return durability;
    }

    public String getGroupName() {
        return group_name;
    }

    public boolean isHA() {
        return ha;
    }

    public String getNodeName() {
        return node_name;
    }

    public int getPriority() {
        return priority;
    }

    public String getGroupNodes() {
        return group_nodes;
    }

    public String[] getGroupNodesArray() {
        return group_nodes.split(",");
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getBlockingQueueCapacity() {
        return max_concurrent_request;
    }

    public void setBlockingQueueCapacity(int blockingQueueCapacity) {
        this.max_concurrent_request = blockingQueueCapacity;
    }

    public int getKeepAliveTime() {
        return keepAliveTime;
    }

    public void setKeepAliveTime(int keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }

    public int getMaximumPoolSize() {
        return maxThreadCount;
    }

    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maxThreadCount = maximumPoolSize;
    }

    public int getCorePoolSize() {
        return execThreadCount;
    }

    public void setCorePoolSize(int corePoolSize) {
        this.execThreadCount = corePoolSize;
    }

    public int getCachePercent() {
        return cache_percent;
    }

    public BDBConfig(){
    }

    public BDBConfig(String file) throws IOException{
        InputStream in = GeneralUtil.getInputStream(file);
        load(in);
    }

    public BDBConfig(InputStream in) throws IOException{
        load(in);
    }

    private void load(InputStream in) throws IOException {
        Properties p = new Properties();
        p.load(in);
        in.close();
        this.repo_dir = p.getProperty("repo_dir");
        this.codec_name = p.getProperty("codec_name", "avro");
        this.transactional = Boolean.parseBoolean(p.getProperty("transactional", "true"));
        this.commit_sync = Boolean.parseBoolean(p.getProperty("commit_sync", "true"));
        this.txnTimeout = Integer.parseInt(p.getProperty("txn_timeout", "1000"));
        this.resultTimeout = Integer.parseInt(p.getProperty("result_timeout", "60"));
        this.max_concurrent_request = Integer.parseInt(p.getProperty("max_concurrent_request", "100000"));
        this.port = Integer.parseInt(p.getProperty("port", "6033"));
        this.root_dir = p.getProperty("root_dir");
        this.cache_percent = Integer.parseInt(p.getProperty("cache_percent", "60"));
        this.machineTopology = p.getProperty("machine_topology");
        this.schemaFile = p.getProperty("schema_file");
        this.app = p.getProperty("app");
        this.appRuleFile = p.getProperty("app_rule_file");
        String tempMonitorServerPort = p.getProperty("monitor_server_port");
        if (tempMonitorServerPort != null && !tempMonitorServerPort.isEmpty()) {
            this.monitorServerPort = Integer.valueOf(tempMonitorServerPort);
        }
        String allow_get_group_name = p.getProperty("allow_get_group_name");
        if (allow_get_group_name == null) {
            this.allowExecuteOversea = Boolean.parseBoolean(p.getProperty("allow_get_group_name", "true"));
        } else {
            this.allowExecuteOversea = Boolean.parseBoolean(allow_get_group_name);
        }
        this.transactionDelayTime = Integer.valueOf(p.getProperty("transaction_delay_time", "100000"));
        this.resultDelayTime = Integer.valueOf(p.getProperty("result_delay_time", "100000"));
        this.userName = p.getProperty("user_name");
        this.password = p.getProperty("password");
        this.ha = Boolean.parseBoolean(p.getProperty("ha", "false"));
        if (this.ha) {
            this.group_name = p.getProperty("group_name");
            this.node_name = p.getProperty("node_name");
            this.group_nodes = p.getProperty("group_nodes");
            this.priority = Integer.parseInt(p.getProperty("priority", "1"));
            this.durability = p.getProperty("durability", "SYNC,NO_SYNC,SIMPLE_MAJORITY").split(",");
        }

        this.maxThreadCount = Integer.parseInt(p.getProperty("max_thread_count", "50"));
        this.execThreadCount = maxThreadCount;
        if (durability != null) {
            System.out.println("durability:" + Arrays.asList(durability));
        } else {
            System.out.println("durability:null");
        }
        this.default_txn_isolation = p.getProperty("default_txn_degree", "READ_COMMITTED");

        this.sendJingWei = Boolean.parseBoolean(p.getProperty("sendJingWei", "false"));
        this.metaTopic = p.getProperty("metaTopic");
        this.jingweiMap = p.getProperty("jingweiMap");

        this.useMysql = Boolean.parseBoolean(p.getProperty("use_mysql"));

        this.isPerfTest = Boolean.parseBoolean(p.getProperty("isPerfTest", "false"));

        this.mysqlDB = p.getProperty("mysql_DB");

        this.cleaner_thread_count = Integer.parseInt(p.getProperty("cleaner_thread_count", "1"));
        this.cleaner_batch_file_count = Integer.parseInt(p.getProperty("cleaner_batch_file_count", "1"));
        this.auto_clean = Boolean.parseBoolean(p.getProperty("auto_clean", "true"));
        this.cleanerLazyMigration = p.getProperty("cleanerLazyMigration", "false");
        this.cleaner_min_utilization = p.getProperty("cleaner_min_utilization", "50");

    }

    public String getRootDir() {
        return root_dir;
    }

    public String getSchemaDir() {
        return root_dir + "/conf/schemas";
    }

    public void setSchemaDir(String dir) {
        this.root_dir = dir;
    }

    public boolean isCommitSync() {
        return commit_sync;
    }

    public void setCommitSync(boolean commit_sync) {
        this.commit_sync = commit_sync;
    }

    public String getCodecName() {
        return codec_name;
    }

    public String getRepoDir() {
        return repo_dir;
    }

    public void setCodecName(String codecName) {
        this.codec_name = codecName;
    }

    public void setRepoDir(String repo_dir) {
        this.repo_dir = repo_dir;
    }

    public String getSchema_file() {
        return schemaFile;
    }

    public void setSchema_file(String schema_file) {
        this.schemaFile = schema_file;
    }

    public String getMachine_topology() {
        return machineTopology;
    }

    public void setMachine_topology(String machine_topology) {
        this.machineTopology = machine_topology;
    }

    public String getApp_rule_file() {
        return appRuleFile;
    }

    public void setApp_rule_file(String app_rule_file) {
        this.appRuleFile = app_rule_file;
    }

    public boolean isAllow_get_group_name() {
        return allowExecuteOversea;
    }

    public void setAllow_get_group_name(boolean allow_get_group_name) {
        this.allowExecuteOversea = allow_get_group_name;
    }

    public boolean isUseMysql() {
        return useMysql;
    }

    public void setUseMysql(boolean useMysql) {
        this.useMysql = useMysql;
    }

    public String getMysqlDB() {
        return mysqlDB;
    }

    public void setMysqlDB(String mysqlDB) {
        this.mysqlDB = mysqlDB;
    }

    public String getMetaTopic() {
        return metaTopic;
    }

    public String getCleanerLazyMigration() {
        return cleanerLazyMigration;
    }

    public void setCleanerLazyMigration(String cleanerLazyMigration) {
        this.cleanerLazyMigration = cleanerLazyMigration;
    }

    public String getCleaner_min_utilization() {
        return cleaner_min_utilization;
    }

    public void setCleaner_min_utilization(String cleaner_min_utilization) {
        this.cleaner_min_utilization = cleaner_min_utilization;
    }

    public int getCleanerThreadCount() {
        return cleaner_thread_count;
    }

    public int getCleanerBatchFileCount() {
        return cleaner_batch_file_count;
    }

    public boolean getAutoClean() {
        return auto_clean;
    }

    @Override
    public String toString() {
        return "ServerConfig [root_dir=" + root_dir + ", codec_name=" + codec_name + ", repo_dir=" + repo_dir
               + ", commit_sync=" + commit_sync + ", cache_percent=" + cache_percent + ", execThreadCount="
               + execThreadCount + ", maxThreadCount=" + maxThreadCount + ", keepAliveTime=" + keepAliveTime + ", ha="
               + ha + ", group_name=" + group_name + ", node_name=" + node_name + ", priority=" + priority
               + ", group_nodes=" + group_nodes + ", userName=" + userName + ", password=" + password + ", durability="
               + Arrays.toString(durability) + ", transactionDelayTime=" + transactionDelayTime + ", resultDelayTime="
               + resultDelayTime + "]";
    }

    /**
     * 在diamond中的appName,用来拉配置,appName如果存在，则schema_file和machine_topology都无效。互斥的。
     */
    protected String  app;
    /**
     * 秒级，事务超时时间
     */
    protected int     txnTimeout;
    /**
     * 秒级，结果集超时时间
     */
    protected int     resultTimeout;
    /**
     * 是否允许在非本机执行query
     */
    protected boolean allowExecuteOversea    = true;

    /**
     * app文件
     */
    protected String  appRuleFile;

    /**
     * 机器拓扑
     */
    protected String  machineTopology;

    /**
     * 表文件
     */
    protected String  schemaFile;

    // 事务
    protected boolean transactional          = true;
    // defalut transaction isolation
    // degree:{READ_UNCOMMITTED|READ_COMMITTED|REPEATABLE_READ|SERIALIZABLE}
    public String     default_txn_isolation;

    /**
     * 如果本机提供服务，那么应该会对外提供port
     */
    protected int     port                   = 100020;

    protected Integer monitorServerPort      = null;

    /**
     * 内部使用，性能测试的时候，屏蔽真正渎取和写入。
     */
    protected boolean isPerfTest             = false;

    protected boolean sendJingWei            = false;
    // 最大并发请求数
    public int        max_concurrent_request = 100000;

    public String getDefaultTnxIsolation() {
        return default_txn_isolation;
    }

    public void setDefaultTxnIsolation(String isolation) {
        this.default_txn_isolation = isolation;
    }

    public int getTxnTimeout() {
        return txnTimeout;
    }

    public void setTxnTimeout(int txnTimeout) {
        this.txnTimeout = txnTimeout;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public int getResultTimeout() {
        return resultTimeout;
    }

    public Integer getMonitorServerPort() {
        return monitorServerPort;
    }

    public void setMonitorServerPort(Integer monitorServerPort) {
        this.monitorServerPort = monitorServerPort;
    }

    public void setResultTimeout(int resultTimeout) {
        this.resultTimeout = resultTimeout;
    }

    public boolean isAllowExecuteOversea() {
        return allowExecuteOversea;
    }

    public void setAllowExecuteOversea(boolean allowExecuteOversea) {
        this.allowExecuteOversea = allowExecuteOversea;
    }

    public String getAppRuleFile() {
        return appRuleFile;
    }

    public void setAppRuleFile(String appRuleFile) {
        this.appRuleFile = appRuleFile;
    }

    public String getMachineTopology() {
        return machineTopology;
    }

    public void setMachineTopology(String machineTopology) {
        this.machineTopology = machineTopology;
    }

    public String getSchemaFile() {
        return schemaFile;
    }

    public void setSchemaFile(String schemaFile) {
        this.schemaFile = schemaFile;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean isTransactional() {
        return transactional;
    }

    public void setTransactional(boolean transactional) {
        this.transactional = transactional;
    }

    public boolean isSendJingWei() {
        return sendJingWei;
    }

    public void setSendJingWei(boolean sendJingWei) {
        this.sendJingWei = sendJingWei;
    }

    public int getMaxConcurrentRequest() {
        return max_concurrent_request;
    }

    public void getMaxConcurrentRequest(int max_concurrent_request) {
        this.max_concurrent_request = max_concurrent_request;
    }

    public boolean isPerfTest() {
        return isPerfTest;
    }

    public void setPerfTest(boolean isPerfTest) {
        this.isPerfTest = isPerfTest;
    }

}
