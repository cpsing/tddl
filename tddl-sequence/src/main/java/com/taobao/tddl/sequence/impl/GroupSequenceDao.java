package com.taobao.tddl.sequence.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.GroupDataSourceRouteHelper;
import com.taobao.tddl.group.jdbc.TGroupDataSource;
import com.taobao.tddl.monitor.eagleeye.EagleeyeHelper;
import com.taobao.tddl.sequence.SequenceDao;
import com.taobao.tddl.sequence.SequenceRange;
import com.taobao.tddl.sequence.exception.SequenceException;
import com.taobao.tddl.sequence.util.RandomSequence;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author JIECHEN 2013-10-31 下午5:48:48
 * @since 5.0.0
 */
public class GroupSequenceDao implements SequenceDao {

    private static final Logger       logger                           = LoggerFactory.getLogger(GroupSequenceDao.class);
    // private static final int MIN_STEP = 1;
    // private static final int MAX_STEP = 100000;

    private static final int          DEFAULT_INNER_STEP               = 1000;

    private static final int          DEFAULT_RETRY_TIMES              = 2;

    private static final String       DEFAULT_TABLE_NAME               = "sequence";
    private static final String       DEFAULT_TEMP_TABLE_NAME          = "sequence_temp";

    private static final String       DEFAULT_NAME_COLUMN_NAME         = "name";
    private static final String       DEFAULT_VALUE_COLUMN_NAME        = "value";
    private static final String       DEFAULT_GMT_MODIFIED_COLUMN_NAME = "gmt_modified";

    private static final int          DEFAULT_DSCOUNT                  = 2;                                              // 默认
    private static final Boolean      DEFAULT_ADJUST                   = false;

    protected static final long       DELTA                            = 100000000L;

    /**
     * 应用名
     */
    protected String                  appName;

    /**
     * group阵列
     */
    protected List<String>            dbGroupKeys;

    protected List<String>            oriDbGroupKeys;

    /**
     * 数据源
     */
    protected Map<String, DataSource> dataSourceMap;

    /**
     * 自适应开关
     */
    protected boolean                 adjust                           = DEFAULT_ADJUST;
    /**
     * 重试次数
     */
    protected int                     retryTimes                       = DEFAULT_RETRY_TIMES;

    /**
     * 数据源个数
     */
    protected int                     dscount                          = DEFAULT_DSCOUNT;

    /**
     * 内步长
     */
    protected int                     innerStep                        = DEFAULT_INNER_STEP;

    /**
     * 外步长
     */
    protected int                     outStep                          = DEFAULT_INNER_STEP;

    /**
     * 序列所在的表名
     */
    protected String                  tableName                        = DEFAULT_TABLE_NAME;

    protected String                  switchTempTable                  = DEFAULT_TEMP_TABLE_NAME;

    private String                    TEST_TABLE_PREFIX                = "__test_";
    // 全链路压测对应sequence表的影子表
    protected String                  testTableName                    = TEST_TABLE_PREFIX + tableName;
    // 全链路压测对应sequence_temp表的影子表
    protected String                  testSwitchTempTable              = TEST_TABLE_PREFIX + switchTempTable;

    /**
     * 存储序列名称的列名
     */
    protected String                  nameColumnName                   = DEFAULT_NAME_COLUMN_NAME;

    /**
     * 存储序列值的列名
     */
    protected String                  valueColumnName                  = DEFAULT_VALUE_COLUMN_NAME;

    /**
     * 存储序列最后更新时间的列名
     */
    protected String                  gmtModifiedColumnName            = DEFAULT_GMT_MODIFIED_COLUMN_NAME;

    /**
     * 初试化
     * 
     * @throws SequenceException
     */
    public void init() throws SequenceException {
        // 如果应用名为空，直接抛出
        if (StringUtils.isEmpty(appName)) {
            SequenceException sequenceException = new SequenceException("appName is Null ");
            logger.error("没有配置appName", sequenceException);
            throw sequenceException;
        }
        if (dbGroupKeys == null || dbGroupKeys.size() == 0) {
            logger.error("没有配置dbgroupKeys");
            throw new SequenceException("dbgroupKeys为空！");
        }

        dataSourceMap = new HashMap<String, DataSource>();
        for (String dbGroupKey : dbGroupKeys) {
            if (dbGroupKey.toUpperCase().endsWith("-OFF")) {
                continue;
            }
            // TGroupDataSource tGroupDataSource = new TGroupDataSource(
            // dbGroupKey, appName, dataSourceType);
            TGroupDataSource tGroupDataSource = new TGroupDataSource(dbGroupKey, appName);
            tGroupDataSource.init();
            dataSourceMap.put(dbGroupKey, tGroupDataSource);
        }
        if (dbGroupKeys.size() >= dscount) {
            dscount = dbGroupKeys.size();
        } else {
            for (int ii = dbGroupKeys.size(); ii < dscount; ii++) {
                dbGroupKeys.add(dscount + "-OFF");
            }
        }
        outStep = innerStep * dscount;// 计算外步长

        outputInitResult();
    }

    /**
     * 初始化完打印配置信息
     */
    private void outputInitResult() {
        StringBuilder sb = new StringBuilder();
        sb.append("GroupSequenceDao初始化完成：\r\n ");
        sb.append("appName:").append(appName).append("\r\n");
        sb.append("innerStep:").append(this.innerStep).append("\r\n");
        sb.append("dataSource:").append(dscount).append("个:");
        for (String str : dbGroupKeys) {
            sb.append("[").append(str).append("]、");
        }
        sb.append("\r\n");
        sb.append("adjust：").append(adjust).append("\r\n");
        sb.append("retryTimes:").append(retryTimes).append("\r\n");
        sb.append("tableName:").append(tableName).append("\r\n");
        sb.append("nameColumnName:").append(nameColumnName).append("\r\n");
        sb.append("valueColumnName:").append(valueColumnName).append("\r\n");
        sb.append("gmtModifiedColumnName:").append(gmtModifiedColumnName).append("\r\n");
        logger.info(sb.toString());
    }

    /**
     * @param index gourp内的序号，从0开始
     * @param value 当前取的值
     * @return
     */
    private boolean check(int index, long value) {
        return (value % outStep) == (index * innerStep);
    }

    /**
     * <pre>
     * 检查并初试某个sequence。 
     * 
     * 1、如果sequece不处在，插入值，并初始化值。 
     * 2、如果已经存在，但有重叠，重新生成。
     * 3、如果已经存在，且无重叠。
     * 
     * @throws SequenceException
     * </pre>
     */
    public void adjust(String name) throws SequenceException, SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        for (int i = 0; i < dbGroupKeys.size(); i++) {
            if (dbGroupKeys.get(i).toUpperCase().endsWith("-OFF"))// 已经关掉，不处理
            {
                continue;
            }
            TGroupDataSource tGroupDataSource = (TGroupDataSource) dataSourceMap.get(dbGroupKeys.get(i));
            try {
                conn = tGroupDataSource.getConnection();
                stmt = conn.prepareStatement(getSelectSql());
                stmt.setString(1, name);
                GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
                rs = stmt.executeQuery();
                int item = 0;
                while (rs.next()) {
                    item++;
                    long val = rs.getLong(this.getValueColumnName());
                    if (!check(i, val)) // 检验初值
                    {
                        if (this.isAdjust()) {
                            this.adjustUpdate(i, val, name);
                        } else {
                            logger.error("数据库中配置的初值出错！请调整你的数据库，或者启动adjust开关");
                            throw new SequenceException("数据库中配置的初值出错！请调整你的数据库，或者启动adjust开关");
                        }
                    }
                }
                if (item == 0)// 不存在,插入这条记录
                {
                    if (this.isAdjust()) {
                        this.adjustInsert(i, name);
                    } else {
                        logger.error("数据库中未配置该sequence！请往数据库中插入sequence记录，或者启动adjust开关");
                        throw new SequenceException("数据库中未配置该sequence！请往数据库中插入sequence记录，或者启动adjust开关");
                    }
                }
            } catch (SQLException e) {// 吞掉SQL异常，我们允许不可用的库存在
                logger.error("初值校验和自适应过程中出错.", e);
                throw e;
            } finally {
                closeDbResource(rs, stmt, conn);
            }
        }
    }

    /**
     * 更新
     * 
     * @param index
     * @param value
     * @param name
     * @throws SequenceException
     * @throws SQLException
     */
    private void adjustUpdate(int index, long value, String name) throws SequenceException, SQLException {
        long newValue = (value - value % outStep) + outStep + index * innerStep;// 设置成新的调整值
        TGroupDataSource tGroupDataSource = (TGroupDataSource) dataSourceMap.get(dbGroupKeys.get(index));
        Connection conn = null;
        PreparedStatement stmt = null;
        // ResultSet rs = null;
        try {
            conn = tGroupDataSource.getConnection();
            stmt = conn.prepareStatement(getUpdateSql());
            stmt.setLong(1, newValue);
            stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            stmt.setString(3, name);
            stmt.setLong(4, value);
            GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
            int affectedRows = stmt.executeUpdate();
            if (affectedRows == 0) {
                throw new SequenceException("faild to auto adjust init value at  " + name + " update affectedRow =0");
            }
            logger.info(dbGroupKeys.get(index) + "更新初值成功!" + "sequence Name：" + name + "更新过程：" + value + "-->"
                        + newValue);
        } catch (SQLException e) { // 吃掉SQL异常，抛Sequence异常
            logger.error("由于SQLException,更新初值自适应失败！dbGroupIndex:" + dbGroupKeys.get(index) + "，sequence Name：" + name
                         + "更新过程：" + value + "-->" + newValue, e);
            throw new SequenceException("由于SQLException,更新初值自适应失败！dbGroupIndex:" + dbGroupKeys.get(index)
                                        + "，sequence Name：" + name + "更新过程：" + value + "-->" + newValue, e);
        } finally {
            closeDbResource(null, stmt, conn);
        }
    }

    /**
     * 插入新值
     * 
     * @param index
     * @param name
     * @return
     * @throws SequenceException
     * @throws SQLException
     */
    private void adjustInsert(int index, String name) throws SequenceException, SQLException {
        TGroupDataSource tGroupDataSource = (TGroupDataSource) dataSourceMap.get(dbGroupKeys.get(index));
        long newValue = index * innerStep;
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = tGroupDataSource.getConnection();
            stmt = conn.prepareStatement(getInsertSql());
            stmt.setString(1, name);
            stmt.setLong(2, newValue);
            stmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
            int affectedRows = stmt.executeUpdate();
            if (affectedRows == 0) {
                throw new SequenceException("faild to auto adjust init value at  " + name + " update affectedRow =0");
            }
            logger.info(dbGroupKeys.get(index) + "   name:" + name + "插入初值:" + name + "value:" + newValue);

        } catch (SQLException e) {
            logger.error("由于SQLException,插入初值自适应失败！dbGroupIndex:" + dbGroupKeys.get(index) + "，sequence Name：" + name
                         + "   value:" + newValue, e);
            throw new SequenceException("由于SQLException,插入初值自适应失败！dbGroupIndex:" + dbGroupKeys.get(index)
                                        + "，sequence Name：" + name + "   value:" + newValue, e);
        } finally {
            closeDbResource(rs, stmt, conn);
        }
    }

    private ConcurrentHashMap<Integer/* ds index */, AtomicInteger/* 掠过次数 */> excludedKeyCount    = new ConcurrentHashMap<Integer, AtomicInteger>(dscount);
    // 最大略过次数后恢复
    private int                                                               maxSkipCount        = 10;
    // 使用慢速数据库保护
    private boolean                                                           useSlowProtect      = false;
    // 保护的时间
    private int                                                               protectMilliseconds = 50;

    private ExecutorService                                                   exec                = Executors.newFixedThreadPool(1);

    protected Lock                                                            configLock          = new ReentrantLock();

    /**
     * 检查groupKey对象是否已经关闭
     * 
     * @param groupKey
     * @return
     */
    protected boolean isOffState(String groupKey) {
        return groupKey.toUpperCase().endsWith("-OFF");
    }

    /**
     * 检查是否被exclude,如果有尝试恢复
     * 
     * @param index
     * @return
     */
    protected boolean recoverFromExcludes(int index) {
        boolean result = true;
        if (excludedKeyCount.get(index) != null) {
            if (excludedKeyCount.get(index).incrementAndGet() > maxSkipCount) {
                excludedKeyCount.remove(index);
                logger.error(maxSkipCount + "次数已过，index为" + index + "的数据源后续重新尝试取序列");
            } else {
                result = false;
            }
        }
        return result;
    }

    protected long queryOldValue(DataSource dataSource, String keyName) throws SQLException, SequenceException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            stmt = conn.prepareStatement(getSelectSql());
            stmt.setString(1, keyName);
            GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            } else {
                throw new SequenceException("找不到对应的sequence记录，请检查sequence : " + keyName);
            }
        } finally {
            closeDbResource(rs, stmt, conn);
        }
    }

    /**
     * CAS更新sequence值
     * 
     * @param dataSource
     * @param keyName
     * @param oldValue
     * @param newValue
     * @return
     * @throws SQLException
     */
    protected int updateNewValue(DataSource dataSource, String keyName, long oldValue, long newValue)
                                                                                                     throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            stmt = conn.prepareStatement(getUpdateSql());
            stmt.setLong(1, newValue);
            stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            stmt.setString(3, keyName);
            stmt.setLong(4, oldValue);
            GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
            return stmt.executeUpdate();
        } finally {
            closeDbResource(rs, stmt, conn);
        }
    }

    /**
     * 从指定的数据库中获取sequence值
     * 
     * @param dataSource
     * @param keyName
     * @return
     * @throws SQLException
     * @throws SequenceException
     */
    protected long getOldValue(final DataSource dataSource, final String keyName) throws SQLException,
                                                                                 SequenceException {
        long result = 0;

        // 如果未使用超时保护或者已经只剩下了1个数据源，无论怎么样去拿
        if (!useSlowProtect || excludedKeyCount.size() >= (dscount - 1)) {
            result = queryOldValue(dataSource, keyName);
        } else {
            FutureTask<Long> future = new FutureTask<Long>(new Callable<Long>() {

                @Override
                public Long call() throws Exception {
                    return queryOldValue(dataSource, keyName);
                }
            });
            try {
                exec.submit(future);
                result = future.get(protectMilliseconds, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new SQLException("[SEQUENCE SLOW-PROTECTED MODE]:InterruptedException", e);
            } catch (ExecutionException e) {
                throw new SQLException("[SEQUENCE SLOW-PROTECTED MODE]:ExecutionException", e);
            } catch (TimeoutException e) {
                throw new SQLException("[SEQUENCE SLOW-PROTECTED MODE]:TimeoutException,当前设置超时时间为"
                                       + protectMilliseconds, e);
            }
        }
        return result;
    }

    /**
     * 生成oldValue生成newValue
     * 
     * @param index
     * @param oldValue
     * @param keyName
     * @return
     * @throws SequenceException
     */
    protected long generateNewValue(int index, long oldValue, String keyName) throws SequenceException {
        long newValue = oldValue + outStep;
        if (!check(index, newValue)) // 新算出来的值有问题
        {
            if (this.isAdjust()) {
                newValue = adjustNewValue(index, newValue);
            } else {
                throwErrorRangeException(index, keyName);
            }
        }
        return newValue;
    }

    protected long adjustNewValue(int index, long newValue) {
        return (newValue - newValue % outStep) + outStep + index * innerStep;// 设置成新的调整值
    }

    protected void throwErrorRangeException(int index, String keyName) throws SequenceException {
        String errorMsg = dbGroupKeys.get(index) + ":" + keyName + "的值得错误，覆盖到其他范围段了！请修改数据库，或者开启adjust开关！";
        throw new SequenceException(errorMsg);
    }

    protected TGroupDataSource getGroupDsByIndex(int index) {
        return (TGroupDataSource) dataSourceMap.get(dbGroupKeys.get(index));
    }

    /**
     * 检查该sequence值是否在正常范围内
     * 
     * @return
     */
    protected boolean isOldValueFixed(long oldValue) {
        boolean result = true;
        StringBuilder message = new StringBuilder();
        if (oldValue < 0) {
            message.append("Sequence value cannot be less than zero.");
            result = false;
        } else if (oldValue > Long.MAX_VALUE - DELTA) {
            message.append("Sequence value overflow.");
            result = false;
        }
        if (!result) {
            message.append(" Sequence value  = ").append(oldValue);
            message.append(", please check table ").append(getTableName());
            logger.info(message.toString());
        }
        return result;
    }

    /**
     * 将该数据源排除到sequence可选数据源以外
     * 
     * @param index
     */
    protected void excludeDataSource(int index) {
        // 如果数据源只剩下了最后一个，就不要排除了
        if (excludedKeyCount.size() < (dscount - 1)) {
            excludedKeyCount.put(index, new AtomicInteger(0));
            logger.error("暂时踢除index为" + index + "的数据源，" + maxSkipCount + "次后重新尝试");
        }
    }

    public SequenceRange nextRange(final String name) throws SequenceException {
        if (name == null) {
            logger.error("序列名为空！");
            throw new IllegalArgumentException("序列名称不能为空");
        }

        configLock.lock();
        try {
            int[] randomIntSequence = RandomSequence.randomIntSequence(dscount);
            for (int i = 0; i < retryTimes; i++) {
                for (int j = 0; j < dscount; j++) {
                    int index = randomIntSequence[j];
                    if (isOffState(dbGroupKeys.get(index)) || !recoverFromExcludes(index)) {
                        continue;
                    }

                    final TGroupDataSource tGroupDataSource = getGroupDsByIndex(index);
                    long oldValue;
                    // 查询，只在这里做数据库挂掉保护和慢速数据库保护
                    try {
                        oldValue = getOldValue(tGroupDataSource, name);
                        if (!isOldValueFixed(oldValue)) {
                            continue;
                        }
                    } catch (SQLException e) {
                        logger.error("取范围过程中--查询出错！" + dbGroupKeys.get(index) + ":" + name, e);
                        excludeDataSource(index);
                        continue;
                    }

                    long newValue = generateNewValue(index, oldValue, name);
                    try {
                        if (0 == updateNewValue(tGroupDataSource, name, oldValue, newValue)) {
                            continue;
                        }
                    } catch (SQLException e) {
                        logger.error("取范围过程中--更新出错！" + dbGroupKeys.get(index) + ":" + name, e);
                        continue;
                    }

                    return new SequenceRange(newValue + 1, newValue + innerStep);

                }
                // 当还有最后一次重试机会时,清空excludedMap,让其有最后一次机会
                if (i == (retryTimes - 2)) {
                    excludedKeyCount.clear();
                }
            }
            logger.error("所有数据源都不可用！且重试" + this.retryTimes + "次后，仍然失败!");
            throw new SequenceException("All dataSource faild to get value!");
        } finally {
            configLock.unlock();
        }
    }

    public void setDscount(int dscount) {
        this.dscount = dscount;
    }

    protected String getInsertSql() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("insert into ").append(getTableName()).append("(");
        buffer.append(getNameColumnName()).append(",");
        buffer.append(getValueColumnName()).append(",");
        buffer.append(getGmtModifiedColumnName()).append(") values(?,?,?);");
        return buffer.toString();
    }

    protected String getSelectSql() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("select ").append(getValueColumnName());
        buffer.append(" from ").append(getTableName());
        buffer.append(" where ").append(getNameColumnName()).append(" = ?");
        return buffer.toString();
    }

    protected String getUpdateSql() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("update ").append(getTableName());
        buffer.append(" set ").append(getValueColumnName()).append(" = ?, ");
        buffer.append(getGmtModifiedColumnName()).append(" = ? where ");
        buffer.append(getNameColumnName()).append(" = ? and ");
        buffer.append(getValueColumnName()).append(" = ?");
        return buffer.toString();
    }

    protected static void closeDbResource(ResultSet rs, Statement stmt, Connection conn) {
        closeResultSet(rs);
        closeStatement(stmt);
        closeConnection(conn);
    }

    protected static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.debug("Could not close JDBC ResultSet", e);
            } catch (Throwable e) {
                logger.debug("Unexpected exception on closing JDBC ResultSet", e);
            }
        }
    }

    protected static void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.debug("Could not close JDBC Statement", e);
            } catch (Throwable e) {
                logger.debug("Unexpected exception on closing JDBC Statement", e);
            }
        }
    }

    protected static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.debug("Could not close JDBC Connection", e);
            } catch (Throwable e) {
                logger.debug("Unexpected exception on closing JDBC Connection", e);
            }
        }
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public int getInnerStep() {
        return innerStep;
    }

    public void setInnerStep(int innerStep) {
        this.innerStep = innerStep;
    }

    public String getTableName() {
        // 全链路压测需求
        String t = EagleeyeHelper.getUserData("t");
        if (!StringUtils.isBlank(t) && t.equals("1")) {
            return testTableName;
        } else {
            return tableName;
        }
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
        this.testTableName = TEST_TABLE_PREFIX + this.tableName;
    }

    public String getNameColumnName() {
        return nameColumnName;
    }

    public void setNameColumnName(String nameColumnName) {
        this.nameColumnName = nameColumnName;
    }

    public String getValueColumnName() {
        return valueColumnName;
    }

    public void setValueColumnName(String valueColumnName) {
        this.valueColumnName = valueColumnName;
    }

    public String getGmtModifiedColumnName() {
        return gmtModifiedColumnName;
    }

    public void setGmtModifiedColumnName(String gmtModifiedColumnName) {
        this.gmtModifiedColumnName = gmtModifiedColumnName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setDbGroupKeys(List<String> dbGroupKeys) {
        // 这里ugly,如果动态变更也调用这个方法的话，那就见鬼了
        this.oriDbGroupKeys = dbGroupKeys;
        this.dbGroupKeys = dbGroupKeys;
    }

    public boolean isAdjust() {
        return adjust;
    }

    public void setAdjust(boolean adjust) {
        this.adjust = adjust;
    }

    public int getMaxSkipCount() {
        return maxSkipCount;
    }

    public void setMaxSkipCount(int maxSkipCount) {
        this.maxSkipCount = maxSkipCount;
    }

    public boolean isUseSlowProtect() {
        return useSlowProtect;
    }

    public void setUseSlowProtect(boolean useSlowProtect) {
        this.useSlowProtect = useSlowProtect;
    }

    public int getProtectMilliseconds() {
        return protectMilliseconds;
    }

    public void setProtectMilliseconds(int protectMilliseconds) {
        this.protectMilliseconds = protectMilliseconds;
    }

    public String getSwitchTempTable() {
        String t = EagleeyeHelper.getUserData("t");
        if (!StringUtils.isBlank(t) && t.equals("1")) {
            return testSwitchTempTable;
        } else {
            return switchTempTable;
        }
    }

    public void setSwitchTempTable(String switchTempTable) {
        this.switchTempTable = switchTempTable;
        this.testSwitchTempTable = TEST_TABLE_PREFIX + this.switchTempTable;
    }
}
