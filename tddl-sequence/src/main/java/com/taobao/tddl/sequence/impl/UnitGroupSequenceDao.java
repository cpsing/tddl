//FIXME 单元化sequenceDao，依赖问题先注释掉
//package com.taobao.tddl.sequence.impl;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Set;
//import java.util.TreeSet;
//
//import javax.sql.DataSource;
//
//import org.apache.commons.lang.StringUtils;
//
//import com.ali.unit.rule.Router;
//import com.ali.unit.rule.RouterUnitsListener;
//import com.taobao.tddl.common.utils.logger.Logger;
//import com.taobao.tddl.common.utils.logger.LoggerFactory;
//import com.taobao.tddl.sequence.exception.SequenceException;
//import com.taobao.tddl.sequence.temp.EagleEye;
//import com.taobao.tddl.sequence.temp.TGroupDataSource;
//
///**
// * @description
// * @author <a href="junyu@taobao.com">junyu</a>
// * @date 2013-5-7下午02:03:52
// */
//public class UnitGroupSequenceDao extends GroupSequenceDao implements RouterUnitsListener {
//
//    private static final Logger logger               = LoggerFactory.getLogger(UnitGroupSequenceDao.class);
//    public static final String  DUMMY_OFF_DBGROUPKEY = "DUMMY-OFF";
//
//    /**
//     * 真正的dbGroupKey 单元数量大于1的前提下，并且unitindex为正数，那么要进行单元化部署的配置变更，否则
//     * 不要搞，后面还会动态变化，这里的逻辑主要为了初始化数据
//     * 
//     * @return
//     * @throws SequenceException
//     */
//    protected List<String> getRealDbGroupKeys() throws SequenceException {
//        int unitCount = getUnitcount();
//        int unitIndex = getUnitindex();
//        if (unitCount > 1 && unitIndex > -1) {
//            List<String> rewritedbGroupKeys = new ArrayList<String>();
//            for (int i = 0; i < unitCount; i++) {
//                if (i == unitIndex) {
//                    rewritedbGroupKeys.addAll(oriDbGroupKeys);
//                } else {
//                    for (@SuppressWarnings("unused")
//                    String key : oriDbGroupKeys) {
//                        rewritedbGroupKeys.add(DUMMY_OFF_DBGROUPKEY);
//                    }
//                }
//            }
//            return rewritedbGroupKeys;
//        } else {
//            return oriDbGroupKeys;
//        }
//    }
//
//    /**
//     * 初试化
//     * 
//     * @throws SequenceException
//     */
//    public void init() throws SequenceException {
//        // 如果应用名为空，直接抛出
//        if (StringUtils.isEmpty(appName)) {
//            SequenceException sequenceException = new SequenceException("appName is Null ");
//            logger.error("没有配置appName", sequenceException);
//            throw sequenceException;
//        }
//
//        if (dbGroupKeys == null || dbGroupKeys.size() == 0) {
//            logger.error("没有配置dbgroupKeys");
//            throw new SequenceException("dbgroupKeys为空！");
//        }
//
//        // 取得unit数据
//        this.changeConfig(false, null);
//        Router.registerUnitsListener(this);
//        dataSourceMap = new HashMap<String, DataSource>();
//        for (String dbGroupKey : dbGroupKeys) {
//            if (dbGroupKey.toUpperCase().endsWith("-OFF")) {
//                continue;
//            }
//            // TGroupDataSource tGroupDataSource = new TGroupDataSource(
//            // dbGroupKey, appName, dataSourceType);
//            TGroupDataSource tGroupDataSource = new TGroupDataSource(dbGroupKey, appName);
//            tGroupDataSource.init();
//            dataSourceMap.put(dbGroupKey, tGroupDataSource);
//        }
//
//        outputInitResult();
//    }
//
//    public int getUnitcount() {
//        Set<String> allUnits = Router.getUnits();
//        if (allUnits == null || allUnits.size() == 0) {
//            return 1;
//        } else {
//            return allUnits.size();
//        }
//    }
//
//    public int getUnitindex() throws SequenceException {
//        // not unit
//        Set<String> units = Router.getUnits();
//        TreeSet<String> orderUnits = new TreeSet<String>();
//        orderUnits.addAll(units);
//        if (orderUnits == null || orderUnits.size() == 0) {
//            return -1;
//        }
//
//        // not unit
//        String currentUnit = Router.getCurrentUnit();
//        if (currentUnit == null) {
//            return -1;
//        }
//
//        Iterator<String> i = orderUnits.iterator();
//        int index = 0;
//        while (i.hasNext()) {
//            if (i.next().equals(currentUnit)) {
//                return index;
//            }
//            index++;
//        }
//
//        StringBuilder err = new StringBuilder("can not find unit in unit list[");
//        while (i.hasNext()) {
//            err.append(i.next());
//            err.append(i.next());
//            err.append(" ");
//        }
//        err.append("],current unit is ");
//        err.append(currentUnit);
//        throw new SequenceException(err.toString());
//    }
//
//    public void changeConfig(boolean outputLog, STATUS status) throws SequenceException {
//        configLock.lock();
//        try {
//            if (Router.isUnitDBUsed()) {
//                this.dbGroupKeys = this.getRealDbGroupKeys();
//                this.dscount = this.dbGroupKeys.size();
//                this.outStep = innerStep * dscount;
//                this.status = status;
//            } else {
//                this.dbGroupKeys = oriDbGroupKeys;
//                this.dscount = dbGroupKeys.size();
//                this.outStep = innerStep * dscount;
//                // 不用单元化那么将status置为null
//                this.status = status;
//            }
//        } finally {
//            configLock.unlock();
//        }
//
//        if (outputLog) {
//            outputInitResult();
//        }
//    }
//
//    private void outputInitResult() {
//        StringBuilder sb = new StringBuilder();
//        sb.append("GroupSequenceDao初始化完成：\r\n ");
//        sb.append("appName:").append(appName).append("\r\n");
//        sb.append("innerStep:").append(this.innerStep).append("\r\n");
//        sb.append("dataSource:").append(dscount).append("个:");
//        for (String str : dbGroupKeys) {
//            sb.append("[").append(str).append("]、");
//        }
//        sb.append("\r\n");
//        sb.append("adjust：").append(adjust).append("\r\n");
//        sb.append("retryTimes:").append(retryTimes).append("\r\n");
//        sb.append("tableName:").append(tableName).append("\r\n");
//        sb.append("switchTempTable:").append(switchTempTable).append("\r\n");
//        sb.append("nameColumnName:").append(nameColumnName).append("\r\n");
//        sb.append("valueColumnName:").append(valueColumnName).append("\r\n");
//        sb.append("gmtModifiedColumnName:").append(gmtModifiedColumnName).append("\r\n");
//        logger.warn(sb.toString());
//    }
//
//    public String getOriTableName() {
//        // 全链路压测需求
//        String t = EagleEye.getUserData("t");
//        if (!StringUtils.isBlank(t) && t.equals("1")) {
//            return this.testTableName;
//        } else {
//            return this.tableName;
//        }
//    }
//
//    public String getSwitchTableName() {
//        // 全链路压测需求
//        String t = EagleEye.getUserData("t");
//        if (!StringUtils.isBlank(t) && t.equals("1")) {
//            return this.testSwitchTempTable;
//        } else {
//            return this.switchTempTable;
//        }
//    }
//
//    @Override
//    public String getTableName() {
//        if (status != null) {
//            if (status == STATUS.BEGIN) {
//                return getSwitchTableName();
//            } else if (status == STATUS.END) {
//                return getOriTableName();
//            } else {
//                logger.error("unknow status:" + status + ",use normal tableName:" + getOriTableName());
//                return getOriTableName();
//            }
//        } else {
//            return getOriTableName();
//        }
//    }
//
//    @Override
//    protected String getInsertSql() {
//        StringBuilder buffer = new StringBuilder();
//        buffer.append("insert into ").append(getTableName()).append("(");
//        buffer.append(getNameColumnName()).append(",");
//        buffer.append(getValueColumnName()).append(",");
//        buffer.append(getGmtModifiedColumnName()).append(") values(?,?,?);");
//        return buffer.toString();
//    }
//
//    @Override
//    protected String getSelectSql() {
//        StringBuilder buffer = new StringBuilder();
//        buffer.append("select ").append(getValueColumnName());
//        buffer.append(" from ").append(getTableName());
//        buffer.append(" where ").append(getNameColumnName()).append(" = ?");
//        return buffer.toString();
//    }
//
//    @Override
//    protected String getUpdateSql() {
//        StringBuilder buffer = new StringBuilder();
//        buffer.append("update ").append(getTableName());
//        buffer.append(" set ").append(getValueColumnName()).append(" = ?, ");
//        buffer.append(getGmtModifiedColumnName()).append(" = ? where ");
//        buffer.append(getNameColumnName()).append(" = ? and ");
//        buffer.append(getValueColumnName()).append(" = ?");
//        return buffer.toString();
//    }
//
//    private volatile STATUS status = null;
//
//    @Override
//    public boolean onChanged(STATUS arg0) {
//        try {
//            this.changeConfig(true, arg0);
//        } catch (SequenceException e) {
//            logger.error("change unit config error!STATUS:" + arg0, e);
//            return false;
//        }
//
//        return true;
//    }
// }
