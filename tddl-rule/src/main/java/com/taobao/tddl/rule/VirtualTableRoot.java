package com.taobao.tddl.rule;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.DBType;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.rule.exceptions.TddlRuleException;
import com.taobao.tddl.rule.utils.RuleUtils;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 一组{@linkplain TableRule}的集合
 * 
 * @author junyu
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 */
public class VirtualTableRoot extends AbstractLifecycle implements Lifecycle {

    private static final Logger                 logger           = LoggerFactory.getLogger(VirtualTableRoot.class);
    protected String                            dbType           = "MYSQL";
    protected Map<String/* 大写key */, TableRule> virtualTableMap;
    protected Map<String/* 大写key */, String>    dbIndexMap;

    protected String                            defaultDbIndex;
    protected boolean                           needIdInGroup    = false;
    protected boolean                           completeDistinct = false;
    protected boolean                           lazyInit         = false;

    public void init() throws TddlException {
        for (Map.Entry<String, TableRule> entry : virtualTableMap.entrySet()) {
            if (!lazyInit) {
                initTableRule(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * 此处有个问题是Map中key对应的VirtualTableRule为null;
     * 
     * @param virtualTableName
     * @return
     */
    public TableRule getVirtualTable(String virtualTableName) {
        RuleUtils.notNull(virtualTableName, "virtual table name is null");
        TableRule tablRule = virtualTableMap.get(virtualTableName.toUpperCase());
        if (tablRule != null && lazyInit && !tablRule.isInited()) {
            try {
                initTableRule(virtualTableName, tablRule);
            } catch (TddlException e) {
                throw new TddlRuleException(e);
            }
        }

        return tablRule;
    }

    public void setTableRules(Map<String, TableRule> virtualTableMap) {
        Map<String, TableRule> logicTableMap = new HashMap<String, TableRule>(virtualTableMap.size());
        for (Entry<String, TableRule> entry : virtualTableMap.entrySet()) {
            logicTableMap.put(entry.getKey().toUpperCase(), entry.getValue()); // 转化大写
        }
        this.virtualTableMap = logicTableMap;
    }

    public void setDbIndexMap(Map<String, String> dbIndexMap) {
        Map<String, String> logicTableMap = new HashMap<String, String>(dbIndexMap.size());
        for (Entry<String, String> entry : dbIndexMap.entrySet()) {
            logicTableMap.put(entry.getKey().toUpperCase(), entry.getValue());// 转化大写
        }
        this.dbIndexMap = logicTableMap;
    }

    private void initTableRule(String tableNameKey, TableRule tableRule) throws TddlException {
        tableNameKey = tableNameKey.toUpperCase();
        logger.warn("virtual table start to init :" + tableNameKey);
        if (tableRule.getDbType() == null) {
            // 如果虚拟表中dbType为null,那指定全局dbType
            tableRule.setDbType(this.getDbTypeEnumObj());
        }

        if (tableRule.getVirtualTbName() == null) {
            tableRule.setVirtualTbName(tableNameKey);
        }
        tableRule.init();
        logger.warn("virtual table inited :" + tableNameKey);
    }

    public String getDefaultDbIndex() {
        return defaultDbIndex;
    }

    public void setDefaultDbIndex(String defaultDbIndex) {
        this.defaultDbIndex = defaultDbIndex;
    }

    public Map<String, String> getDbIndexMap() {
        return dbIndexMap;
    }

    public DBType getDbTypeEnumObj() {
        return DBType.valueOf(this.dbType);
    }

    public String getDbType() {
        return this.dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public boolean isNeedIdInGroup() {
        return needIdInGroup;
    }

    public void setNeedIdInGroup(boolean needIdInGroup) {
        this.needIdInGroup = needIdInGroup;
    }

    public boolean isCompleteDistinct() {
        return completeDistinct;
    }

    public void setCompleteDistinct(boolean completeDistinct) {
        this.completeDistinct = completeDistinct;
    }

    public void setLazyInit(boolean lazyInit) {
        this.lazyInit = lazyInit;
    }

}
