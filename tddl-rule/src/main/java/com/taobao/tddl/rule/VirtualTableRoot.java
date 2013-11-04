package com.taobao.tddl.rule;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.taobao.tddl.common.model.DBType;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.rule.utils.RuleUtils;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 一组{@linkplain VirtualTable}的集合
 * 
 * @author junyu
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 */
public class VirtualTableRoot extends AbstractLifecycle implements Lifecycle {

    private static final Logger                    logger           = LoggerFactory.getLogger(VirtualTableRoot.class);
    protected String                               dbType           = "MYSQL";
    protected Map<String/* 小写key */, VirtualTable> virtualTableMap;
    protected Map<String/* 小写key */, String>       dbIndexMap;

    protected String                               defaultDbIndex;
    protected boolean                              needIdInGroup    = false;
    protected boolean                              completeDistinct = false;

    public void init() {
        for (Map.Entry<String, VirtualTable> entry : virtualTableMap.entrySet()) {
            logger.warn("virtual table start to init :" + entry.getKey());
            VirtualTable vtab = entry.getValue();
            if (vtab.getDbType() == null) {
                // 如果虚拟表中dbType为null,那指定全局dbType
                vtab.setDbType(this.getDbTypeEnumObj());
            }

            if (vtab.getVirtualTbName() == null) {
                vtab.setVirtualTbName(entry.getKey());
            }
            vtab.init();
            logger.warn("virtual table inited :" + entry.getKey());
        }
    }

    /**
     * 此处有个问题是Map中key对应的VirtualTableRule为null;
     * 
     * @param virtualTableName
     * @return
     */
    public VirtualTable getVirtualTable(String virtualTableName) {
        RuleUtils.notNull(virtualTableName, "virtual table name is null");
        return virtualTableMap.get(virtualTableName.toLowerCase());
    }

    public void setTableRules(Map<String, VirtualTable> virtualTableMap) {
        Map<String, VirtualTable> lowerKeysLogicTableMap = new HashMap<String, VirtualTable>(virtualTableMap.size());
        for (Entry<String, VirtualTable> entry : virtualTableMap.entrySet()) {
            lowerKeysLogicTableMap.put(entry.getKey().toLowerCase(), entry.getValue()); // 转化小写
        }
        this.virtualTableMap = lowerKeysLogicTableMap;
    }

    public void setDbIndexMap(Map<String, String> dbIndexMap) {
        Map<String, String> lowerKeysDbIndexMap = new HashMap<String, String>(dbIndexMap.size());
        for (Entry<String, String> entry : dbIndexMap.entrySet()) {
            lowerKeysDbIndexMap.put(entry.getKey().toLowerCase(), entry.getValue());// 转化小写
        }
        this.dbIndexMap = lowerKeysDbIndexMap;
    }

    public Map<String, VirtualTable> getVirtualTableMap() {
        return virtualTableMap;
    }

    public void setVirtualTableMap(Map<String, VirtualTable> virtualTableMap) {
        this.virtualTableMap = virtualTableMap;
    }

    public Map<String, VirtualTable> getTableRules() {
        return virtualTableMap;
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
}
