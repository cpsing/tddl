package com.taobao.tddl.optimizer.parse.hint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DirectlyRouteCondition extends ExtraCmdRouteCondition implements RouteCondition {

    protected String      dbId;                           // 目标库的id
    protected Set<String> tables = new HashSet<String>(2); // 目标表的id

    public Set<String> getTables() {
        return tables;
    }

    public void setTables(Set<String> tables) {
        this.tables = tables;
    }

    public void addTable(String table) {
        tables.add(table);
    }

    public String getDbId() {
        return dbId;
    }

    public void setDBId(String dbId) {
        this.dbId = dbId;
    }

    /**
     * @return 两级的MAP , 顺序为：db index key , original table , targetTable
     */
    public Map<String, List<Map<String, String>>> getShardTableMap() {
        List<Map<String/* original table */, String/* targetTable */>> tableList = new ArrayList<Map<String, String>>(1);
        for (String targetTable : tables) {
            Map<String/* original table */, String/* target table */> table = new HashMap<String, String>(tables.size());
            table.put(virtualTableName, targetTable);
            if (!table.isEmpty()) {
                tableList.add(table);
            }
        }

        Map<String/* key */, List<Map<String/* original table */, String/* targetTable */>>> shardTableMap = new HashMap<String, List<Map<String, String>>>(2);
        shardTableMap.put(dbId, tableList);
        return shardTableMap;
    }
}
