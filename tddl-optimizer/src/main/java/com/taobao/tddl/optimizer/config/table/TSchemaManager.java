//package com.taobao.tddl.optimizer.config.table;
//
//import java.util.Collection;
//import java.util.HashSet;
//import java.util.Set;
//
//import com.taobao.tddl.common.exception.TddlException;
//import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
//import com.taobao.tddl.common.utils.logger.Logger;
//import com.taobao.tddl.common.utils.logger.LoggerFactory;
//import com.taobao.tddl.optimizer.rule.RuleSchemaManager;
//
//public class TSchemaManager extends AbstractLifecycle implements SchemaManager {
//
//    private final static Logger logger              = LoggerFactory.getLogger(TSchemaManager.class);
//
//    private StaticSchemaManager staticSchemaManager = null;
//    private RuleSchemaManager   ruleSchemaManager   = null;
//
//    public TSchemaManager(StaticSchemaManager staticSchemaManager, RuleSchemaManager ruleSchemaManager){
//        super();
//        this.staticSchemaManager = staticSchemaManager;
//        this.ruleSchemaManager = ruleSchemaManager;
//    }
//
//    @Override
//    public TableMeta getTable(String tableName) {
//        TableMeta schema = null;
//
//        if (staticSchemaManager != null) {
//            schema = staticSchemaManager.getTable(tableName);
//            if (schema != null) return schema;
//        }
//
//        if (ruleSchemaManager != null) {
//            schema = ruleSchemaManager.getTable(tableName);
//        }
//        return schema;
//    }
//
//    @Override
//    public void putTable(String tableName, TableMeta tableMeta) {
//
//        if (staticSchemaManager != null) {
//            staticSchemaManager.putTable(tableName, tableMeta);
//        }
//    }
//
//    @Override
//    public Collection<TableMeta> getAllTables() {
//        Set<TableMeta> all = new HashSet();
//
//        if (staticSchemaManager != null) {
//            all.addAll(staticSchemaManager.getAllTables());
//        }
//
//        if (ruleSchemaManager != null)
//
//        {
//            all.addAll(ruleSchemaManager.getAllTables());
//
//        }
//
//        return all;
//    }
//
//    protected void doInit() throws TddlException {
//
//        if (this.staticSchemaManager != null) this.staticSchemaManager.init();
//
//        if (this.ruleSchemaManager != null) this.ruleSchemaManager.init();
//    }
//
// }
