package com.taobao.tddl.optimizer;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.optimizer.config.MockRepoIndexManager;
import com.taobao.tddl.optimizer.config.table.RepoSchemaManager;
import com.taobao.tddl.optimizer.config.table.StaticSchemaManager;
import com.taobao.tddl.optimizer.config.table.parse.MatrixParser;
import com.taobao.tddl.optimizer.costbased.CostBasedOptimizer;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.LocalStatManager;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.RepoStatManager;
import com.taobao.tddl.optimizer.parse.SqlParseManager;
import com.taobao.tddl.optimizer.parse.cobar.CobarSqlParseManager;
import com.taobao.tddl.optimizer.rule.OptimizerRule;
import com.taobao.tddl.repo.mysql.sqlconvertor.SqlConvertor;
import com.taobao.tddl.repo.mysql.sqlconvertor.SqlMergeNode;
import com.taobao.tddl.rule.TddlRule;

public class BaseSqlOptimizerTest {

    protected static final String       APPNAME     = "tddl";
    protected static final String       table_file  = "matrix/mysql_schema.xml";
    protected static final String       matrix_file = "matrix/server_topology.xml";
    protected static final String       rule_file   = "matrix/mysql_rule.xml";

    protected static SqlParseManager    parser      = new CobarSqlParseManager();
    protected static OptimizerRule      rule;
    protected static RepoSchemaManager  schemaManager;
    protected static CostBasedOptimizer optimizer;
    protected static RepoStatManager    statManager;
    protected static SqlConvertor       sqlConvert  = new SqlConvertor();

    @BeforeClass
    public static void initial() throws TddlException {
        parser.init();

        OptimizerContext context = new OptimizerContext();
        TddlRule tddlRule = new TddlRule();
        tddlRule.setAppRuleFile("classpath:" + rule_file);
        tddlRule.setAppName(APPNAME);
        tddlRule.init();

        rule = new OptimizerRule(tddlRule);

        StaticSchemaManager localSchemaManager = StaticSchemaManager.parseSchema(Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream(table_file));

        Matrix matrix = MatrixParser.parse(Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream(matrix_file));

        schemaManager = new RepoSchemaManager();
        schemaManager.setLocal(localSchemaManager);
        schemaManager.setGroup(matrix.getGroup("andor_mysql_group_0"));
        schemaManager.init();

        LocalStatManager local = new LocalStatManager();
        local.init();

        statManager = new RepoStatManager();
        statManager.setLocal(local);
        statManager.setUseCache(true);
        statManager.setGroup(matrix.getGroup("andor_mysql_group_0"));
        statManager.init();

        context.setMatrix(matrix);
        context.setRule(rule);
        context.setSchemaManager(schemaManager);
        context.setStatManager(statManager);
        context.setIndexManager(new MockRepoIndexManager(schemaManager));

        OptimizerContext.setContext(context);

        optimizer = new CostBasedOptimizer();
        optimizer.setSqlParseManager(parser);
        optimizer.init();
    }

    @AfterClass
    public static void tearDown() throws TddlException {
        schemaManager.destory();
        statManager.destory();
        parser.destory();
        optimizer.destory();
    }

    public SqlMergeNode getMergeNode(String sql) throws Exception {
        return sqlConvert.convert(null, optimizer.optimizeAndAssignment(sql, null, null, true), false);
    }

    public String getSql0(SqlMergeNode node) {
        return node.getSubQuerys().get("group0").get(0).getSql();
    }

    public String getSql1(SqlMergeNode node) {
        return node.getSubQuerys().get("group1").get(0).getSql();
    }
}
