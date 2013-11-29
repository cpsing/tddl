package com.taobao.tddl.optimizer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.optimizer.config.Matrix;
import com.taobao.tddl.optimizer.config.MockRepoIndexManager;
import com.taobao.tddl.optimizer.config.table.LocalSchemaManager;
import com.taobao.tddl.optimizer.config.table.RepoSchemaManager;
import com.taobao.tddl.optimizer.config.table.parse.MatrixParser;
import com.taobao.tddl.optimizer.costbased.CostBasedOptimizer;
import com.taobao.tddl.optimizer.parse.SqlParseManager;
import com.taobao.tddl.optimizer.parse.cobar.CobarSqlParseManager;
import com.taobao.tddl.optimizer.rule.OptimizerRule;
import com.taobao.tddl.rule.TddlRule;

public class BaseOptimizerTest {

    protected static final String       APPNAME     = "tddl";
    protected static final String       table_file  = "config/test_table.xml";
    protected static final String       matrix_file = "config/test_matrix.xml";
    protected static final String       rule_file   = "config/test_rule.xml";

    protected static SqlParseManager    parser      = new CobarSqlParseManager();
    protected static OptimizerRule      rule;
    protected static RepoSchemaManager  schemaManager;
    protected static CostBasedOptimizer optimizer;

    @BeforeClass
    public static void initial() {
        parser.init();

        OptimizerContext context = new OptimizerContext();
        TddlRule tddlRule = new TddlRule();
        tddlRule.setAppRuleFile("classpath:" + rule_file);
        tddlRule.setAppName(APPNAME);
        tddlRule.init();

        rule = new OptimizerRule(tddlRule);

        LocalSchemaManager localSchemaManager = LocalSchemaManager.parseSchema(Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream(table_file));

        Matrix matrix = MatrixParser.parse(Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream(matrix_file));

        schemaManager = new RepoSchemaManager();
        schemaManager.setLocal(localSchemaManager);
        schemaManager.setUseCache(true);
        schemaManager.setGroup(matrix.getGroup("andor_group_0"));
        schemaManager.init();

        context.setMatrix(matrix);
        context.setRule(rule);
        context.setSchemaManager(schemaManager);
        context.setIndexManager(new MockRepoIndexManager(schemaManager));

        OptimizerContext.setContext(context);

        optimizer = new CostBasedOptimizer();
        optimizer.setSqlParseManager(parser);
        optimizer.init();
    }

    @AfterClass
    public static void tearDown() {
        schemaManager.destory();
        parser.destory();
        optimizer.destory();
    }

    protected Map<Integer, ParameterContext> convert(List<Object> args) {
        Map<Integer, ParameterContext> map = new HashMap<Integer, ParameterContext>(args.size());
        int index = 0;
        for (Object obj : args) {
            ParameterContext context = new ParameterContext(ParameterMethod.setObject1, new Object[] { index, obj });
            map.put(index, context);
            index++;
        }
        return map;
    }

    protected Map<Integer, ParameterContext> convert(Object[] args) {
        return convert(Arrays.asList(args));
    }

}
