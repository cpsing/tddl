package com.taobao.tddl.optimizer.costbased;

import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.costbased.esitimater.Cost;
import com.taobao.tddl.optimizer.costbased.esitimater.CostEsitimaterFactory;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * @author Dreamond
 */
public class IndexChooserTest extends BaseOptimizerTest {

    @Test
    public void testChooseIndex() throws QueryException {
        TableNode table = new TableNode("t1");
        QueryTreeNode qn = table.query("id=1");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(qn, null, null);

        Cost cost = CostEsitimaterFactory.estimate(qn);
        System.out.println(qc);
        System.out.println(cost);
    }

    @Test
    public void testGetKVIndexMeta() throws QueryException {
        // TableMeta ts = smm.getTableStatistics("TEST");
        // Assert.assertEquals(999, ts.getTableRows());
    }

}
