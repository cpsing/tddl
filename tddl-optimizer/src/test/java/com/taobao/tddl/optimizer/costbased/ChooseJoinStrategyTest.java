package com.taobao.tddl.optimizer.costbased;

import org.codehaus.groovy.syntax.ParserException;
import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinType;
import com.taobao.tddl.optimizer.exceptions.QueryException;

public class ChooseJoinStrategyTest extends BaseOptimizerTest {

    /**
     * c1有索引，c2无索引
     * 
     * @throws ParserException
     * @throws QueryException
     */
    @Test
    public void testChooseJoinOrderAndIndexForASubQueryTree() throws ParserException, QueryException {
        {
            // table10 index nested loop join table11 use table11.c1 as index
            TableNode table1 = new TableNode("TABLE10");
            TableNode table2 = new TableNode("TABLE11");
            QueryTreeNode qn = table1.join(table2).addJoinKeys("c2", "c1");
            qn.build();
            // qn = o.chooseJoinOrderAndIndexForASubQueryTree(qn, null, null);
            Assert.assertEquals(JoinType.INDEX_NEST_LOOP, ((JoinNode) qn).getJoinStrategy().getType());
            Assert.assertEquals("TABLE10", ((JoinNode) qn).getLeftNode().getName());
            Assert.assertEquals("TABLE11", ((JoinNode) qn).getRightNode().getName());
            Assert.assertEquals("TABLE11._C1", ((TableNode) ((JoinNode) qn).getRightNode()).getIndexUsed().getName());
            System.out.println();
        }

        {
            // table11 index nested loop join table10 use table10.c1 as index
            TableNode table1 = new TableNode("TABLE10");
            TableNode table2 = new TableNode("TABLE11");
            QueryTreeNode qn = table1.join(table2).addJoinKeys("c1", "c2");
            qn.build();
            // qn = o.chooseJoinOrderAndIndexForASubQueryTree(qn, null, null);
            Assert.assertEquals(JoinType.INDEX_NEST_LOOP, ((JoinNode) qn).getJoinStrategy().getType());
            Assert.assertEquals("TABLE11", ((JoinNode) qn).getLeftNode().getName());
            Assert.assertEquals("TABLE10", ((JoinNode) qn).getRightNode().getName());
            Assert.assertEquals("TABLE10._C1", ((TableNode) ((JoinNode) qn).getRightNode()).getIndexUsed().getName());
            System.out.println();
        }
    }

}
