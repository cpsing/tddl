package com.taobao.tddl.optimizer.costbased;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.dml.DeleteNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.PutNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * @author Dreamond
 */
public class DMLTest extends BaseOptimizerTest {

    // 测试update
    @Test
    public void testUpdate() throws QueryException {
        TableNode table = new TableNode("STUDENT");
        String values[] = { "NAME" };
        UpdateNode update = ((TableNode) table.query("ID>=5&&ID<=5")).update("NAME", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(update, null, null);
        System.out.println(plan);

        String sql = "UPDATE STUDENT SET NAME = NAME WHERE ID>=5&&ID<=5";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        System.out.println(plan);
    }

    public void testUpdate_范围更新生成merge() throws QueryException {
        TableNode table = new TableNode("STUDENT");
        String values[] = { "NAME" };
        UpdateNode update = ((TableNode) table.query("ID>=5")).update("NAME", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(update, null, null);
        System.out.println(plan);
        Assert.assertTrue(plan instanceof IMerge);
        System.out.println(plan);

        String sql = "UPDATE STUDENT SET NAME = NAME WHERE ID>=5";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        System.out.println(plan);
    }

    // 测试put
    @Test
    public void testPut() throws QueryException {
        TableNode table = new TableNode("STUDENT");
        Comparable values[] = { 2 };
        PutNode update = ((TableNode) table).put("ID", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(update, null, null);
        System.out.println(plan);

        String sql = "INSERT INTO STUDENT(ID) VALUES(2)";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        System.out.println(plan);

    }

    @Test
    public void testPut_全字段() throws QueryException {
        TableNode table = new TableNode("STUDENT");
        Comparable values[] = { 2, "sysu", "sun" };
        PutNode put = table.put("ID SCHOOL NAME", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(put, null, null);
        System.out.println(plan);

        String sql = "REPLACE INTO STUDENT(ID,SCHOOL,NAME) VALUES(2,'sysu','sun')";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        System.out.println(plan);
    }

    // 测试delete
    @Test
    public void testDelete() throws QueryException {
        TableNode table = new TableNode("STUDENT");
        ASTNode delete = ((TableNode) table.query("ID>=5&&ID<=5"));// .delete();
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(delete, null, null);
        System.out.println(plan);

        String sql = "DELETE FROM STUDENT WHERE ID>=5&&ID<=5";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        System.out.println(plan);

    }

    @Test
    public void testDelete_范围删除会生成merge() throws QueryException {
        TableNode table = new TableNode("STUDENT");
        DeleteNode delete = ((TableNode) table.query("ID>=5")).delete();
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(delete, null, null);
        Assert.assertTrue(plan instanceof IMerge);
        System.out.println(plan);

        String sql = "DELETE FROM STUDENT WHERE ID>=5";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        System.out.println(plan);
    }

    // 测试Insert
    @Test
    public void testInsert() throws QueryException {
        TableNode table = new TableNode("STUDENT");
        Comparable values[] = { 2 };
        InsertNode insert = table.insert("ID", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(insert, null, null);
        System.out.println(plan);

        String sql = "INSERT INTO STUDENT(ID) VALUES(2)";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        System.out.println(plan);
    }

    // 测试Insert
    @Test
    public void testInsert_全字段() throws QueryException {
        TableNode table = new TableNode("STUDENT");
        Comparable values[] = { 2, "sysu", "sun" };
        InsertNode insert = table.insert("ID SCHOOL NAME", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(insert, null, null);
        System.out.println(plan);

        String sql = "INSERT INTO STUDENT VALUES(2,'sysu','sun')";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        System.out.println(plan);
    }

}
