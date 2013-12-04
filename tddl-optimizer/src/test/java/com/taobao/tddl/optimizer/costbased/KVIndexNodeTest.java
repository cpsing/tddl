package com.taobao.tddl.optimizer.costbased;

import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.exceptions.QueryException;

public class KVIndexNodeTest extends BaseOptimizerTest {

    @Test
    public void testNormal() throws QueryException {

        KVIndexNode studentID = new KVIndexNode("STUDENT._ID");
        studentID.select("ID,NAME,SCHOOL");
        studentID.keyQuery("ID=1");
        studentID.valueQuery("SCHOOL=1 AND NAME = 333");
        studentID.build();

        IQuery q = (IQuery) studentID.toDataNodeExecutor();
        System.out.println(q);
    }

    @Test
    public void testJoin() throws QueryException {
        KVIndexNode studentID = new KVIndexNode("STUDENT._ID");
        studentID.alias("STUDENT");
        studentID.select("ID,NAME,SCHOOL");

        KVIndexNode studentName = new KVIndexNode("STUDENT._NAME");
        studentName.alias("STUDENT._NAME");
        studentName.select("ID,NAME");
        studentName.keyQuery("NAME=1");

        studentID.valueQuery("SCHOOL=1");

        JoinNode join = studentName.join(studentID).addJoinKeys("ID", "ID");
        // build之前的操作順序可以任意
        join.build();

        System.out.println(join);
    }
}
