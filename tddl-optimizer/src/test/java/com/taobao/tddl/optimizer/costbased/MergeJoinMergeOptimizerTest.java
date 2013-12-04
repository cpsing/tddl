package com.taobao.tddl.optimizer.costbased;

import org.codehaus.groovy.syntax.ParserException;
import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.costbased.after.MergeJoinMergeOptimizer;
import com.taobao.tddl.optimizer.exceptions.QueryException;

public class MergeJoinMergeOptimizerTest extends BaseOptimizerTest {

    private MergeJoinMergeOptimizer o = new MergeJoinMergeOptimizer();

    @Test
    public void testExpandLeft() throws ParserException, QueryException {
        System.out.println("==========testExpandLeft==============");
        IJoin j = this.getMergeJoinMerge();
        System.out.println(j);
        IQueryTree res = o.expandLeft(j);
        System.out.println(res);
        System.out.println("========================");
        j = this.getMergeJoinQuery();
        System.out.println(j);
        res = o.expandLeft(j);
        System.out.println(res);
        System.out.println("========================");
        j = this.getQueryJoinMerge();
        System.out.println(j);
        res = o.expandLeft(j);
        System.out.println(res);
        System.out.println("========================");
        j = this.getQueryJoinQuery();
        System.out.println(j);
        res = o.expandLeft(j);
        System.out.println(res);
        System.out.println("========================");
    }

    @Test
    public void testExpandRight() throws ParserException, QueryException {
        System.out.println("==========testExpandRight==============");
        IJoin j = this.getMergeJoinMerge();
        System.out.println(j);
        IQueryTree res = o.expandRight(j);
        System.out.println(res);
        System.out.println("========================");
        j = this.getMergeJoinQuery();
        System.out.println(j);
        res = o.expandRight(j);
        System.out.println(res);
        System.out.println("========================");
        j = this.getQueryJoinMerge();
        System.out.println(j);
        res = o.expandRight(j);
        System.out.println(res);
        System.out.println("========================");
        j = this.getQueryJoinQuery();
        System.out.println(j);
        res = o.expandRight(j);
        System.out.println(res);
        System.out.println("========================");
    }

    @Test
    public void testCartesianProduct() throws ParserException, QueryException {
        System.out.println("==========testCartesianProduct==============");
        IJoin j = this.getMergeJoinMerge();
        System.out.println(j);
        IQueryTree res = o.cartesianProduct(j);
        System.out.println(res);
        System.out.println("========================");
        j = this.getMergeJoinQuery();
        System.out.println(j);
        res = o.cartesianProduct(j);
        System.out.println(res);
        System.out.println("========================");
        j = this.getQueryJoinMerge();
        System.out.println(j);
        res = o.cartesianProduct(j);
        System.out.println(res);
        System.out.println("========================");
        j = this.getQueryJoinQuery();
        System.out.println(j);
        res = o.cartesianProduct(j);
        System.out.println(res);
        System.out.println("========================");
    }

    private IJoin getMergeJoinMerge() {
        IJoin j = this.getJoin();
        j.setLeftOuter(true);
        j.setRightOuter(true);
        IQuery q1 = this.getQuery(1);
        IQuery q2 = this.getQuery(2);
        IQuery q3 = this.getQuery(3);
        IQuery q4 = this.getQuery(4);
        IQuery q5 = this.getQuery(5);
        IQuery q6 = this.getQuery(6);

        IMerge leftMerge = this.getMerge(7);
        IMerge rightMerge = this.getMerge(8);

        leftMerge.addSubNode(q1).addSubNode(q2).addSubNode(q3);
        rightMerge.addSubNode(q4).addSubNode(q5).addSubNode(q6);

        j.setLeftNode(leftMerge);
        j.setRightNode(rightMerge);
        return j;
    }

    private IJoin getQueryJoinMerge() {
        IJoin j = this.getJoin();
        j.setLeftOuter(true);
        j.setRightOuter(true);
        IQuery q1 = this.getQuery(1);
        IQuery q4 = this.getQuery(4);
        IQuery q5 = this.getQuery(5);
        IQuery q6 = this.getQuery(6);

        IMerge rightMerge = this.getMerge(8);
        rightMerge.addSubNode(q4).addSubNode(q5).addSubNode(q6);

        j.setLeftNode(q1);
        j.setRightNode(rightMerge);
        return j;
    }

    private IJoin getMergeJoinQuery() {
        IJoin j = this.getJoin();
        j.setLeftOuter(true);
        j.setRightOuter(true);
        IQuery q1 = this.getQuery(1);
        IQuery q2 = this.getQuery(2);
        IQuery q3 = this.getQuery(3);
        IQuery q4 = this.getQuery(4);

        IMerge leftMerge = this.getMerge(7);
        leftMerge.addSubNode(q1).addSubNode(q2).addSubNode(q3);

        j.setLeftNode(leftMerge);
        j.setRightNode(q4);
        return j;
    }

    private IJoin getQueryJoinQuery() {
        IJoin j = this.getJoin();
        j.setLeftOuter(true);
        j.setRightOuter(true);
        IQuery q1 = this.getQuery(1);
        IQuery q4 = this.getQuery(4);

        j.setLeftNode(q1);
        j.setRightNode(q4);
        return j;
    }

    private IQuery getQuery(Integer id) {
        IQuery q = ASTNodeFactory.getInstance().createQuery();
        q.setAlias(id.toString());
        return q;
    }

    private IMerge getMerge(Integer id) {
        IMerge m = ASTNodeFactory.getInstance().createMerge();
        m.setAlias(id.toString());
        return m;
    }

    private IJoin getJoin() {
        return ASTNodeFactory.getInstance().createJoin();
    }
}
