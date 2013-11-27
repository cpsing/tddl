package com.taobao.tddl.optimizer.costbased.after;
//package com.taobao.ustore.optimizer.impl.afters;
//
//import java.util.Map;
//
//import com.taobao.ustore.common.exception.EmptyResultRestrictionException;
//import com.taobao.ustore.common.inner.bean.IDataNodeExecutor;
//import com.taobao.ustore.common.inner.bean.IJoin;
//import com.taobao.ustore.common.inner.bean.IMerge;
//import com.taobao.ustore.common.inner.bean.IQuery;
//import com.taobao.ustore.common.inner.bean.IQueryCommon;
//import com.taobao.ustore.common.inner.bean.ParameterContext;
//import com.taobao.ustore.optimizer.QueryPlanOptimizer;
//import com.taobao.ustore.optimizer.context.OptimizerContext;
//import com.taobao.ustore.optimizer.util.BoolUtil;
//
//public class MergeFilterOptimizer implements QueryPlanOptimizer {
//
//	private OptimizerContext oc;
//
//	public MergeFilterOptimizer(OptimizerContext oc) {
//		this.oc = oc;
//	}
//
//	/**
//	 * 如果设置了OptimizerExtraCmd.MergeConcurrent 并且值为True，则将所有的Merge变为并行
//	 * @throws Exception 
//	 */
//	@Override
//	public IDataNodeExecutor optimize(IDataNodeExecutor dne,
//			Map<Integer, ParameterContext> parameterSettings,
//			Map<String, Comparable> extraCmd) throws EmptyResultRestrictionException {
//		
//		this.mergeRestriction(dne);
//		
//		return dne;
//
//	}
//
//	public void mergeRestriction(IDataNodeExecutor node)
//			throws EmptyResultRestrictionException {
//		BoolUtil e = new BoolUtil(oc);
//		if (node instanceof IMerge) {
//			for (int i = 0; i < ((IMerge) node).getSubNode().size(); i++) {
//				this.mergeRestriction( ((IMerge) node)
//						.getSubNode().get(i));
//			}
//		}
//
//		if (node instanceof IJoin) {
//
//			this.mergeRestriction(((IJoin) node).getLeftNode());
//			this.mergeRestriction(((IJoin) node).getRightNode());
//		}
//
//		if (node instanceof IQuery) {
//			((IQuery) node)
//					.setKeyFilter(e.merge(((IQuery) node).getKeyFilter()));
//		}
//		if(node instanceof IQueryCommon)
//			((IQueryCommon) node).setValueFilter(e.merge(((IQueryCommon) node).getResultSetFilter()));
//	}
//
//}
