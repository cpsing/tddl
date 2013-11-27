package com.taobao.tddl.optimizer.costbased.after;
//package com.taobao.ustore.optimizer.impl.afters;
//
//import java.util.Map;
//
//import com.taobao.ustore.common.inner.bean.IColumn;
//import com.taobao.ustore.common.inner.bean.IDataNodeExecutor;
//import com.taobao.ustore.common.inner.bean.IJoin;
//import com.taobao.ustore.common.inner.bean.IMerge;
//import com.taobao.ustore.common.inner.bean.IParallelizableQueryCommon.QUERY_CONCURRENCY;
//import com.taobao.ustore.common.inner.bean.IQuery;
//import com.taobao.ustore.common.inner.bean.IQueryCommon;
//import com.taobao.ustore.common.inner.bean.ISelectable;
//import com.taobao.ustore.common.inner.bean.ParameterContext;
//import com.taobao.ustore.optimizer.QueryPlanOptimizer;
//
//public class MergeAliasOptimizer implements QueryPlanOptimizer {
//
//	public MergeAliasOptimizer() {
//	}
//
//	/**
//	 * 如果设置了OptimizerExtraCmd.MergeConcurrent 并且值为True，则将所有的Merge变为并行
//	 */
//	@Override
//	public IDataNodeExecutor optimize(IDataNodeExecutor dne,
//			Map<Integer, ParameterContext> parameterSettings,
//			Map<String, Comparable> extraCmd) {
//
//		if (dne instanceof IQueryCommon) {
//			this.findMergeAndReomoveAliasInChild(dne);
//		}
//
//		return dne;
//	}
//
//	void findMergeAndReomoveAliasInChild(IDataNodeExecutor dne) {
//		if (dne instanceof IMerge) {
//			((IMerge) dne).setQueryConcurrency(QUERY_CONCURRENCY.SEQUENTIAL);
//
//			for (IDataNodeExecutor child : ((IMerge) dne).getSubNode()) {
//				if (child instanceof IQueryCommon) {
//					((IQueryCommon) child).setAlias(null);
//					for (ISelectable s : ((IQueryCommon) child).getColumns()) {
//						s.setAlias(null);
//
//						if (s instanceof IColumn) {
//							((IQueryCommon) child).setAlias(s.getTableName());
//						}
//
//					}
//					//索引的查询必须有别名，其它的可以没有
//					if (!(child instanceof IQuery && ((IQuery)child).getSubQuery()==null)) {
//							((IQueryCommon) child).setAlias(null);
//					}
//				}
//				this.findMergeAndReomoveAliasInChild(child);
//			}
//		}
//
//		if (dne instanceof IJoin) {
//			this.findMergeAndReomoveAliasInChild(((IJoin) dne).getLeftNode());
//			this.findMergeAndReomoveAliasInChild(((IJoin) dne).getRightNode());
//		}
//
//		if (dne instanceof IQuery && ((IQuery) dne).getSubQuery() != null) {
//			this.findMergeAndReomoveAliasInChild(((IQuery) dne).getSubQuery());
//
//		}
//
//	}
//}
