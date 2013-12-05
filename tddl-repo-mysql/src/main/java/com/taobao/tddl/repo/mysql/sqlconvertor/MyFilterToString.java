package com.taobao.tddl.repo.mysql.sqlconvertor;
///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.taobao.ustore.sqlconvertor;
//
//import java.util.Map;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import com.taobao.ustore.common.inner.bean.IBooleanFilter;
//import com.taobao.ustore.common.inner.bean.IFilter;
//import com.taobao.ustore.common.inner.bean.IFilter.OPERATION;
//import com.taobao.ustore.common.inner.bean.IFunction;
//import com.taobao.ustore.common.inner.bean.ILogicalFilter;
//import com.taobao.ustore.common.inner.bean.ISelectable;
//import com.taobao.ustore.common.inner.bean.ParameterContext;
//
///**
// * 
// * @author Dreamond
// */
//public class MyFilterToString {
//
//	public static String toSimpleString(boolean bindVal, IFilter filter,
//			AtomicInteger bindValSequence,
//			Map<Integer, ParameterContext> param, Map<String, String> tabMap) {
//		String res = toSimpleString2(bindVal, filter, bindValSequence, param);
//		if (res == null)
//			return res;
//		return res;
//	}
//
//	private static String toSimpleString2(boolean bindVal, IFilter filter,
//			AtomicInteger bindValSequence, Map<Integer, ParameterContext> param) {
//		if (filter == null) {
//			return null;
//		}
//		if (filter instanceof IBooleanFilter) {
//			boolean dontUseTableNameInQuery = false;
//			return processBoolFilterForToString2(dontUseTableNameInQuery,
//					bindVal, filter, bindValSequence, param, null);
//		} else if (filter instanceof ILogicalFilter) {
//			ILogicalFilter lf = (ILogicalFilter) filter;
//			StringBuilder sb = new StringBuilder();
//
//			sb.append("(").append(
//					toSimpleString2(bindVal, lf.getSubFilter().get(0),
//							bindValSequence, param));
//			for (int i = 1; i < lf.getSubFilter().size(); i++) {
//				sb.append(operationtoString2(lf.getOperation())
//						+ toSimpleString2(bindVal, lf.getSubFilter().get(i),
//								bindValSequence, param));
//			}
//			sb.append(")");
//			return sb.toString();
//		}
//		return null;
//	}
//
//	public static String toString(boolean bindVal, IFilter filter,
//			AtomicInteger bindValSequence,
//			Map<Integer, ParameterContext> param, OneQuery oneQuery) {
//		String res = toString2(bindVal, filter, bindValSequence, param,
//				oneQuery);
//		if (res == null)
//			return res;
//		return res;
//	}
//
//	public static String buildWhereBuilder(boolean bindVal,
//			StringBuilder builder, AtomicInteger bindValSequence,
//			Map<Integer, ParameterContext> paramMap, OneQuery oneQuery) {
//		StringBuilder whereBuilder = new StringBuilder();
//		IFilter filter = oneQuery.filter;
//		if (filter != null) {
//			whereBuilder.append(MyFilterToString.toString(bindVal, filter,
//					bindValSequence, paramMap, oneQuery));
//		}
//		String whereToken = whereBuilder.toString();
//		return whereToken;
//	}
//
//	public static String join(String[] ss, String j) {
//		if (ss.length == 1) {
//			return ss[0];
//		}
//
//		StringBuilder res = new StringBuilder();
//		res.append(ss[0]);
//		for (int i = 1; i < ss.length; i++) {
//			res.append(j).append(ss[i]);
//		}
//
//		return res.toString();
//	}
//
//	static String toString2(boolean bindVal, IFilter filter,
//			AtomicInteger bindValSequence,
//			Map<Integer, ParameterContext> param, OneQuery oneQuery) {
//		if (filter == null) {
//			return null;
//		}
//		if (filter instanceof IBooleanFilter) {
//			boolean useTableNameInQuery = true;
//			return processBoolFilterForToString2(useTableNameInQuery, bindVal,
//					filter, bindValSequence, param, oneQuery);
//		} else if (filter instanceof ILogicalFilter) {
//			ILogicalFilter lf = (ILogicalFilter) filter;
//			StringBuilder sb = new StringBuilder();
//			sb.append("(").append(
//					toString2(bindVal, lf.getSubFilter().get(0),
//							bindValSequence, param, oneQuery));
//			for (int i = 1; i < lf.getSubFilter().size(); i++) {
//				sb.append(operationtoString2(lf.getOperation())
//						+ toString2(bindVal, lf.getSubFilter().get(i),
//								bindValSequence, param, oneQuery));
//			}
//			sb.append(")");
//			return sb.toString();
//		}
//		return null;
//	}
//
//	private static String processBoolFilterForToString2(
//			boolean useTableNameInQuery, boolean bindVal, IFilter filter,
//			AtomicInteger bindValSequence,
//			Map<Integer, ParameterContext> param, OneQuery oneQuery) {
//		IBooleanFilter bf = (IBooleanFilter) filter;
//		StringBuilder filterSb = new StringBuilder();
//		Object obj = bf.getColumn();
//		if(bf.isNot()){
//			filterSb.append(" not (");
//		}
//		processISeleatable(useTableNameInQuery, oneQuery, filterSb, obj,
//				bindVal, bindValSequence, param);
//
//		if (!bf.getOperation().equals(OPERATION.CONSTANT)) {
//			filterSb.append(" ").append(bf.getOperation().getOPERATIONString())
//			.append(" ");
//		} 
//		
//		obj = bf.getValue();
//		if(bf.getOperation().getOPERATIONString().equals("IS")){
//			filterSb.append(String.valueOf(obj));
//		}else{
//			if (obj != null) {
//				processISeleatable(useTableNameInQuery, oneQuery, filterSb, obj,
//						bindVal, bindValSequence, param);
//			} else if (bf.getValues() != null && !bf.getValues().isEmpty()) {
//				filterSb.append("(");
//				boolean first = true;
//				for (Object value : bf.getValues()) {
//					if (first) {
//						first = false;
//					} else {
//						filterSb.append(",");
//					}
//					processISeleatable(useTableNameInQuery, oneQuery, filterSb,
//							value, bindVal, bindValSequence, param);
//				}
//				filterSb.append(")");
//			}else if (obj==null && !bf.getOperation().equals(OPERATION.CONSTANT))
//			{
//				filterSb.append(" null ");
//			}
//		}
//		if(bf.isNot()){
//			filterSb.append(")");
//		}
//		return filterSb.toString();
//	}
//
//	/**
//	 * 
//	 * @param oneQuery
//	 * @param filterSb
//	 * @param obj
//	 * @param bindVal
//	 * @param bindValSequence
//	 * @param param
//	 */
//	private static void processISeleatable(boolean useTableNameInQuery,
//			OneQuery oneQuery, StringBuilder filterSb, Object obj,
//			boolean bindVal, AtomicInteger bindValSequence,
//			Map<Integer, ParameterContext> param) {
//		if (obj instanceof ISelectable) {
//			ISelectable is = (ISelectable) obj;
//			if (useTableNameInQuery) {
//				filterSb.append(JDBC_Utils.getColumnName(is, oneQuery, bindVal, bindValSequence, param,false));
//			} else if (is instanceof IFunction){
//				filterSb.append(JDBC_Utils.getColumnName(is, oneQuery, bindVal, bindValSequence, param,true));
//			}else {
//				if (is.getAlias() != null) {
//					filterSb.append(is.getAlias());
//				} else {
//					filterSb.append(is.getColumnName());
//				}
//			}
//		} else if (obj instanceof Comparable) {
//			filterSb.append(JDBC_Utils.getValue(bindVal, obj, bindValSequence, param));
//		} else {
//			filterSb.append(String.valueOf(obj));
//		}
//	}
//
//	public static String operationtoString2(OPERATION op) {
//		if (op == OPERATION.AND) {
//			return " and ";
//		} else if (op == OPERATION.OR) {
//			return " or ";
//		} else if (op == OPERATION.EQ) {
//			return " = ";
//		} else if (op == OPERATION.NOT_EQ) {
//			return " != ";
//		} else if (op == OPERATION.GT) {
//			return " > ";
//		} else if (op == OPERATION.GT_EQ) {
//			return " >= ";
//		} else if (op == OPERATION.IS_NOT_NULL) {
//			return " is not null ";
//		} else if (op == OPERATION.IS_NULL) {
//			return " is null ";
//		} else if (op == OPERATION.LIKE) {
//			return " like ";
//		} else if (op == OPERATION.LT) {
//			return " < ";
//		} else if (op == OPERATION.LT_EQ) {
//			return " <= ";
//		} else if (op == OPERATION.IN) {
//			return " in ";
//		} else if (op == OPERATION.NULL_SAFE_EQUAL) {
//			return " <=> ";
//		}
//
//		throw new IllegalArgumentException(" e... error " + op);
//	}
//}
