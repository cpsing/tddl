package com.taobao.tddl.optimizer.core.expression;

/**
 * 代表一个列信息，非函数列
 * 
 * @author jianxing <jianxing.qx@taobao.com>
 * @author whisper
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 * @since 5.0.0
 */
public interface IColumn extends ISelectable<IColumn> {

    public static final String STAR = "*";
}
