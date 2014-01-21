package com.taobao.tddl.common.utils.convertor;

/**
 * 数据类型转化
 * 
 * @author jianghang 2014-1-21 上午12:08:57
 * @since 5.1.0
 */
public interface Convertor {

    public Object convert(Object src, Class destClass);

}
