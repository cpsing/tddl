package com.taobao.tddl.common.utils.convertor;

/**
 * @author jianghang 2011-6-21 下午03:46:57
 */
public class AbastactConvertor implements Convertor {

    @Override
    public Object convert(Object src, Class destClass) {
        throw new ConvertorException("unSupport!");
    }

    @Override
    public Object convertCollection(Object src, Class destClass, Class... componentClasses) {
        throw new ConvertorException("unSupport!");
    }

}
