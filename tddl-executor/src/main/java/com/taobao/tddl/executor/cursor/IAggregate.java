///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.taobao.ustore.spi.cursor;
//
//import com.taobao.ustore.common.inner.KVPair;
//import com.taobao.ustore.common.inner.bean.IColumn;
//
///**
// *
// * @author jianxing <jianxing.qx@taobao.com> 
// */
//public interface IAggregate extends IFunction {
//    
//    void add(KVPair kv);
//    
//    void merge(KVPair kv);
//    
//    Object getResult();
//    
//    IColumn.DATA_TYPE getReturnType();
//    /**
//     * 获取aggrate name
//     * @return
//     */
//    public String getAggrateName();
//    
//    public String getAlias();
//    
//    public void setAlias(String alias);
//}
