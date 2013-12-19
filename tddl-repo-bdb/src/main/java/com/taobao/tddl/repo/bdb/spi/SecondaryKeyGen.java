///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package com.taobao.ustore.repo.je;
//
//import java.io.StringReader;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
//import org.apache.lucene.analysis.Analyzer;
//import org.apache.lucene.analysis.TokenStream;
//import org.apache.lucene.analysis.cjk.CJKAnalyzer;
//import org.apache.lucene.analysis.tokenattributes.TermAttribute;
//import org.apache.lucene.util.Version;
//
//import com.taobao.ustore.common.config.schema.internal.ColumnMeta;
//import com.taobao.ustore.common.config.schema.internal.IndexMeta;
//import com.taobao.ustore.common.inner.CodecFactory;
//import com.taobao.ustore.common.inner.KVPair;
//import com.taobao.ustore.common.inner.bean.CloneableRecord;
//import com.taobao.ustore.common.inner.bean.RecordCodec;
//import com.taobao.ustore.common.util.ExecUtil;
//
///**
// * 从主索引记录中生成二级索引键值
// *
// * @author jianxing <jianxing.qx@taobao.com>
// */
//public class SecondaryKeyGen {
//
//    IndexMeta secondaryMeta;
//    IndexMeta primaryMeta;
//    static Analyzer analyzer = new CJKAnalyzer(Version.LUCENE_35);
//
//    public SecondaryKeyGen(IndexMeta primaryMeta, IndexMeta secondaryMeta) {
//        this.primaryMeta = primaryMeta;
//        this.secondaryMeta = secondaryMeta;
//    }
//
//    final String getCacheKey(ColumnMeta[] columns, String prefix) {
//        String cacheKey = primaryMeta.getTableName() + "_" + prefix;
//        for (ColumnMeta column : columns) {
//            cacheKey += "_" + column.getName();
//        }
//        return cacheKey;
//    }
//
//    /**
//     * todo:这里默认把主键作为二级索引的值，需要改成指定列的值。
//     *
//     * @param primaryKey
//     * @param primaryValue
//     * @return
//     */
//    public KVPair createSecondaryRecord(CloneableRecord primaryKey, CloneableRecord primaryValue) {
//        //RecordCodec secondaryKeyCodec = ThreadLocalCodec.getThreadLocalCodec(cache_key_sk, secondaryMeta.getKeyColumns())[0];
//        RecordCodec secondaryKeyCodec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(Arrays.asList(secondaryMeta.getKeyColumns()));
//        CloneableRecord record = secondaryKeyCodec.newEmptyRecord();
//        for (ColumnMeta column : secondaryMeta.getKeyColumns()) {
//            String columnName = column.getName();
//            Object value = null;
// 
//            if( (value =ExecUtil.get(column, primaryValue)) == null ){
//                
//                value = ExecUtil.get(column, primaryKey);
//            }
//            
//            record.put(columnName, value);
//        }
//            
//        //System.out.println("生成二级索引:"+record+","+primaryKey);
//        return new KVPair(record, primaryKey);
//    }
//
//    public KVPair createSecondaryRecord(byte[] primaryKey, byte[] primaryValue) {
//        //RecordCodec primaryKeyCodec = ThreadLocalCodec.getThreadLocalCodec(cache_key_pk, primaryMeta.getKeyColumns())[0];
//        RecordCodec primaryKeyCodec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(Arrays.asList(primaryMeta.getKeyColumns()));
//        RecordCodec primaryValueCodec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(Arrays.asList(primaryMeta.getValueColumns()));
//        return createSecondaryRecord(primaryKeyCodec.decode(primaryKey, true), primaryValueCodec.decode(primaryValue, true));
//    }
//
//    public List<KVPair> createSecondaryRecords(byte[] primaryKey, byte[] primaryValue) {
//        //RecordCodec primaryKeyCodec = ThreadLocalCodec.getThreadLocalCodec(cache_key_pk, primaryMeta.getKeyColumns())[0];
//        RecordCodec primaryKeyCodec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(Arrays.asList(primaryMeta.getKeyColumns()));
//        RecordCodec primaryValueCodec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(Arrays.asList(primaryMeta.getValueColumns()));
//        return createSecondaryRecords(primaryKeyCodec.decode(primaryKey, true), primaryValueCodec.decode(primaryValue, true));
//    }
//
//    public List<KVPair> createSecondaryRecords(CloneableRecord primaryKey, CloneableRecord primaryValue) {
//        List<KVPair> ret = new ArrayList();
//        RecordCodec secondaryKeyCodec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(Arrays.asList(secondaryMeta.getKeyColumns()));
//        for (ColumnMeta column : secondaryMeta.getKeyColumns()) {
//            String columnName = column.getName();
//            Object value = null;
//            if (ExecUtil.get(column, primaryValue) != null) {
//                value = ExecUtil.get(column, primaryValue).toString();
//            } else if (ExecUtil.get(column, primaryKey) != null) {
//                value = ExecUtil.get(column, primaryKey).toString();
//            }
//            //analyze value.
//            if (!(value instanceof String)) {
//                throw new RuntimeException("secondary key field must be string.");
//            }
//            TokenStream ts = analyzer.tokenStream("", new StringReader((String) value));
//            ts.addAttribute(TermAttribute.class);
//            try {
//                while (ts.incrementToken()) {
//                    CloneableRecord record = secondaryKeyCodec.newEmptyRecord();
//                    String term = ts.getAttribute(TermAttribute.class).term();
//                    record.put(columnName, term);
//                    ret.add(new KVPair(record, primaryKey));
//                }
//            } catch (Exception ex) {
//                throw new RuntimeException("analyze error", ex);
//            }
//        }
//        //System.out.println("生成二级索引:"+kv.key+","+kv.value);
//        //return new KVPair(record, primaryKey);
//        return ret;
//    }
//
//    public RecordCodec getSecondaryKeyCodec() {
//         return CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(Arrays.asList(secondaryMeta.getKeyColumns()));
//    }
//
//    public static List<String> analyze(String s) {
//        List<String> ret = new ArrayList();
//
//        TokenStream ts = analyzer.tokenStream("", new StringReader(s));
//        ts.addAttribute(TermAttribute.class);
//        try {
//            while (ts.incrementToken()) {
//                String term = ts.getAttribute(TermAttribute.class).term();
//                ret.add(term);
//            }
//        } catch (Exception ex) {
//            throw new RuntimeException("analyze error", ex);
//        }
//        return ret;
//    }
//
//    public static void main(String[] args) {
//        System.out.println(analyze("0,1 1"));
//    }
//
//    public static Analyzer getAnalyzer() {
//        return analyzer;
//    }
//
//    public static void setAnalyzer(Analyzer analyzer) {
//        SecondaryKeyGen.analyzer = analyzer;
//    }
//}
