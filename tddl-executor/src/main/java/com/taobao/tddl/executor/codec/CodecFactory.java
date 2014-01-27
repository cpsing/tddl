package com.taobao.tddl.executor.codec;

import java.util.List;

import com.taobao.tddl.optimizer.config.table.ColumnMeta;

/**
 * @author mengshi.sunmengshi 2013-12-2 下午6:24:37
 * @since 5.0.0
 */
public abstract class CodecFactory {

    public static final String AVRO         = "avro";
    public static final String FIXED_LENGTH = "fixedLength";

    // public static CodecFactory avro = new AvroCodecFactory();
    public static CodecFactory fixedLength  = new FixedLengthCodecFactory();

    public static CodecFactory getInstance(String s) {
        if (AVRO.equals(s)) {
            return fixedLength;
        } else if (FIXED_LENGTH.equals(s)) {
            return fixedLength;
        }
        return null;
    }

    public abstract RecordCodec getCodec(List<ColumnMeta> columns);

    // //外部通过配置使用class.forName加载
    // public static class AvroCodecFactory extends CodecFactory{
    //
    // //缓存schema
    // Map<List<ColumnMeta>,Schema> schemaCache = new ConcurrentHashMap();
    //
    // @Override
    // public RecordCodec getCodec(List<ColumnMeta> columns) {
    // Schema schema = schemaCache.get(columns);
    // if(schema == null){
    // schema = AvroCodec.generateAvroSchema(columns);
    // schemaCache.put(columns, schema);
    // }
    // return new AvroCodec(schema);
    // }
    //
    // }

    public static class FixedLengthCodecFactory extends CodecFactory {

        @Override
        public RecordCodec getCodec(List<ColumnMeta> columns) {
            return new FixedLengthCodec(columns);
        }

    }

}
