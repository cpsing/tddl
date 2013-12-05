//package com.taobao.ustore.jdbc.common.command;
//
//import java.util.Map;
//
//import com.taobao.ustore.common.inner.ICursorMeta;
//import com.taobao.ustore.common.inner.ISchematicCursor;
//import com.taobao.ustore.common.inner.bean.ExtraCmd;
//import com.taobao.ustore.common.inner.bean.IDataNodeExecutor;
//import com.taobao.ustore.common.inner.bean.IJoin;
//import com.taobao.ustore.common.util.GeneralUtil;
//import com.taobao.ustore.jdbc.common.CursorMyUtils;
//import com.taobao.ustore.jdbc.common.DatasourceMySQLImplement;
//import com.taobao.ustore.jdbc.common.cursor.JoinMyCursor;
//import com.taobao.ustore.jdbc.mysql.My_Cursor;
//import com.taobao.ustore.jdbc.mysql.My_JdbcHandler;
//import com.taobao.ustore.optimizer.impl.context.AndorContext;
//import com.taobao.ustore.qe.QueryHandlerCommon;
//import com.taobao.ustore.spi.DataSourceGetter;
//import com.taobao.ustore.spi.ExecutionContext;
//
//public class JoinMyHandler extends QueryHandlerCommon {
//
//    protected DataSourceGetter dsGetter;
//
//    public JoinMyHandler(AndorContext commonConfig){
//        super(commonConfig);
//        dsGetter = new DatasourceMySQLImplement();
//    }
//
//    @Override
//    public ISchematicCursor handle(Map<String, Comparable> context, ISchematicCursor cursor,
//                                   IDataNodeExecutor executor, ExecutionContext executionContext) throws Exception {
//        return doQuery(context, cursor, executor, executionContext);
//    }
//
//    public ISchematicCursor doQuery(Map<String, Comparable> context, ISchematicCursor cursor,
//                                    IDataNodeExecutor executor, ExecutionContext executionContext) throws Exception {
//
//        if (cursor != null) {
//            throw new UnsupportedOperationException();
//        }
//        IJoin ijoin = (IJoin) executor;
//        // 直接用左表indeKey
//        // nestBuildTableAndSchema(ijoin.getGroupDataNode(),executionContext,iquery.getIndexName(),iquery.getDbName(),
//        // false);
//
//        My_JdbcHandler jdbcHandler = CursorMyUtils.getJdbcHandler(dsGetter, context, executor, executionContext);
//        jdbcHandler.setExtraCmd(context);
//        ICursorMeta meta = GeneralUtil.convertToICursorMeta(ijoin);
//        My_Cursor my_cursor = new My_Cursor(jdbcHandler, meta, ijoin.getGroupDataNode(), null, ijoin.isStreaming(),this.andorContext);
//        my_cursor.setCursorMeta(meta);
//        my_cursor.setGroupNodeName(ijoin.getGroupDataNode());
//        JoinMyCursor c = new JoinMyCursor(my_cursor, meta, ijoin, jdbcHandler, ijoin.isStreaming());
//        if ("True".equalsIgnoreCase(GeneralUtil.getExtraCmd(context,
//            ExtraCmd.ExecutionExtraCmd.EXECUTE_QUERY_WHEN_CREATED))) {
//            c.init();
//        }
//        return c;
//    }
//
//}
