package com.rison.bigdata.application.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rison.bigdata.bean.TableProcess;
import com.rison.bigdata.common.ConstantConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @packageName: com.rison.bigdata.application.func
 * @className: TableProcessFunction
 * @author: Rison
 * @date: 2023/1/5 10:17
 * @description:
 **/
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    Logger logger = LoggerFactory.getLogger(TableProcessFunction.class);

    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
        Logger logger = LoggerFactory.getLogger(TableProcessFunction.class);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (connection == null){
            connection = DriverManager.getConnection(ConstantConfig.PHOENIX_SERVER);
        }
    }

    //value:{"database":"gmall-211126-flink","table":"base_trademark","type":"update","ts":1652499176,"xid":188,"commit":true,"data":{"id":13,"tm_name":"atguigu","logo_url":"/bbb/bbb"},"old":{"logo_url":"/aaa/aaa"}}
    //value:{"database":"gmall-211126-flink","table":"order_info","type":"update","ts":1652499176,"xid":188,"commit":true,"data":{"id":13,...},"old":{"xxx":"/aaa/aaa"}}
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        //TODO 1. 获取广播流
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String table = jsonObject.getString("table");
        TableProcess tableProcess = broadcastState.get(table);
        if (tableProcess != null) {
            filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());
            jsonObject.put("sinkTable", tableProcess.getSinkTable());
            collector.collect(jsonObject);
        }else {
            logger.error("## DIM not find table");
        }

    }

    //value:{"before":null,"after":{"source_table":"aa","sink_table":"bb","sink_columns":"cc","sink_pk":"id","sink_extend":"xxx"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1652513039549,"snapshot":"false","db":"gmall-211126-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1652513039551,"transaction":null}
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

        //TODO 1. 解析数据
        com.alibaba.fastjson.JSONObject jsonObject = JSON.parseObject(value);
        String afterData = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(afterData, TableProcess.class);

        //TODO 2. 检查hbase & 建表
        checkTable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());

        //TODO 3. 广播数据

        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(), tableProcess);


    }

    /**
     * create table if not exists db.tn(id varchar primary key,bb varchar,cc varchar) xxx
     * @param sinkTable Phoenix表名
     * @param sinkColumns Phoenix表字段
     * @param sinkPk Phoenix表主键
     * @param sinkExtend Phoenix表扩展字段
     */
    public void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend){
        PreparedStatement preparedStatement = null;
        if (sinkPk == null || "".equals(sinkPk)){
            sinkPk = "id";
        }
        if (sinkExtend == null){
            sinkExtend = "";
        }

        StringBuilder createTableSQL = new StringBuilder("create table if not exists ")
                .append(ConstantConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i ++){
            String column = columns[i];
            if (sinkPk.equals(column)){
                createTableSQL.append(column).append(" varchar primary key");
            }else {
                createTableSQL.append(column).append( "varchar");
            }
            //判断是否最后一个字段
            if (i < columns.length - 1){
                createTableSQL.append(", \n");
            }
            createTableSQL.append(") \n").append(sinkColumns);
            logger.info("## create DIM  hbase table sql : {}", createTableSQL);
            try {
                preparedStatement  = connection.prepareStatement(createTableSQL.toString());
            } catch (SQLException e) {
                logger.error("DIM hbase table create fail !");
                throw new RuntimeException(e);
            }finally {
                if (preparedStatement != null){
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    /**
     * 过滤字段
     *
     * @param data        {"id":13,"tm_name":"atguigu","logo_url":"/bbb/bbb"}
     * @param sinkColumns "id,tm_name"
     */
    private void filterColumn(JSONObject data, String sinkColumns) {

        //切分sinkColumns
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }

}
