package com.rison.bigdata.application.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rison.bigdata.application.func.TableProcessFunction;
import com.rison.bigdata.bean.TableProcess;
import com.rison.bigdata.utils.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @packageName: com.rison.bigdata.application.dim
 * @className: DimApplication
 * @author: Rison
 * @date: 2023/1/4 13:57
 * @description:
 **/
public class DimApplication {
    private static Logger logger = LoggerFactory.getLogger(DimApplication.class);

    public static void main(String[] args) {
        //TODO 1. 获取flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 2. 设置checkpoint & stateBackend
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5 * 1000L));

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hdfsCluster/flink/dim/ck");

        //TODO 3. 消费dim kafka 数据
        DataStreamSource<String> dimKafkaDS = env.addSource(KafkaUtil.getFlinkKafkaConsumer("DIM_TOPIC", "DIM_TOPIC_001"));

        //TODO 4.过滤掉非JSON数据&保留新增、变化以及初始化数据并将数据转换为JSON格式
        SingleOutputStreamOperator<JSONObject> filterDS = dimKafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> collector) throws Exception {
                try {
                    //将数据转换为JSON
                    JSONObject jsonObject = JSON.parseObject(value);
                    //获取操作类型
                    String type = jsonObject.getString("type");
                    //保留新增/更新/初始化数据
                    if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    logger.error("dim application data is : {}", e);

                }
            }
        });

        //TODO 5. 获取mysql配置表CDC数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("tbds-10-1-0-50")
                .port(3306)
                .username("root")
                .password("metadata@Tbds.com")
                .databaseList("rison_app")
                .tableList("rison_app.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql source");

        //TODO 6. 将cdc数据设置为broadcast flow
        MapStateDescriptor<String, TableProcess> mysqlDesc = new MapStateDescriptor("map-state", String.class, TableProcess.class);
        BroadcastStream<String> mysqlBroadcastDS = mysqlDS.broadcast(mysqlDesc);


        //TODO 7. 主流和广播流connect
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(mysqlBroadcastDS);

        //TODO 8. 处理连接流,根据配置信息处理主流数据
        SingleOutputStreamOperator<JSONObject> process = connectedStream.process(new TableProcessFunction(mysqlDesc));


    }
}
