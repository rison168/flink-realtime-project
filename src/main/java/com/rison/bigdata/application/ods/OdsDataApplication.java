package com.rison.bigdata.application.ods;

import com.rison.bigdata.utils.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @packageName: com.rison.bigdata.application.ods
 * @className: OdsDataApplication
 * @author: Rison
 * @date: 2022/12/30 15:03
 * @description:
 **/
public class OdsDataApplication {
    public static void main(String[] args) throws Exception {
        //TODO 1. 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启CheckPoint
        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        env.setStateBackend(new FsStateBackend("hdfs://hdfsCluster/flink/rison/data/ck"));
        env.setParallelism(1);
        //TODO 2. source mysql-cdc
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("tbds-10-1-0-50")
                .port(3306)
                .databaseList("rison_app")
//                .tableList("rison_app.activity_info")
                .username("root")
                .password("metadata@Tbds.com")
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        dataStreamSource.setParallelism(1)
                .print().setParallelism(1);


        dataStreamSource.addSink(KafkaUtil.getFlinkKafkaProducer("ODS_DATA_TOPIC"));

        env.execute("ods_data");

    }
}

/*
/usr/hdp/2.2.0.0-2041/flink/bin/flink run \
-m yarn-cluster \
-ytm 4g \
-p 1 \
-c com.rison.bigdata.application.ods.OdsDataApplication \
/root/rison/flink-realtime-project-1.0-SNAPSHOT.jar
 */
