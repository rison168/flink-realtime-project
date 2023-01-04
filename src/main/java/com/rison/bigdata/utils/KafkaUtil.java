package com.rison.bigdata.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @packageName: com.rison.bigdata.utils
 * @className: KafaUtil
 * @author: Rison
 * @date: 2023/1/4 10:44
 * @description:
 **/
public class KafkaUtil {
    private static final String KAFKA_SERVERS = "";

    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String groupId){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(
                topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        if (consumerRecord == null || consumerRecord.value() == null){
                            return "";
                        }else {
                            return String.valueOf(consumerRecord.value());
                        }
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                },
                properties
        );
    }

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic){
        return new FlinkKafkaProducer<String>(
                KAFKA_SERVERS,
                topic,
                new SimpleStringSchema()
        );
    }

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic, String defaultTopic){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        return new FlinkKafkaProducer<String>(
                defaultTopic,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long aLong) {
                        if (element == null){
                            return new ProducerRecord<>(topic, "".getBytes());
                        }
                        return new ProducerRecord<>(topic, element.getBytes());
                    }
                },
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public String getKafkaSourceDDL(String topic, String groupId){
        return  " with ('connector' = 'kafka', \n" +
                " 'topic' = '" + topic + "', \n" +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVERS + "', \n" +
                " 'properties.group.id' = '" + groupId + "', \n" +
                " 'format' = 'json', \n" +
                " 'scan.startup.mode' = 'group-offsets')";
    }

    public String getKafkaSinkDDL(String topic){
        return " WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVERS + "', " +
                "  'format' = 'json' " +
                ")";
    }

    public String getKafkaUpsertDDL(String topic){
        return  " WITH ( " +
                "  'connector' = 'upsert-kafka', \n" +
                "  'topic' = '" + topic + "', \n" +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVERS + "', \n" +
                "  'key.format' = 'json', \n" +
                "  'value.format' = 'json' \n" +
                ")";
    }


    public String getTopicDb(String topic, String groupId){
        return "CREATE TABLE topic_db ( \n" +
                " `database` STRING, \n" +
                " `table` STRING, \n" +
                " `type` STRING, \n" +
                " `data` MAP<STRING, STRING>, \n" +
                " `old` MAP<STRING, STRING>, \n" +
                " `pt` AS PROCTIME(), \n" +
                " `table` STRING, \n" +
                ") \n" +
                getKafkaSourceDDL(topic, groupId);
    }

}
