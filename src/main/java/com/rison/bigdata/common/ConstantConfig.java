package com.rison.bigdata.common;

/**
 * @packageName: com.rison.bigdata.common
 * @className: ConstantConfig
 * @author: Rison
 * @date: 2023/1/5 14:38
 * @description:
 **/
public class ConstantConfig {
    // Phoenix库名
    public static final String HBASE_SCHEMA = "RISON_DIM_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/rison_app";
}
