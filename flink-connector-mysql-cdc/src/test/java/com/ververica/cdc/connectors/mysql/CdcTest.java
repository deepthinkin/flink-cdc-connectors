package com.ververica.cdc.connectors.mysql;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class CdcTest {
    @Test
    public void testConsumingAllEvents_external_mysql() throws Exception {
        com.ververica.cdc.connectors.mysql.source.MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname("192.168.74.21")
                        .port(3306)
                        .databaseList("flinkcdc_test")
                        .tableList("flinkcdc_test.user_table_1_1,flinkcdc_test.user_table_1_2")
                        .username("root")
                        .password("root")
                        .serverId("5401-5404")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true).splitSize()
                        // output the schema changes as well
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 4
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
                .setParallelism(4)
                .print()
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
