package org.feather.flink.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: feather-flink
 * @package: org.feather.flink.sink
 * @className: SinkApp
 * @author: feather
 * @description:
 * @since: 2023-07-30 10:41
 * @version: 1.0
 */

public class SinkApp {
    public static void main(String[] args) throws Exception {
        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        System.out.println("source:"+source.getParallelism());
        source.print();
        env.execute("SinkApp");


    }
}
