package org.feather.flink.basic;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @projectName: feather-flink
 * @package: org.feather.flink.basic
 * @className: StreamingWCApp
 * @author: feather
 * @description: 基于flink实时处理快速入门案例
 * @since: 2023-07-23 16:23
 * @version: 1.0
 */

/**
 * 使用Flink进行流式/实时应用程序的开发
 * 数据源来自于socket，词频统计分析(wordcount)，统计结果输出到控制台
 *
 * 业务逻辑：wc
 *
 * pk,pk,pk,flink,flink
 * 1) 每行数据按照指定的分隔符进行拆分
 * 	分隔符就是,
 * 	String[] words = value.split(",")
 * 2) 每个单词赋值为1：出现的次数
 *    (pk, 1)
 *    (pk, 1)
 *    (pk, 1)
 *    (flink, 1)
 *    (flink, 1)
 * 3) shuffle：相同的key分到一个任务中去进行累加操作
 *     (pk, 3)
 *     (flink, 2)
 */
public class StreamingWCApp {

    public static void main(String[] args) throws Exception {
        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 对接数据源的数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        // 业务逻辑处理： transformation
        source.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] words = value.split(",");
                        for(String word : words) {
                            out.collect(word.toLowerCase().trim());
                        }
                    }
                }).filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return StringUtils.isNotEmpty(value);
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>(value, 1);
                    }
                }).keyBy(new KeySelector<Tuple2<String,Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }).sum(1)
                .print();

        env.execute("StreamgingWCApp");
    }

}
