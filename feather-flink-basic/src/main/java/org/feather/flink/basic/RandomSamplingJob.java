package org.feather.flink.basic;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @projectName: feather-flink
 * @package: org.feather.flink.basic
 * @className: RandomSamplingJob
 * @author: feather
 * @description:随机采样
 * @since: 2023-07-26 22:08
 * @version: 1.0
 */

public class RandomSamplingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1
        env.setParallelism(1);

        DataStream<Tuple2<Integer, Integer>> inputStream = env.fromElements(
                Tuple2.of(1, 10),
                Tuple2.of(1, 10),
                Tuple2.of(1, 10),
                Tuple2.of(4, 10),
                Tuple2.of(5, 10),
                Tuple2.of(6, 10),
                Tuple2.of(7, 10),
                Tuple2.of(10, 10),
                Tuple2.of(13, 10)
        );

        DataStream<Tuple2<Integer, Integer>> sampledStream = inputStream
                //元组的第一个字段进行分组
                .keyBy(0)
                .flatMap(new RandomSampler());

        sampledStream.print();

        env.execute("Random Sampling Job");
    }

    public static class RandomSampler extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        private ValueState<Boolean> sampled;

        @Override
        public void open(Configuration parameters) throws Exception {
            sampled = getRuntimeContext().getState(new ValueStateDescriptor<>("sampled", Boolean.class, false));
        }

        @Override
        public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            if (!sampled.value()) {
                sampled.update(true);
                out.collect(value);
            }
        }
    }

}
