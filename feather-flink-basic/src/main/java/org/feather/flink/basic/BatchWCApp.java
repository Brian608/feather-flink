package org.feather.flink.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @projectName: feather-flink
 * @package: org.feather.flink.basic
 * @className: BatchWCApp
 * @author: feather
 * @description: TODO
 * @since: 2023-07-23 17:38
 * @version: 1.0
 */

public class BatchWCApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = environment.readTextFile("data/wc.data");

        source.flatMap(new PKFlatMapFunction())
                .map(new PKMapFunction())
                .groupBy(0)
                .sum(1)
                .print();

    }
}

class PKFlatMapFunction implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        String[] words = value.split(",");
        for(String word : words) {
            out.collect(word.toLowerCase().trim());
        }
    }
}

class PKMapFunction implements MapFunction<String, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String value) throws Exception {
        return new Tuple2<>(value, 1);
    }
}
