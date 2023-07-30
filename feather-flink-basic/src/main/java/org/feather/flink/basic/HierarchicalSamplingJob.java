package org.feather.flink.basic;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * @projectName: feather-flink
 * @package: org.feather.flink.basic
 * @className: HierarchicalSamplingJob
 * @author: feather
 * @description: 分层采样
保证代表性：通过分层采样，可以确保每个层级中的样本都有足够的代表性，从而更准确地反映原始数据集的分布和特征。不同层级的样本可以更全面地覆盖数据集的各个方面，从而避免因过于集中的采样导致数据偏差。

提高效率：分层采样可以根据数据的特征和需求，有选择地对感兴趣的层级进行采样，而忽略对其他层级的采样。这样可以减少处理的数据量，提高处理和分析的效率。

精确估计：分层采样可以提供更精确的估计结果，特别是在有限资源和时间限制下。通过合理地选择采样的层级和样本数量，可以在保证结果精度的前提下，降低采样和计算的成本。

对于给定一组数据进行分层采样的情况，最终结果的样貌将取决于数据的层级划分和每个层级上的采样逻辑。分层采样通常会选择具有代表性的层级，并在每个层级上进行独立的采样操作。最终的结果将保留每个层级中的一部分样本，以保证整体数据集的分布特征。

分层采样的逻辑是在保证代表性的同时，针对每个层级进行独立的采样操作。这样可以更好地反映不同层级的数据特征，并在保证效率的前提下提供精确的估计。通过分层采样，可以灵活地选择和控制采样的层级和样本数量，以满足特定的分析需求。
 * @since: 2023-07-26 13:53
 * @version: 1.0
 */

public class HierarchicalSamplingJob {
    public static void main(String[] args) throws Exception {
        try {
            // 设置 Flink 执行环境
            ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

            // 读取本地 CSV 文件，跳过第一行标题行
            DataSet<Tuple3<String, Integer, Double>> inputDataSet = env.readCsvFile("data/HierarchicalSamplingTestData.CSV")
                    .includeFields(true, true, true)
                    .ignoreFirstLine()
                    .types(String.class, Integer.class, Double.class);

            // 实现分层采样
            DataSet<Tuple3<String, Integer, Double>> sampledDataSet = inputDataSet
                    .groupBy(new AgeRangeGrouping()) // 根据年龄段进行分组
                    .reduceGroup(new SamplingReducer());

            // 打印采样结果到控制台
            sampledDataSet.print();

            // 执行任务
            env.execute("Hierarchical Sampling Example");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class AgeRangeGrouping implements KeySelector<Tuple3<String, Integer, Double>, String> {
        @Override
        public String getKey(Tuple3<String, Integer, Double> value) throws Exception {
            int age = value.f1;
            if (age <= 20) {
                return "20岁以下";
            } else if (age <= 30) {
                return "21-30岁";
            } else if (age <= 40) {
                return "31-40岁";
            } else {
                return "40岁以上";
            }
        }
    }

    public static class SamplingReducer implements GroupReduceFunction<Tuple3<String, Integer, Double>, Tuple3<String, Integer, Double>> {
        @Override
        public void reduce(Iterable<Tuple3<String, Integer, Double>> values, Collector<Tuple3<String, Integer, Double>> out) throws Exception {
            double threshold = 0.5; // 采样阈值为0.5
            for (Tuple3<String, Integer, Double> value : values) {
                if (Math.random() < threshold) { // 进行分层采样，根据 Math.random() 的值进行过滤
                    out.collect(value);
                }
            }
        }
    }
}