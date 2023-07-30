package org.feather.flink.basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;

/**
 * @projectName: feather-flink
 * @package: org.feather.flink.basic
 * @className: RandomSamplingExample
 * @author: feather
 * @description:随机采样
 * 数据压缩：当处理的数据集非常大时，通过随机采样可以将数据集的规模缩小至一个较小的代表性样本，从而更高效地进行后续的分析和处理。由于随机采样通常能够保留原始数据集的分布特征，因此在样本上进行分析的结果往往能够代表整个数据集的特征。
 *
 * 快速预览：通过随机采样，可以快速获取数据集的一个子集来预览数据的分布、特性和相关性。这样有助于在后续的数据处理和分析工作中更好地了解数据，并进行进一步的决策。
 *
 * 数据探索：随机采样可以用来发现数据集中的一些特殊情况、异常点或者有趣的模式。通过采样一部分数据进行探索，可以更好地理解数据集的特性，并为后续的数据处理和挖掘工作提供指导。
 *
 * 针对你提供一组数据进行随机采样的情况，最终结果的样貌将取决于采样的概率和被采样数据的特点。如果采样概率较高，那么最终结果会比较接近原始数据集；如果采样概率较低，那么最终结果会相对稀疏一些。
 *
 * 随机采样的逻辑是基于概率的判断，对于每个元素，根据设定的抽样概率决定它是否被保留。这是为了保证采样结果的随机性和代表性。通过随机采样可以有效地减小数据集的规模，同时保持数据集的特征和分布，提供一个可以快速分析和预览的样本。
 * @since: 2023-07-26 10:14
 * @version: 1.0
 */

public class RandomSamplingExample {
    public static void main(String[] args) throws Exception {

        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 生成数据流
        DataStream<Integer> inputStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 定义抽样的概率
        double samplingProbability = 0.5;

        // 应用随机抽样函数到输入流上
        DataStream<Integer> sampledStream = inputStream.map(new RandomSampler(samplingProbability));

        // 打印抽样结果到控制台
        sampledStream.print();

        // 执行Flink作业
        env.execute("随机抽样示例");
    }
    public static class RandomSampler implements MapFunction<Integer, Integer> {

        private double samplingProbability;
        private Random random;

        public RandomSampler(double samplingProbability) {
            this.samplingProbability = samplingProbability;
            this.random = new Random();
        }

        @Override
        public Integer map(Integer value) throws Exception {
            if (random.nextDouble() <= samplingProbability) {
                return value;
            } else {
                return null;
            }
        }
    }
}
