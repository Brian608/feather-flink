package org.feather.flink.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
/**
 * @projectName: feather-flink
 * @package: org.feather.flink.basic
 * @className: BalanceSampling
 * @author: feather
 * @description:
 * 解决样本不均衡问题：在某些应用场景中，数据集中的不同类别或群体可能存在明显的不均衡性，即某些类别或群体的样本数量明显少于其他类别或群体。这种情况下，平衡采样可以通过提高少数类别或群体的采样概率，或减少多数类别或群体的采样概率，来解决样本不均衡问题。
 *
 * 提高模型性能：在机器学习和数据挖掘任务中，模型性能通常会受到样本不均衡的影响。由于少数类别或群体的样本数量较少，模型可能倾向于对多数类别或群体进行较好的分类或预测，而忽略少数类别或群体。通过平衡采样，可以提高模型对不同类别或群体的学习能力，从而改善模型的性能和泛化能力。
 *
 * 提升数据分析可信度：在数据分析和决策过程中，样本的代表性和可信度至关重要。通过平衡采样，可以确保每个类别或群体中的样本数量相对平衡，避免样本数量较少的类别或群体对分析结果产生过大的偏差，进而提升数据分析的可信度和准确性。
 *
 * 对于给定一组数据进行平衡采样的情况，最终结果的样貌将取决于平衡采样的策略和逻辑。平衡采样通常会根据数据集中不同类别或群体的分布情况，调整采样概率或者样本数量，以实现类别或群体之间的平衡。最终的结果将保持不同类别或群体之间相对平衡的样本表示。
 *
 * 平衡采样的逻辑是根据不同类别或群体的分布情况，调整采样策略以实现平衡。这样可以保证样本在不同类别或群体之间具有相对均衡的表示，从而解决样本不均衡问题，并提升模型性能和数据分析的可信度。通过平衡采样，我们可以更全面、准确地分析和理解数据，并做出更准确的决策。
 * @since: 2023-07-26 16:18
 * @version: 1.0
 */

public class BalanceSampling {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取CSV文件
        DataStream<Tuple3<String, Integer, Double>> input = env.readTextFile("data/HierarchicalSamplingTestData.CSV")
                .flatMap(new CSVParser());

        // 平衡采样逻辑
        DataStream<Tuple3<String, Integer, Double>> balancedSample = input
                .keyBy(1) // 以年龄为key进行分组
                .flatMap(new BalanceSamplingFunction());

        balancedSample.print();

        env.execute("Balance Sampling");
    }

    // 解析CSV数据
    public static class CSVParser implements FlatMapFunction<String, Tuple3<String, Integer, Double>> {
        @Override
        public void flatMap(String line, Collector<Tuple3<String, Integer, Double>> out) {
            // 以逗号分隔CSV字段
            String[] fields = line.split(",");
            if (fields.length == 3) {
                String name = fields[0];
                try {
                    int age = Integer.parseInt(fields[1]);
                    double salary = Double.parseDouble(fields[2]);
                    out.collect(new Tuple3<>(name, age, salary));
                } catch (NumberFormatException e) {
                    System.err.println("Invalid data format: " + line);
                }
            } else {
                System.err.println("Invalid data format: " + line);
            }
        }
    }

    // 平衡采样函数
    public static class BalanceSamplingFunction extends org.apache.flink.api.common.functions.RichFlatMapFunction<Tuple3<String, Integer, Double>, Tuple3<String, Integer, Double>> {
        private transient ValueState<Boolean> sampledFlag; // 标识是否已采样

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                    "sampledFlag",
                    TypeInformation.of(new TypeHint<Boolean>() {})
            );
            sampledFlag = getRuntimeContext().getState(flagDescriptor);
        }

        @Override
        public void flatMap(Tuple3<String, Integer, Double> value, Collector<Tuple3<String, Integer, Double>> out) throws Exception {
            if (sampledFlag.value() == null || !sampledFlag.value()) { // 当前key未被采样
                sampledFlag.update(true); // 设置采样标志为已采样
                out.collect(value); // 保留数据
            }
        }
    }
}