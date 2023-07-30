package org.feather.flink.basic;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @projectName: feather-flink
 * @package: org.feather.flink.basic
 * @className: RandomSampling
 * @author: feather
 * @description:随机抽样
 * @since: 2023-07-25 21:51
 * @version: 1.0
 */

public class RandomSampling {
    public static List<Integer> randomSample(List<Integer> population, int k) {
        List<Integer> sample = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < k; i++) {
            int index = random.nextInt(population.size());
            sample.add(population.get(index));
            population.remove(index);
        }

        return sample;
    }

    public static void main(String[] args) {
        List<Integer> population = new ArrayList<>();
        // 添加总体数据
        for (int i = 1; i <= 100; i++) {
            population.add(i);
        }

        int sampleSize = 10;
        List<Integer> sample = randomSample(population, sampleSize);

        System.out.println("随机抽样结果：");
        for (int num : sample) {
            System.out.println(num);
        }
    }
}
