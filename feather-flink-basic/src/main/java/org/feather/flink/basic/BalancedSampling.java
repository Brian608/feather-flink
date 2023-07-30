package org.feather.flink.basic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @projectName: feather-flink
 * @package: org.feather.flink.basic
 * @className: BalancedSampling
 * @author: feather
 * @description:平衡抽样
 * @since: 2023-07-25 21:55
 * @version: 1.0
 */

public class BalancedSampling {
    public static List<Integer> balancedSample(List<List<Integer>> populations, int k) {
        List<Integer> sample = new ArrayList<>();
        Random random = new Random();

        int numPopulations = populations.size();
        int populationSize = populations.get(0).size();

        for (int i = 0; i < k; i++) {
            int populationIndex = random.nextInt(numPopulations);
            List<Integer> population = populations.get(populationIndex);

            int index = random.nextInt(populationSize);
            sample.add(population.get(index));
            population.remove(index);
            populationSize--;

            if (populationSize == 0) {
                populations.remove(populationIndex);
                numPopulations--;
                if (numPopulations == 0) {
                    break;
                }
                populationSize = populations.get(0).size();
            }
        }

        return sample;
    }
    public static void main(String[] args) {
        List<List<Integer>> populations = new ArrayList<>();
        // 添加各组数据
        populations.add(new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5)));
        populations.add(new ArrayList<>(Arrays.asList(6, 7, 8, 9, 10)));
        populations.add(new ArrayList<>(Arrays.asList(11, 12, 13, 14, 15)));

        int sampleSize = 7;
        List<Integer> sample = balancedSample(populations, sampleSize);

        System.out.println("平衡抽样结果：");
        for (int num : sample) {
            System.out.println(num);
        }
    }
}
