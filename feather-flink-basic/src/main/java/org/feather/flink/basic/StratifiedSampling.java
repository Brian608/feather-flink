package org.feather.flink.basic;

import java.util.*;

/**
 * @projectName: feather-flink
 * @package: org.feather.flink.basic
 * @className: StratifiedSampling
 * @author: feather
 * @description: 分层抽样
 * @since: 2023-07-25 21:53
 * @version: 1.0
 */

public class StratifiedSampling {
    public static List<Integer> stratifiedSample(Map<String, List<Integer>> population, int k) {
        List<Integer> sample = new ArrayList<>();
        Random random = new Random();

        for (Map.Entry<String, List<Integer>> entry : population.entrySet()) {
            String stratum = entry.getKey();
            List<Integer> stratumPopulation = entry.getValue();

            int stratumSize = stratumPopulation.size();
            int stratumSampleSize = (int) Math.round((double) stratumSize / population.size() * k);

            for (int i = 0; i < stratumSampleSize; i++) {
                int index = random.nextInt(stratumSize);
                sample.add(stratumPopulation.get(index));
                stratumPopulation.remove(index);
                stratumSize--;
            }
        }

        return sample;
    }

    public static void main(String[] args) {
        Map<String, List<Integer>> population = new HashMap<>();
        // 添加分层数据
        population.put("层1", new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5)));
        population.put("层2", new ArrayList<>(Arrays.asList(6, 7, 8, 9, 10)));
        population.put("层3", new ArrayList<>(Arrays.asList(11, 12, 13, 14, 15)));

        int sampleSize = 7;
        List<Integer> sample = stratifiedSample(population, sampleSize);

        System.out.println("分层抽样结果：");
        for (int num : sample) {
            System.out.println(num);
        }
    }
}
