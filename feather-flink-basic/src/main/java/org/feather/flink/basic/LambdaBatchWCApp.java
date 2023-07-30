package org.feather.flink.basic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @projectName: feather-flink
 * @package: org.feather.flink.basic
 * @className: LambdaBatchWCApp
 * @author: feather
 * @description: Java lambda 表达式 实现离线数据计算统计
 * @since: 2023-07-23 17:57
 * @version: 1.0
 */

public class LambdaBatchWCApp {
    public static void main(String[] args) {
        String projectDirectory = System.getProperty("user.dir");
        String dataDirectory = projectDirectory + File.separator + "data";
        String fileName = "wc.data";
        String filePath = dataDirectory + File.separator + fileName;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            Map<String, Long> wordCountMap = br.lines()
                    .flatMap(line -> Arrays.stream(line.trim().split(",")))
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

            // 打印每个单词及出现的次数
            wordCountMap.forEach((word, count) -> System.out.println(word + " : " + count));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
