package com.numb.mapred.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * WordCount Mappper类
 * 继承org.apache.hadoop.mapreduce.Mapper（hadoop 2.x 3.x）
 * 注意：org.apache.hadoop.mapred.Mapper 是hadoop1.x内的
 *
 * 泛型参数说明：
 * KEYIN map阶段输入key的类型：偏移量 LongWritable
 * VALUEIN map阶段输入value的类型：Text
 * KEYOUT map阶段输出key的类型：Text
 * VALUEOUT map阶段输入value的类型：IntWritable
 *
 * map输出即为reduce的输入
 *
 * @author gaoxiangyu
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outK = new Text();

    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);

        // 1 获取一行 转成String
        String line = value.toString();

        // 2 切割处理
        String[] words = line.split(" ");

        for (String word : words) {
            // 封装outK
            outK.set(word);

            // 写出
            context.write(outK, outV);
        }

    }
}
