package com.numb.mapred.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * WordCount Reducer
 * 继承org.apache.hadoop.mapreduce.Reducer（hadoop 2.x 3.x）
 * 注意：org.apache.hadoop.mapred.Reducer 是hadoop1.x内的
 *
 * 泛型参数说明：
 * KEYIN reduce阶段输入key的类型：Text
 * VALUEIN reduce阶段输入value的类型：IntWritable
 * KEYOUT reduce阶段输出key的类型：Text
 * VALUEOUT reduce阶段输入value的类型：IntWritable
 *
 * map输出即为reduce的输入
 *
 * @author gaoxiangyu
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);

        int sum = 0;

        // 累加
        for (IntWritable value : values) {
            sum += value.get();
        }
        outV.set(sum);
        // 写出
        context.write(key, outV);
    }
}
