package com.numb.mapred.wordcount;

import org.apache.hadoop.mapreduce.Reducer;

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
 * @author gaoxiangyu
 */
public class WordCountReduce extends Reducer {
}
