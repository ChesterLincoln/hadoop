package com.numb.mapred.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author gaoxiangyu
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean outK = new FlowBean();
    private Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] splits = line.split("\t");

        String phone = splits[1];
        outV.set(phone);

        String upFlow = splits[splits.length - 3];
        String downFlow = splits[splits.length - 2];

        outK.setUpFlow(Long.parseLong(upFlow));
        outK.setDownFlow(Long.parseLong(downFlow));
        outK.setSumFlow();

        context.write(outK, outV);
    }
}
