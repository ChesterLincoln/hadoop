package com.numb.mapred.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author gaoxiangyu
 */
public class ProvincePartition extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {

        String prePhone = text.toString().substring(0, 3);

        switch (prePhone) {
            case "135":
                return 0;
            case "136":
                return 1;
            case "137":
                return 2;
            case "138":
                return 3;
            case "139":
                return 4;
            default:
                return 5;
        }
    }
}
