package com.zeno.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Iterator;


public class TemperaturePerMonthReducer extends Reducer<MapperOutputKey, IntWritable, Text, IntWritable> {

    /** 因为map可能被吊起多次，定义为成员减少gc，同时，你要知道，源码中看到了，
     *  map输出的key，value，是会发生序列化，变成字节数组进入buffer的。
     */
    private MapperOutputKey keyOutput = new MapperOutputKey();
    private IntWritable temperature = new IntWritable();
    private Text outputKeyText = new Text();

    @Override
    protected void reduce(MapperOutputKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        String outputKey = key.getYear() + "-" + key.getMonth() + "-";
        outputKeyText.set(outputKey);

        for (IntWritable value : values){
            outputKeyText.set(outputKey + key.getDay());
            context.write(outputKeyText,value);
        }


//        Iterator<IntWritable> iter = values.iterator();
//
//        int flg = 0 ;
//        int day = 0;
//        while(iter.hasNext()){
//            IntWritable val = iter.next();  // -> context.nextKeyValue() ->  对key和value更新值！！！！
//
//            if(flg == 0){
//                context.write(outputKeyText,val);
//                flg++;
//                day = key.getDay();
//
//            }
//
//            if(flg != 0 &&  day != key.getDay()){
//                context.write(outputKeyText,val);
//                break;
//            }
//        }
    }
}