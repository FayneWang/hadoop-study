package com.zeno.hadoop.mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Mapper 输出自定义Key
 * @author zeno
 */
public class MapperOutputKey implements WritableComparable<MapperOutputKey> {


    private int year;
    private int month;
    private int day;
    private int temperature;

    public MapperOutputKey(){
    }

    public MapperOutputKey(int year, int month, int day, int temperature) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.temperature = temperature;
    }


    public void setValues(int year, int month, int day, int temperature) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.temperature = temperature;
    }

    @Override
    public int compareTo(MapperOutputKey o) {
        //我们为了让这个案例体现api开发，所以下边的逻辑是一种通用的逻辑：按照时间正序，
        //但是我们目前业务需要的是  年，月，温度，且温度倒序，所以一会还得开发一个sortComparator。。。。
        //the value 0 if x == y; a value less than 0 if x < y; and a value greater than 0 if x > y

        int yearComp = Integer.compare(this.year,o.year);
        if (yearComp != 0){
            return yearComp;
        }

        int monthComp = Integer.compare(this.month,o.month);
        if (monthComp != 0){
            return monthComp;
        }

        return Integer.compare(this.day,o.day);

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(temperature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.temperature = in.readInt();
    }


    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }
}

