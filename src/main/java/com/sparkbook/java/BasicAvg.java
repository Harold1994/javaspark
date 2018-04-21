package com.sparkbook.java;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;


import java.io.Serializable;
import java.util.Arrays;

public final class BasicAvg {
    public static class AvgCount implements Serializable {
        public int total;
        public int num;

        public AvgCount(int total, int sum) {
            this.total = total;
            this.num = sum;
        }
        public float avg() {
            return total/num;
        }
    }

    public static void main(String[] args) {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }

        JavaSparkContext sc = new JavaSparkContext(master, "basicavg", System.getenv("SPARK_HOME"),System.getenv("JARS"));
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4));
        JavaDoubleRDD result = rdd.mapToDouble(
                new DoubleFunction<Integer>() {
                    @Override
                    public double call(Integer x) throws Exception {
                        return (double) x*x;
                    }
                }
        );
//        result.persist(StorageLevel.DISK_ONLY());
//        System.out.println(result.mean());
//        Function2<AvgCount, Integer, AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {
//
//            public AvgCount call(AvgCount a, Integer b) throws Exception {
//                a.total += b;
//                a.num += 1;
//                return a;
//            }
//        };
//
//        Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
//            public AvgCount call(AvgCount a, AvgCount b) throws Exception {
//                a.total += b.total;
//                a.num += b.num;
//                return a;
//            }
//        };
//        AvgCount initial = new AvgCount(0,0);
//        AvgCount result = rdd.aggregate(initial,addAndCount,combine);
//        System.out.println(result.avg());
//        System.out.println(rdd.countByValue());
//        sc.stop();
    }
}
