package com.sparkbook.java;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.StringReader;
import java.util.Iterator;

public class BasicLoadWholeCsv {
    public static class parseLine implements FlatMapFunction<Tuple2<String, String>, String[]> {
        @Override
        public Iterator<String[]> call(Tuple2<String, String> file) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(file._2()));
            return reader.readAll().iterator();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new Exception("Usage BasicLoadCsv sparkMaster csvInputFile csvOutputFile key");
        }
        String master = args[0];
        String csvInput = args[1];
        String outputFile = args[2];
        final String key = args[3];
        JavaSparkContext sc = new JavaSparkContext(master, "loadwholecsv", System.getenv("SPARK_HOME"),System.getenv("JARS"));
        JavaPairRDD<String, String> csvData = sc.wholeTextFiles(csvInput);
        JavaRDD<String[]> keyedRDD = csvData.flatMap(new parseLine());
        JavaRDD<String[]> result =
                keyedRDD.filter(new Function<String[], Boolean>() {
                    @Override
                    public Boolean call(String[] strings) throws Exception {
                        return strings[0].equals(key);
                    }
                });
        result.saveAsTextFile(outputFile);
    }

}
