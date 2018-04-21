package com.sparkbook.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

public class BasicLoadJson {
    public static class Person implements Serializable {
        public String name;
        public Boolean lovesPandas;
    }

    public static class LikePandas implements Function<Person, Boolean> {

        @Override
        public Boolean call(Person person) throws Exception {
            return person.lovesPandas;
        }
    }

    public static class ParseJson implements FlatMapFunction<Iterator<String>, Person> {

        @Override
        public Iterator<Person> call(Iterator<String> lines) throws Exception {
            ArrayList<Person> people = new ArrayList<Person>();
            ObjectMapper mapper = new ObjectMapper();
            while (lines.hasNext()) {
                String line = lines.next();
                try {
                    people.add(mapper.readValue(line, Person.class));
                } catch (Exception e) {

                }
            }
            return people.iterator();
        }
    }

    public static class WriteJson implements FlatMapFunction<Iterator<Person>, String> {

        @Override
        public Iterator<String> call(Iterator<Person> people) throws Exception {
            ArrayList<String> text = new ArrayList<String>();
            ObjectMapper mapper = new ObjectMapper();
            while (people.hasNext()) {
                Person person = people.next();
                text.add(mapper.writeValueAsString(person));
            }
            return text.iterator();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new Exception("Usage BasicLoadJson [sparkMaster] [jsoninput] [jsonoutput]");
        }
        String master = args[0];
        String fileName = args[1];
        String outfile = args[2];

        JavaSparkContext sc = new JavaSparkContext(master, "basicloadjson", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> input = sc.textFile(fileName);
        JavaRDD<Person> result = input.mapPartitions(new ParseJson()).filter(new LikePandas());
        JavaRDD<String> formatted = result.mapPartitions(new WriteJson());
        formatted.saveAsTextFile(outfile);
    }
}
