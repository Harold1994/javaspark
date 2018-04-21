package com.sparkbook.java;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.StatCounter;
import org.spark_project.jetty.client.HttpClient;
import scala.Tuple2;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ChapterSixExample {
    public static class SumInts implements Function2<Integer, Integer, Integer> {
        @Override
        public Integer call(Integer v1, Integer v2) throws Exception {
            return v1 + v2;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new Exception("Usage AccumulatorExample sparkMaster inputFile outDirectory");
        }
        String sparkMaster = args[0];
        String inputFile = args[1];
        String inputFile2 = args[2];
        String outputDir = args[3];

        JavaSparkContext sc = new JavaSparkContext(sparkMaster, "ChapterSixExample", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> rdd = sc.textFile(inputFile);
        final Accumulator<Integer> count = sc.accumulator(0);
        rdd.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                if (s.contains("KK6JKQ")) {
                    count.add(1);
                }
            }
        });

        System.out.println("lines with 'KK6JKQ' : " + count);

        final Accumulator<Integer> blankLines = sc.accumulator(0);
        JavaRDD<String> callSigns = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                if (s.equals("")) {
                    blankLines.add(1);
                }
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        callSigns.saveAsTextFile(outputDir+".callSigns");
        System.out.println("Blank lines: " + blankLines.value());

        final Accumulator<Integer> validSignCount = sc.accumulator(0);
        final Accumulator<Integer> invalidSignCount = sc.accumulator(0);
        JavaRDD<String> validCallSigns = callSigns.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String callSign) throws Exception {
                        Pattern p = Pattern.compile("\\A\\d?\\p{Alpha}{1,2}\\d{1,4}\\p{Alpha}{1,3}\\Z");
                        Matcher m = p.matcher(callSign);
                        boolean b = m.matches();
                        if (b) {
                            validSignCount.add(1);
                        } else {
                            invalidSignCount.add(1);
                        }
                        return b;
                    }
                }
        );


        JavaPairRDD<String, Integer> contactCounts = validCallSigns.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String callSigns) throws Exception {
                        return new Tuple2<String, Integer> (callSigns, 1 );
                    }
                }
        ).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) throws Exception {
                return x+y;
            }
        });

        contactCounts.count();

        if (invalidSignCount.value() < 0.1 * validSignCount.value()) {
            contactCounts.saveAsTextFile(outputDir + "contactCount");
        } else {
            System.out.println("Too many errors " + invalidSignCount.value() + "for " + validSignCount.value());
            System.exit(1);
        }

//        final Broadcast<String []> signPrefixes = sc.broadcast(loadCallSignTable());
//        JavaPairRDD<String, Integer> countryContactCounts = contactCounts.mapToPair(
//                new PairFunction<Tuple2<String, Integer>, String, Integer>() {
//                    @Override
//                    public Tuple2<String, Integer> call(Tuple2<String, Integer> callSignCount) throws Exception {
//                        String sign = callSignCount._1();
//                        String country = lookupCountry(sign, signPrefixes.value());
//                        return new Tuple2<String, Integer>(country, callSignCount._2());
//                    }
//                }
//        ).reduceByKey(new SumInts());
//        countryContactCounts.saveAsTextFile(outputDir + "countries.txt");
//        System.out.println("Saved country contact counts as a file");
//
////        JavaPairRDD<String, CallLog[]> contactsContactLists = validCallSigns.mapPartitionsToPair(
////                new PairFlatMapFunction<Iterator<String>, String, CallLog[]>() {
////                    @Override
////                    public Iterator<Tuple2<String, CallLog[]>> call(Iterator<String> input) throws Exception {
//                        ArrayList<Tuple2<String, CallLog[]>> callsignQsos = new ArrayList<Tuple2<String, CallLog[]>>();
//                        ArrayList<Tuple2<String, ContentExchange>> requests = new ArrayList<Tuple2<String, ContentExchange>>();
//                        ObjectMapper mapper = createMapper();
//                        HttpClient client = new HttpClient();
//                        try {
//                            client.start();
//                            while (input.hasNext()) {
//                                requests.add(createRequestForSign(input.next(), client));
//                            }
//
//                            for (Tuple2<String, ContentExchange> signExchange : requests) {
//                                callsignQsos.add(fetchResultFromRequest(mapper, signExchange));
//                            }
//                        } catch (Exception e) {
//                    }
//                    return callsignQsos.iterator();
//                }}
//        );
//        System.out.println(StringUtils.join(contactsContactLists.collect(), ","));
//
//        String distScript = System.getProperty("user.dir") + "/src/R/finddistance.R";
//        String distScriptName = "finddistance.R";
//        sc.addFile(distScript);
//        JavaRDD<String> pipeInputs = contactsContactLists.values().map(new VerifyCallLogs()).flatMap(
//                new FlatMapFunction<CallLog[], String>() { public Iterator<String> call(CallLog[] calls) {
//                    ArrayList<String> latLons = new ArrayList<String>();
//                    for (CallLog call: calls) {
//                        latLons.add(call.mylat + "," + call.mylong +
//                                "," + call.contactlat + "," + call.contactlong);
//                    }
//                    return latLons.iterator();
//                }
//                });
//        JavaRDD<String> distances = pipeInputs.pipe(SparkFiles.get(distScriptName));
//
//        JavaDoubleRDD distanceDoubles = distances.mapToDouble(new DoubleFunction<String>() {
//            public double call(String value) {
//                return Double.parseDouble(value);
//            }});
//        final StatCounter stats = distanceDoubles.stats();
//        final Double stddev = stats.stdev();
//        final Double mean = stats.mean();
//        JavaDoubleRDD reasonableDistances =
//                distanceDoubles.filter(new Function<Double, Boolean>() {
//                    public Boolean call(Double x) {
//                        return (Math.abs(x-mean) < 3 * stddev);}});
//        System.out.println(StringUtils.join(reasonableDistances.collect(), ","));
//        sc.stop();
//        System.exit(0);
//    }

//    private static Tuple2<String,ContentExchange> createRequestForSign(String sign, HttpClient client) {
//        ContentExchange exchange = new ContentExchange(true);
//        exchange.setURL("http://new73s.herokuapp.com/qsos/" + sign + ".json");
//        client.send(exchange);
//        return new Tuple2(sign, exchange);
//    }
//
//    static Tuple2<String, CallLog[]> fetchResultFromRequest(ObjectMapper mapper,
//                                                            Tuple2<String, ContentExchange> signExchange) {
//        String sign = signExchange._1();
//        ContentExchange exchange = signExchange._2();
//        return new Tuple2(sign, readExchangeCallLog(mapper, exchange));
//    }

//    private static String lookupCountry(String sign, String[] table) {
//        Integer pos = Arrays.binarySearch(table, sign);
//        if (pos < 0 ) {
//            pos = -pos-1;
//        }
//        return table[pos].split(",")[1];
//    }
//
//    static String[] loadCallSignTable() throws FileNotFoundException {
//        Scanner callSignTbl = new Scanner(new File("files/callsign_tbl_sorted"));
//        ArrayList<String> callSignList = new ArrayList<String>();
//        while (callSignTbl.hasNextLine()) {
//            callSignList.add(callSignTbl.nextLine());
//        }
//        return callSignList.toArray(new String[0]);
//    }
//
//    static ObjectMapper createMapper() {
//        ObjectMapper mapper = new ObjectMapper();
//        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//        return mapper;
//    }
//
//    public static class VerifyCallLogs implements Function<CallLog[], CallLog[]> {
//        public CallLog[] call(CallLog[] input) {
//            ArrayList<CallLog> res = new ArrayList<CallLog>();
//            if (input != null) {
//                for (CallLog call: input) {
//                    if (call != null && call.mylat != null && call.mylong != null
//                            && call.contactlat != null && call.contactlong != null) {
//                        res.add(call);
//                    }
//                }
//            }
//            return res.toArray(new CallLog[0]);
//        }
    }
}
