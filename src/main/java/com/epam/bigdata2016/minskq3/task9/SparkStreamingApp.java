package com.epam.bigdata2016.minskq3.task9;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;

import com.epam.bigdata2016.minskq3.task9.model.LogEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class SparkStreamingApp {
    private static final Pattern SPACE = Pattern.compile(" ");


    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err
                    .println("Usage: SparkStreamingLogAggregationApp {master} {zkQuorum} {group} {topic} {numThreads} {table} {columnFamily}");
            System.exit(1);
        }

        String master = args[0];
        String zkQuorum = args[1];
        String group = args[2];
        String[] topics = args[3].split(",");
        int numThreads = Integer.parseInt(args[4]);
        String tableName = args[5];
        String columnFamily = args[6];

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingLogAggregationApp");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));


        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        System.out.println("$1");




//        HTableDescriptor crTable = new HTableDescriptor(Bytes.toBytes("loglines"));
 //       HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes("logline"));
 //       crTable.addFamily(family);

//        HBaseAdmin admin = new HBaseAdmin(conf);
//        admin.createTable(crTable);

        JavaDStream<LogEntity> lines = messages.map(new Function<Tuple2<String, String>, LogEntity>() {
            @Override
            public LogEntity call(Tuple2<String, String> tuple2) {

                System.out.println("%1");

                Configuration conf = HBaseConfiguration.create();
                //conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
                conf.set("hbase.zookeeper.property.clientPort", "2181");
                conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
                conf.set("zookeeper.znode.parent", "/hbase");


                Put put = new Put(Bytes.toBytes(new java.util.Date().getTime()));
                put.add(Bytes.toBytes("details"), Bytes.toBytes("logline"), Bytes.toBytes(tuple2._2()));
                try {
                    System.out.println("%2");
                    HTable table = new HTable(conf, "loglines");

                    table.put(put);

                    table.close();
                    System.out.println("%3");

                } catch (Exception e) {
                    System.out.println("### IOException" + e.getMessage());
                }
                System.out.println("###1 " + tuple2.toString());
                return new LogEntity(tuple2._2());
            }
        });
//
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", zkQuorum);
//
//
//        HTable table = new HTable(conf, "logstable");
//
//        Put put = new Put(Bytes.toBytes(new java.util.Date().getTime()));
//        put.add(Bytes.toBytes("details"), Bytes.toBytes("UniqueId"), Bytes.toBytes(dataBean.getUid()));
//
//
//        lines.foreachRDD(new Function<JavaRDD<LogEntity>, Void>() {
//            @Override
//            public Void call(JavaRDD<LogEntity> rdd) throws Exception {
//                if (rdd != null) {
//
//
//                }
//            }
//        });

//
//        lines.f
//        lines.foreachRDD(new Function2<JavaPairRDD<String, Integer>, Void>() {
//                             @Override
//                             public Void call(JavaPairRDD<String, Integer> rdd, Time time) throws IOException {
//                                 // Get or register the blacklist Broadcast
//                                 final Broadcast<List<String>> blacklist = JavaWordBlacklist.getInstance(new JavaSparkContext(rdd.context()));
//                                 // Get or register the droppedWordsCounter Accumulator
//                                 final LongAccumulator droppedWordsCounter = JavaDroppedWordsCounter.getInstance(new JavaSparkContext(rdd.context()));
//                                 // Use blacklist to drop words and use droppedWordsCounter to count them
//                                 String counts = rdd.filter(new Function<Tuple2<String, Integer>, Boolean>() {
//                                     @Override
//                                     public Boolean call(Tuple2<String, Integer> wordCount) throws Exception {
//                                         if (blacklist.value().contains(wordCount._1())) {
//                                             droppedWordsCounter.add(wordCount._2());
//                                             return false;
//                                         } else {
//                                             return true;
//                                         }
//                                     }
//                                 }).collect().toString();
//                                 String output = "Counts at time " + time + " " + counts;
//                             }
//                         }
//
//                lines.foreachRDD(VoidFunction < >);

//        lines.foreachRDD(new Function<JavaRDD<LogEntity>, Void>() {
//            public Void call(JavaRDD<LogEntity> personRDD) throws Exception {
//                //pushRawDataToHBase(hBaseContext, personRDD);
//                return null;
//            }
//        });


//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", zkQuorum);
//
//
//        HTable table = new HTable(conf, "testtable");
//        Put put = new Put(Bytes.toBytes("row1"));
//        put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
//                Bytes.toBytes("val1"));
//        put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
//                Bytes.toBytes("val2"));
//        table.put(put);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<LogEntity, String>() {
            @Override
            public Iterator<String> call(LogEntity x) {
                return Arrays.asList(SPACE.split(x.getLine())).iterator();
            }
        });

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });


        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }
}