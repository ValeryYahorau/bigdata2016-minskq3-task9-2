package com.epam.bigdata2016.minskq3.task9;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class SparkStreamingApp {
    private static SimpleDateFormat tmsFormatter = new SimpleDateFormat("yyyyMMddhhmmss");
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final String SPLIT = "\\t";
    private static final String NULL = "null";
    private static final String BID_ID = "bid_Id";
    private static final String TIMESTAMP_DATE = "timestamp_data";
    private static final String IPINYOU_ID = "ipinyou_Id";
    private static final String USER_AGENT = "user_agent";
    private static final String IP = "ip";
    private static final String REGION = "region";
    private static final String CITY = "city";
    private static final String AD_EXCHANGE = "ad_exchange";
    private static final String DOMAIN = "domain";
    private static final String URL = "url";
    private static final String AU_ID = "anonymous_url_id";
    private static final String AS_ID = "ad_slot_id";
    private static final String AS_WIDTH = "ad_slot_width";
    private static final String AS_HEIGHT = "ad_slot_height";
    private static final String AS_VISIBILITY = "ad_slot_visibility";
    private static final String AS_FORMAT = "ad_slot_format";
    private static final String P_PRICE = "paying_price";
    private static final String CREATIVE_ID = "creative_id";
    private static final String B_PRICE = "bidding_price";
    private static final String ADV_ID = "advertiser_id";
    private static final String USER_TAGS = "user_tags";
    private static final String STREAM_ID = "stream_id";
    private static final String DEVICE = "device";
    private static final String OS = "os_name";


    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: SparkStreamingLogAggregationApp {zkQuorum} {group} {topic} {numThreads} {table} {columnFamily}");
            System.exit(1);
        }

        String zkQuorum = args[0];
        String group = args[1];
        String[] topics = args[2].split(",");
        int numThreads = Integer.parseInt(args[3]);
        String tableName = args[4];
        String columnFamily = args[5];

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingLogAggregationApp").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
        jssc.checkpoint("hdfs://sandbox.hortonworks.com/tmp/aux2/checkpoint");

        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        JavaDStream<String> lines = messages.map(tuple2 -> {
            Configuration conf = getConfig();
            HTable table = new HTable(conf, tableName);

            // Split each line into fields
            String[] fields = tuple2._2().toString().split(SPLIT);

            UserAgent ua = UserAgent.parseUserAgentString(fields[3]);
            String device =  ua.getBrowser() != null ? ua.getOperatingSystem().getDeviceType().getName() : null;
            String os = ua.getBrowser() != null ? ua.getOperatingSystem().getName() : null;

            if (!NULL.equals(fields[2])) {
                String rowKey = fields[2] + "_" + fields[1];

                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(BID_ID), Bytes.toBytes(fields[0]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(TIMESTAMP_DATE), Bytes.toBytes(formatter.format(tmsFormatter.parse(fields[1]))));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(IPINYOU_ID), Bytes.toBytes(fields[2]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(USER_AGENT), Bytes.toBytes(fields[3]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(IP), Bytes.toBytes(fields[4]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(REGION), Bytes.toBytes(Integer.parseInt(fields[5])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(CITY), Bytes.toBytes(Integer.parseInt(fields[6])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AD_EXCHANGE), Bytes.toBytes(Integer.parseInt(fields[7])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(DOMAIN), Bytes.toBytes(fields[8]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(URL), Bytes.toBytes(fields[9]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AU_ID), Bytes.toBytes(fields[10]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AS_ID), Bytes.toBytes(fields[11]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AS_WIDTH), Bytes.toBytes(Integer.parseInt(fields[12])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AS_HEIGHT), Bytes.toBytes(Integer.parseInt(fields[13])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AS_VISIBILITY), Bytes.toBytes(Integer.parseInt(fields[14])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AS_FORMAT), Bytes.toBytes(Integer.parseInt(fields[15])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(P_PRICE), Bytes.toBytes(Integer.parseInt(fields[16])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(CREATIVE_ID), Bytes.toBytes(fields[17]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(B_PRICE), Bytes.toBytes(Integer.parseInt(fields[18])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(ADV_ID), Bytes.toBytes(Integer.parseInt(fields[19])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(USER_TAGS), Bytes.toBytes(Long.parseLong(fields[20])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(STREAM_ID), Bytes.toBytes(Integer.parseInt(fields[21])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(DEVICE), Bytes.toBytes(device));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(OS), Bytes.toBytes(os));

                try {
                    table.put(put);
                } catch (IOException e) {
                    System.out.println("IOException" + e.getMessage());
                }
                System.out.println("Line was written: " + tuple2.toString());

                table.flushCommits();
                table.close();
            }
            return new String(tuple2._2());
        });

        messages.checkpoint(new Duration(10000));
        lines.print();

        jssc.start();
        jssc.awaitTermination();
    }

    private static Configuration getConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        return conf;
    }
}