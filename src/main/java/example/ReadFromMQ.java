package example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class ReadFromMQ {

    //// VARIABLES
    public static String KAFKA_CONSUMER_TOPIC = "input";
//    public static String KAFKA_CONSUMER_TOPIC = "flink-from-kafka";
    public static String KAFKA_MASTER_PRODUCER_TOPIC = "CREDIT_CARD_MASTER_500";
    public static String KAFKA_VISA_PRODUCER_TOPIC = "CREDIT_CARD_VISA_500";
    //// TEST IN CLUSTER
    public static String BOOTSTRAP_SERVER = "172.30.74.84:9092,172.30.74.85:9092,172.30.74.86:9092";
//    public static String BOOTSTRAP_SERVER = "poc01.kbtg:9092,poc02.kbtg:9092,poc03.kbtg:9092";
    //// TEST IN MY LOCAL
//    public static String BOOTSTRAP_SERVER = "localhost:9092";

    public static String TXN_ID = "TXN_ID";
    public static String TIMESTAMP = "TIMESTAMP";
    public static String CARD_TYPE = "CARD_TYPE";
    public static String CARD_STATUS = "CARD_STATUS";
    public static String TXN_AMT = "TXN_AMT";
    public static String CARD_NUMBER = "CARD_NUMBER";

    public static Logger LOG = LoggerFactory.getLogger(ReadFromMQ.class);

    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVER);

        //// READ FROM EARLIEST HERE
//        properties.setProperty("auto.offset.reset", "earliest");

        //// END VARIABLES
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaProducer<String> masterProducer = new FlinkKafkaProducer(KAFKA_MASTER_PRODUCER_TOPIC, new ProducerStringSerializationSchema(KAFKA_MASTER_PRODUCER_TOPIC), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        FlinkKafkaProducer<String> visaProducer = new FlinkKafkaProducer(KAFKA_VISA_PRODUCER_TOPIC, new ProducerStringSerializationSchema(KAFKA_VISA_PRODUCER_TOPIC), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        ////////////////////////// RECEIVE JSON
        //// RECEIVE JSON
        FlinkKafkaConsumer<ObjectNode> JsonSource = new FlinkKafkaConsumer(KAFKA_CONSUMER_TOPIC, new JSONKeyValueDeserializationSchema(false), properties);
        DataStream<Tuple6<Long, Long, String, String, Double, String>> messageStream = env.addSource(JsonSource).flatMap(new FlatMapFunction<ObjectNode, Tuple6<Long, Long, String, String, Double, String>>() {
            @Override
            public void flatMap(ObjectNode s, Collector<Tuple6<Long, Long, String, String, Double, String>> collector) throws Exception {
                collector.collect(new Tuple6<Long, Long, String, String, Double, String>(s.get("value").get(TXN_ID).asLong(),
                        s.get("value").get(TIMESTAMP).asLong(),
                        s.get("value").get(CARD_TYPE).asText(),
                        s.get("value").get(CARD_STATUS).asText(),
                        s.get("value").get(TXN_AMT).asDouble(),
                    s.get("value").get(CARD_NUMBER).asText()));
            }
        });
        //////////////////////////
        ////////////////////////// RECIEVE BINARY
//        FlinkKafkaConsumer<byte[]> kafkaSource = new FlinkKafkaConsumer(KAFKA_CONSUMER_TOPIC, new AbstractDeserializationSchema<byte[]>() {
//            @Override
//            public byte[] deserialize(byte[] bytes) throws IOException {
//                return bytes;
//            }
//        }, properties);
//
//        //// CONVERTING PROCESS
//        DataStream<Tuple6<Long, Long, String, String, Double, String>> messageStream = env.addSource(kafkaSource).process(new ProcessFunction<byte[], Tuple6<Long, Long, String, String, Double, String>>() {
//            @Override
//            public void processElement(byte[] s, Context context, Collector<Tuple6<Long, Long, String, String, Double, String>> collector) throws Exception {
//
//                TransactionObject transactionObject = new TransactionObject();
//                transactionObject.setTXN_ID(Arrays.copyOfRange(s, 0, 9));
//                transactionObject.setTIMESTAMP(Arrays.copyOfRange(s, 35, 45));
//                transactionObject.setCARD_TYPE(Arrays.copyOfRange(s, 25, 26));
//                transactionObject.setCARD_STATUS(Arrays.copyOfRange(s, 26, 27));
//                transactionObject.setTXN_AMT(Arrays.copyOfRange(s, 27, 35));
//                transactionObject.setCARD_NUMBER(Arrays.copyOfRange(s, 9, 25));
//
//                collector.collect(new Tuple6<Long, Long, String, String, Double, String>(transactionObject.getTXN_ID(), transactionObject.getTIMESTAMP(), transactionObject.getCARD_TYPE(), transactionObject.getCARD_STATUS(), transactionObject.getTXN_AMT(), transactionObject.getCARD_NUMBER()));
//            }
//        });
        //////////////////////////

        //// CREDIT_CARD_MASTER
        DataStreamSink<String> CREDIT_CARD_MASTER = messageStream.filter(new FilterFunction<Tuple6<Long, Long, String, String, Double, String>>() {
            @Override
            public boolean filter(Tuple6<Long, Long, String, String, Double, String> transactionData) throws Exception {
                return (transactionData.f2.equals("M") && transactionData.f3.equals("A") && (transactionData.f4 > 500.00));
            }
        }).process(new ProcessFunction<Tuple6<Long, Long, String, String, Double, String>, String>() {
            @Override
            public void processElement(Tuple6<Long, Long, String, String, Double, String> transactionData, Context context, Collector<String> collector) throws Exception {
                String result = "{\"TXN_ID\":" + transactionData.f0
                        + ",\"CARD_NUMBER\":\"" + transactionData.f5 + "\""
                        + ",\"TXN_AMT\":" + transactionData.f4
                        + ",\"TIMESTAMP\":" + transactionData.f1
                        + "}";
                LOG.info("CREDIT_CARD_MASTER: " + result);
                collector.collect(result);
            }
        }).rebalance().addSink(masterProducer);



        //// CREDIT_CARD_VISA
        DataStreamSink<String> CREDIT_CARD_VISA = messageStream.filter(new FilterFunction<Tuple6<Long, Long, String, String, Double, String>>() {
            @Override
            public boolean filter(Tuple6<Long, Long, String, String, Double, String> transactionData) throws Exception {
                return (transactionData.f2.equals("V") && transactionData.f3.equals("A") && (transactionData.f4 > 500.00));
            }
        }).process(new ProcessFunction<Tuple6<Long, Long, String, String, Double, String>, String>() {
            @Override
            public void processElement(Tuple6<Long, Long, String, String, Double, String> transactionData, Context context, Collector<String> collector) throws Exception {
                String result = "{\"TXN_ID\":" + transactionData.f0
                        + ",\"CARD_NUMBER\":\"" + transactionData.f5 + "\""
                        + ",\"TXN_AMT\":" + transactionData.f4
                        + ",\"TIMESTAMP\":" + transactionData.f1
                        + "}";
                LOG.info("CREDIT_CARD_MASTER: " + result);
                collector.collect(result);
            }
        }).rebalance().addSink(visaProducer);


        //// SINK TO KAFKA
//        DataStreamSink<String> sendingToKafka = messageStream.process(new ProcessFunction<Tuple6<Long, Long, String, String, Double, String>, String>() {
//            @Override
//            public void processElement(Tuple6<Long, Long, String, String, Double, String> transactionData, Context context, Collector<String> collector) throws Exception {
//                collector.collect("{\"TXN_ID\":" + transactionData.f0
//                        + ",\"TIMESTAMP\":" + transactionData.f1
//                        + ",\"CARD_TYPE\":\"" + transactionData.f2 +"\""
//                        + ",\"CARD_STATUS\":\"" + transactionData.f3 +"\""
//                        + ",\"TXN_AMT\":" + transactionData.f4
//                        + ",\"CARD_NUMBER\":\"" + transactionData.f5 + "\""
//                        + "}");
//            }
//        }).rebalance().addSink(myProducer);

        env.execute("Read from MQ");
    }
}
