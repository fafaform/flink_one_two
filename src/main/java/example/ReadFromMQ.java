package example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class ReadFromMQ {
    //// VARIABLES
    public static String KAFKA_CONSUMER_TOPIC = "mq-sample100";
    public static String KAFKA_PRODUCER_TOPIC = "flink-from-kafka";
    //// TEST IN CLUSTER
    public static String BOOTSTRAP_SERVER = "poc01.kbtg:9092,poc02.kbtg:9092,poc03.kbtg:9092";
    //// TEST IN MY LOCAL
//    public static String BOOTSTRAP_SERVER = "localhost:9092";

    public static Logger LOG = LoggerFactory.getLogger(ReadFromMQ.class);

    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVER);

        //// READ FROM EARLIEST HERE
//        properties.setProperty("auto.offset.reset", "earliest");

        //// END VARIABLES
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer(KAFKA_PRODUCER_TOPIC, new ProducerStringSerializationSchema(KAFKA_PRODUCER_TOPIC), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        FlinkKafkaConsumer<byte[]> kafkaSource = new FlinkKafkaConsumer(KAFKA_CONSUMER_TOPIC, new AbstractDeserializationSchema<byte[]>() {
            @Override
            public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
            }
        }, properties);

        //// CONVERTING PROCESS
        DataStream<Tuple6<Long, Long, String, String, Double, String>> messageStream = env.addSource(kafkaSource).process(new ProcessFunction<byte[], Tuple6<Long, Long, String, String, Double, String>>() {
            @Override
            public void processElement(byte[] s, Context context, Collector<Tuple6<Long, Long, String, String, Double, String>> collector) throws Exception {

                TransactionObject transactionObject = new TransactionObject();
                transactionObject.setTXN_ID(Arrays.copyOfRange(s, 0, 9));
                transactionObject.setTIMESTAMP(Arrays.copyOfRange(s, 35, 45));
                transactionObject.setCARD_TYPE(Arrays.copyOfRange(s, 25, 26));
                transactionObject.setCARD_STATUS(Arrays.copyOfRange(s, 26, 27));
                transactionObject.setTXN_AMT(Arrays.copyOfRange(s, 27, 35));
                transactionObject.setCARD_NUMBER(Arrays.copyOfRange(s, 9, 25));

                collector.collect(new Tuple6<Long, Long, String, String, Double, String>(transactionObject.getTXN_ID(), transactionObject.getTIMESTAMP(), transactionObject.getCARD_TYPE(), transactionObject.getCARD_STATUS(), transactionObject.getTXN_AMT(), transactionObject.getCARD_NUMBER()));
            }
        });

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
        }).rebalance().print();



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
        }).rebalance().print();


        //// SINK TO KAFKA
        DataStreamSink<String> sendingToKafka = messageStream.process(new ProcessFunction<Tuple6<Long, Long, String, String, Double, String>, String>() {
            @Override
            public void processElement(Tuple6<Long, Long, String, String, Double, String> transactionData, Context context, Collector<String> collector) throws Exception {
                collector.collect("{\"TXN_ID\":" + transactionData.f0
                        + ",\"TIMESTAMP\":" + transactionData.f1
                        + ",\"CARD_TYPE\":\"" + transactionData.f2 +"\""
                        + ",\"CARD_STATUS\":\"" + transactionData.f3 +"\""
                        + ",\"TXN_AMT\":" + transactionData.f4
                        + ",\"CARD_NUMBER\":\"" + transactionData.f5 + "\""
                        + "}");
            }
        }).rebalance().addSink(myProducer);

        env.execute("Read from MQ");
    }
}
