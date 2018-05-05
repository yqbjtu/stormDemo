

package com.yq;

import com.yq.bolts.CounterBolt;
import com.yq.bolts.MongoBolt;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.Arrays;
import java.util.UUID;

/**
 * Simple to Introduction
 * className: KafkaStormDemo
 *
 * @author YangQian
 * @version 2018/4/26 11:30
 */
public class KafkaStormDemo {
    public static void main(String[] args) throws Exception{
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.NIMBUS_THRIFT_PORT, 6627);

        String zkUrl = "127.0.0.1:9092";
        String topic = "test";
        BrokerHosts hosts = new ZkHosts(zkUrl);
        //localhost:9092 but we specified the port explicitly
        Broker brokerForPartition1 = new Broker("localhost", 9092);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,
                UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());


        // Consume new data from the topic
        kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        //zookeeper集群host

        kafkaSpoutConfig.zkServers = Arrays.asList(new String[] {"127.0.0.1"});
        kafkaSpoutConfig.zkPort = 2181;
        System.out.println("kafkaSpoutConfig setting done.");


        TopologyBuilder builder = new TopologyBuilder();

        /**
         * The output field of the RandomSentenceSpout ("word") is provided as the boltMessageField
         * so that this gets written out as the message in the kafka topic.
         */
        builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));
        builder.setBolt("word", new MongoBolt()).shuffleGrouping("kafka-spout");
        builder.setBolt("word-counter", new CounterBolt()).shuffleGrouping("word");

        if (args != null && args.length > 0) {
            System.out.println("args0[0]:" + args[0]);
            config.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
        } else {
            System.out.println("args:" + args);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("KafkaStormDemo", config, builder.createTopology());
            Thread.sleep(10000);
            cluster.killTopology("KafkaStormDemo");
            cluster.shutdown();
        }
    }
}
