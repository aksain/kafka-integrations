package com.aksain.kafka.storm;

import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author Amit Kumar
 *
 */
public class KafkaStormIntegrationDemo {
	
	private static final Logger LOG = Logger.getLogger(KafkaStormIntegrationDemo.class);

	public static void main(String[] args) {
		
		if(args.length < 4) {
			LOG.fatal("Incorrect number of arguments. Required arguments: <zk-hosts> <kafka-topic> <zk-path> <clientid>");
			System.exit(1);
		}
		
		// Build Spout configuration using input command line parameters
		final BrokerHosts zkrHosts = new ZkHosts(args[0]);
		final String kafkaTopic = args[1];
		final String zkRoot = args[2];
		final String clientId = args[3];
		final SpoutConfig kafkaConf = new SpoutConfig(zkrHosts, kafkaTopic, zkRoot, clientId);
		kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

		// Build topology to consume message from kafka and print them on console
		final TopologyBuilder topologyBuilder = new TopologyBuilder();
		// Create KafkaSpout instance using Kafka configuration and add it to topology
		topologyBuilder.setSpout("kafka-spout", new KafkaSpout(kafkaConf), 1);
		//Route the output of Kafka Spout to Logger bolt to log messages consumed from Kafka
		topologyBuilder.setBolt("print-messages", new LoggerBolt()).globalGrouping("kafka-spout");
		
		// Submit topology to local cluster i.e. embedded storm instance in eclipse
		final LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("kafka-topology", new HashMap<>(), topologyBuilder.createTopology());
	}

}