package storm.start;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.WordCountBolt;
import bolts.WordNormalizerBolt;
import spouts.RandomWordSpout;
import spouts.WordSpout;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
    	TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomWordSpout(), 5);

        builder.setBolt("wordCount", new WordCountBolt(), 8).fieldsGrouping("spout", new Fields("word"));
        builder.setBolt("wordNormalzer", new WordNormalizerBolt(), 12).fieldsGrouping("wordCount", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);


        if (args != null && args.length > 0) {
          conf.setNumWorkers(3);

          StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
          conf.setMaxTaskParallelism(3);

          LocalCluster cluster = new LocalCluster();
          cluster.submitTopology("word-count", conf, builder.createTopology());

          Thread.sleep(10000);

          cluster.shutdown();
        }
    }
}
