package us.yellosoft.storm.tutorial;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.PriorityQueue;
import java.util.Map;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

public class WordCountTopology {
  public static class TextSpout extends BaseRichSpout implements IRichSpout {
    public static final String TEXT_DIRECTORY="resources/sherlock-holmes/";

    private SpoutOutputCollector collector;

    private Queue<BufferedReader> bufferedReaders = new PriorityQueue<BufferedReader>();

    public TextSpout() throws FileNotFoundException, IOException {
      Collection<File> textFiles = FileUtils.listFiles(new File(TEXT_DIRECTORY), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);

      for (File textFile:textFiles) {
        FileReader fileReader = new FileReader(textFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        bufferedReaders.add(bufferedReader);
      }
    }

    public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
      this.collector = collector;
    }

    public void nextTuple() {
      BufferedReader bufferedReader = bufferedReaders.peek();
      if (bufferedReader != null) {
        try {
          String line = bufferedReader.readLine();

          if (line != null) {
            List<Object> tuple = new ArrayList<Object>();
            tuple.add(line);
            collector.emit(tuple);
          }
          else {
            bufferedReader.close();

            bufferedReaders.poll();
          }
        }
        catch(IOException e) {
          collector.reportError(e);
        }
      }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("line"));
    }
  }

  public static class WordSplitter extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String[] words = tuple.getString(0).split(" ");

      for (String word:words) {
        List<Object> tuple2 = new ArrayList<Object>();
        tuple2.add(word);
        collector.emit(tuple2);
      }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("textSpout", new TextSpout(), 1);
    builder.setBolt("wordSplitter", new WordSplitter(), 5).shuffleGrouping("textSpout");

    Config config = new Config();
    config.setDebug(true);
    config.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", config, builder.createTopology());
  }
}
