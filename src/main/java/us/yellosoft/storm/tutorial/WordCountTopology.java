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
import java.util.Queue;
import java.util.PriorityQueue;
import java.util.Map;
import java.util.HashMap;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

public class WordCountTopology {
  public static class TextSpout extends BaseRichSpout implements IRichSpout {
    public static final String TEXT_DIRECTORY="resources/sherlock-holmes/";

    private SpoutOutputCollector collector;

    private Queue<String> lines = new PriorityQueue<String>();

    public TextSpout() throws FileNotFoundException, IOException {
      Collection<File> textFiles = FileUtils.listFiles(new File(TEXT_DIRECTORY), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);

      for (File textFile:textFiles) {
        FileReader fileReader = new FileReader(textFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line = bufferedReader.readLine();
        while (line != null) {
          lines.add(line);
          line = bufferedReader.readLine();
        }

        bufferedReader.close();
      }
    }

    public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
      this.collector = collector;
    }

    public void nextTuple() {
      if (lines.peek() != null) {
        collector.emit(new Values(lines.poll()));
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
        if (word.length() > 0) {
          collector.emit(new Values(word));
        }
      }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static class WordCount extends BaseBasicBolt {
    private Map<String, Integer> frequencies = new HashMap<String, Integer>();

    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer oldFrequency = frequencies.get(word);

      if (oldFrequency == null) {
        oldFrequency = 0;
      }

      Integer newFrequency = oldFrequency + 1;

      frequencies.put(word, newFrequency);

      collector.emit(new Values(word, newFrequency));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "frequency"));
    }
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("text-spout", new TextSpout(), 1);
    builder.setBolt("word-splitter", new WordSplitter(), 4).shuffleGrouping("text-spout");
    builder.setBolt("word-counter", new WordCount(), 8).fieldsGrouping("word-splitter", new Fields("word"));

    Config config = new Config();
    config.setDebug(true);
    config.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", config, builder.createTopology());
  }
}
