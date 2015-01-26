package us.yellosoft.storm.tutorial;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.spout.ISpout;
import backtype.storm.spout.ShellSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.IBolt;
import backtype.storm.topology.BasicBoltExecutor;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;

import java.util.Collection;
import java.util.Queue;
import java.util.PriorityQueue;
import java.util.Map;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;

public class WordCountTopology {
  public static class TextSpout extends ShellSpout implements ISpout {
    public static final String TEXT_DIRECTORY="resources/sherlock-holmes/";

    private SpoutOutputCollector collector;

    private Queue<BufferedReader> bufferedReaders = new PriorityQueue<BufferedReader>();

    public TextSpout() {
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
        String line = bufferedReader.readLine();

        if (line != null) {
          collector.emit(line);
        }
        else {
          bufferedReader.close();

          bufferedReaders.poll();
        }
      }
    }
  }

  public static class WordSplitter extends BasicBoltExecutor implements IBolt {
    public void process(Tuple tuple) {
      String[] words = tuples.values[0].split(" ");
      for (String word:words) {
        tuple.emit(word);
      }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }
}
