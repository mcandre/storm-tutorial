package us.yellosoft.storm.tutorial;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
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
import java.io.IOException;

/** Word count frequency topology */
public final class WordCountTopology {
  /** Utility class */
  private WordCountTopology() {}

  /** Text generator */
  public static class TextSpout extends BaseRichSpout implements IRichSpout {
    public static final long serialVersionUID = 1L;

    public static final String TEXT_DIRECTORY = "resources/sherlock-holmes/";

    private SpoutOutputCollector collector;

    private Queue<String> lines = new PriorityQueue<String>();

    /** Construct a TextSpout
        @throws IOException on IO error
     */
    public TextSpout() throws IOException {
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

    /** Open the spout
        @param stormConf Storm configuration
        @param context topology
        @param collector text emitter
     */
    public void open(
      final Map stormConf,
      final TopologyContext context,
      final SpoutOutputCollector collector
    ) {
      this.collector = collector;
    }

    /** Generate next text */
    public void nextTuple() {
      if (lines.peek() != null) {
        collector.emit(new Values(lines.poll()));
      }
    }

    /** Specify emitted fields
        @param declarer Storm specification
     */
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("line"));
    }
  }

  /** Word splitting bolt */
  public static class WordSplitter extends BaseBasicBolt {
    public static final long serialVersionUID = 1L;

    /** Split text into words
        @param tuple text
        @param collector word emitter
     */
    public void execute(
      final Tuple tuple,
      final BasicOutputCollector collector
    ) {
      String[] words = tuple.getString(0).split(" ");

      for (String word:words) {
        if (word.length() > 0) {
          collector.emit(new Values(word));
        }
      }
    }

    /** Emitter specification
        @param declarer Storm specification
     */
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  /** Frequency counter */
  public static class WordCount extends BaseBasicBolt {
    public static final long serialVersionUID = 1L;

    private Map<String, Integer> frequencies = new HashMap<String, Integer>();

    /** Count word frequencies
        @param tuple words
        @param collector frequency emitter
     */
    public void execute(
      final Tuple tuple,
      final BasicOutputCollector collector
    ) {
      final String word = tuple.getString(0);
      Integer oldFrequency = frequencies.get(word);

      if (oldFrequency == null) {
        oldFrequency = 0;
      }

      final Integer newFrequency = oldFrequency + 1;

      frequencies.put(word, newFrequency);

      collector.emit(new Values(word, newFrequency));
    }

    /** Emitter specification
        @param declarer Storm specification
     */
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "frequency"));
    }
  }

  /** CLI entry point
      @param args CLI flags
      @throws Exception on error
   */
  public static void main(final String[] args) throws Exception {
    final TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("text-spout", new TextSpout(), 1);
    builder.setBolt("word-splitter", new WordSplitter(), 4).shuffleGrouping("text-spout");
    builder.setBolt("word-counter", new WordCount(), 8).fieldsGrouping("word-splitter", new Fields("word"));

    final Config config = new Config();
    config.setDebug(true);
    config.setMaxTaskParallelism(3);

    final LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", config, builder.createTopology());
  }
}
