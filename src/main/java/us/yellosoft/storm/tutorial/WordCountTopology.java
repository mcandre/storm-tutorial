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
import java.util.Scanner;
import java.nio.charset.StandardCharsets;
import java.io.IOException;

/** Word count frequency topology */
public final class WordCountTopology {
  /** Utility class */
  private WordCountTopology() {}

  /** Text generator */
  public static class TextSpout extends BaseRichSpout implements IRichSpout {
    public static final long serialVersionUID = 1L;

    public static final String TEXT_DIRECTORY = "resources/sherlock-holmes/";

    private transient SpoutOutputCollector collector;

    private Queue<String> lines = new PriorityQueue<String>();

    /** Construct a TextSpout
        @throws IOException on IO error
     */
    public TextSpout() throws IOException {
      Collection<File> textFiles = FileUtils.listFiles(new File(TEXT_DIRECTORY), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);

      for (File textFile:textFiles) {
        Scanner scanner = new Scanner(textFile, StandardCharsets.UTF_8.name());
        while (scanner.hasNextLine()) {
          lines.add(scanner.nextLine());
        }

        scanner.close();
      }
    }

    @Override
    public void open(
      final Map stormConf,
      final TopologyContext context,
      final SpoutOutputCollector collector
    ) {
      this.collector = collector;
    }

    @Override
    public void nextTuple() {
      if (lines.peek() != null) {
        collector.emit(new Values(lines.poll()));
      }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("line"));
    }
  }

  /** Word splitting bolt */
  public static class WordSplitter extends BaseBasicBolt {
    public static final long serialVersionUID = 1L;

    @Override
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

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  /** Frequency counter */
  public static class WordCount extends BaseBasicBolt {
    public static final long serialVersionUID = 1L;

    private Map<String, Integer> frequencies = new HashMap<String, Integer>();

    @Override
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

    @Override
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
