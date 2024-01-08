package DataCalcul;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.codehaus.jettison.json.JSONObject;


/**
 * The StatsCalculs class is responsible for calculating statistics based on input data.
 * It contains a mapper and reducer class for processing the data and generating the output.
 */
public class StatsCalculs {

  /**
   * Mapper class for calculating statistics in the Hadoop job.
   * This class extends the Mapper class and overrides the map() method to process input key-value pairs.
   * The input key is of type LongWritable and the input value is of type Text.
   * The output key is of type Text and the output value is of type DeckStats.
   */
  /**
   * Mapper class for calculating statistics.
   * This class extends the Mapper class and overrides the map() method to process input key-value pairs and generate intermediate key-value pairs for statistics calculation.
   */
  public static class StatsCalculsMapper extends Mapper<LongWritable, Text, Text, DeckStats> {
    /**
     * Processes input key-value pairs and generates intermediate key-value pairs for statistics calculation.
     *
     * @param key     The input key.
     * @param value   The input value.
     * @param context The context object for writing intermediate key-value pairs.
     * @throws IOException          If an I/O error occurs.
     * @throws InterruptedException If the thread is interrupted.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      try {
        // Parse the input value as a JSON object
        JSONObject obj = new JSONObject(value.toString());
        int win = Integer.parseInt(obj.getString("win"));

        if (obj.has("cards") && obj.has("cards2") && obj.has("date")) {
          // Sort the deck cards
          String cards = SortedDeck.sortDeck(obj.getString("cards"));
          String cards2 = SortedDeck.sortDeck(obj.getString("cards2"));

          if (cards.length() > 16 || cards2.length() > 16) { // Ignore decks with more than 8 cards
            return;
          }

          String date = obj.getString("date");

          LocalDateTime dateTime = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));

          int month = dateTime.getMonthValue();
          int week = dateTime.get(WeekFields.ISO.weekOfWeekBasedYear());

          double diffForceWin = 0;

          if (obj.has("deck") && obj.has("deck2")) {
            double deck1 = obj.getDouble("deck");
            double deck2 = obj.getDouble("deck2");
            diffForceWin = Math.abs(deck1 - deck2);
          }

          DeckStats deckStats1 = new DeckStats();
          DeckStats deckStats2 = new DeckStats();

          deckStats1.setUseDeck(1);
          deckStats2.setUseDeck(1);
          deckStats1.addPlayer(obj.getString("player"));
          deckStats2.addPlayer(obj.getString("player2"));

          if (win == 1) {
            double clanTr = 0;
            if (obj.has("clanTr")) {
              clanTr = obj.getDouble("clanTr");
            }
            deckStats1.setBestClan(clanTr);
            deckStats1.setWinDeck(1);
            deckStats1.setDiffForceWin(diffForceWin);

          } else if (win == 0) {
            double clanTr = 0;
            if (obj.has("clanTr2")) {
              clanTr = obj.getDouble("clanTr2");
            }
            deckStats2.setBestClan(clanTr);
            deckStats2.setWinDeck(1);
            deckStats2.setDiffForceWin(diffForceWin);
          }

          // Write intermediate key-value pairs for global, month, and week statistics
          context.write(new Text("GLOBAL_" + cards), deckStats1);
          context.write(new Text("MONTH_" + month + "_" + cards), deckStats1);
          context.write(new Text("WEEK_" + week + "_" + cards), deckStats1);
          context.write(new Text("GLOBAL_" + cards2), deckStats2);
          context.write(new Text("MONTH_" + month + "_" + cards2), deckStats2);
          context.write(new Text("WEEK_" + week + "_" + cards2), deckStats2);
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Reducer class for calculating statistics in the Hadoop MapReduce job.
   * It takes a Text key, Iterable of DeckStats values, and Context as input,
   * and emits a Text key and DeckStats value as output.
   */
  public static class StatsCalculsReducer extends Reducer<Text, DeckStats, Text, DeckStats> {

    /**
     * Reducer function that calculates the statistics for each key.
     * It iterates over the DeckStats values, aggregates the data, and emits the result.
     *
     * @param key      The input key.
     * @param values   The Iterable of DeckStats values.
     * @param context  The Context object for emitting the output.
     * @throws IOException          If an I/O error occurs.
     * @throws InterruptedException If the thread is interrupted.
     */
    @Override
    public void reduce(Text key, Iterable<DeckStats> values, Context context) throws IOException, InterruptedException {

      DeckStats deckStats = new DeckStats();
      Set<String> players = new HashSet<String>();
      double diffForceWin = 0;
      for (DeckStats val : values) {
        players.addAll(val.getPlayers());
        deckStats.setWinDeck(deckStats.getWinDeck() + val.getWinDeck());
        deckStats.setUseDeck(deckStats.getUseDeck() + val.getUseDeck());
        diffForceWin += val.getDiffForceWin();
        deckStats.setBestClan(Math.max(deckStats.getBestClan(), val.getBestClan()));
      }

      if (deckStats.getUseDeck() > 0) {
        deckStats.setDiffForceWin(diffForceWin / deckStats.getUseDeck());
      }

      deckStats.setPlayers(players);

      context.write(key, deckStats);

    }
  }

  public static void mainStatsCalculs(String input, String output) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "StatsCalculs");
    job.setNumReduceTasks(5);
    job.setJarByClass(StatsCalculs.class);
    job.setMapperClass(StatsCalculsMapper.class);
    job.setReducerClass(StatsCalculsReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DeckStats.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    job.waitForCompletion(true);
  }
}
