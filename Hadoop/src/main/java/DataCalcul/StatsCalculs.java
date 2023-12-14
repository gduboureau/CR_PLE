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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.codehaus.jettison.json.JSONObject;


public class StatsCalculs {

  public static class StatsCalculsMapper extends Mapper<LongWritable, Text, Text, DeckStats> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      
      try {

        JSONObject obj = new JSONObject(value.toString());
        int win = Integer.parseInt(obj.getString("win"));

        if (obj.has("cards") && obj.has("cards2") && obj.has("date")) {

          String cards = SortedDeck.sortDeck(obj.getString("cards"));
          String cards2 = SortedDeck.sortDeck(obj.getString("cards2"));

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


          // for global
          context.write(new Text("GLOBAL_" + cards), deckStats1);

          // for month
          context.write(new Text("MONTH_" + month + "_" + cards), deckStats1);

          // for week
          context.write(new Text("WEEK_" + week + "_" + cards), deckStats1);

          // for global
          context.write(new Text("GLOBAL_" + cards2), deckStats2);

          // for month
          context.write(new Text("MONTH_" + month + "_" + cards2), deckStats2);

          // for week
          context.write(new Text("WEEK_" + week + "_" + cards2), deckStats2);

        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static class StatsCalculsReducer extends Reducer<Text, DeckStats, Text, DeckStats> {

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
    job.setNumReduceTasks(1);
    job.setJarByClass(StatsCalculs.class);
    job.setMapperClass(StatsCalculsMapper.class);
    job.setReducerClass(StatsCalculsReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DeckStats.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    job.waitForCompletion(true);
  }
}
