import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.codehaus.jettison.json.JSONObject;

public class WinDeck {
  public static class WinDeckMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text wordWeek = new Text();
    private Text wordMonth = new Text();
    private	Text wordGlobal = new Text();
    private IntWritable winValue = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      try {
        JSONObject obj = new JSONObject(value.toString());
        if (obj.has("cards") && obj.has("cards2") && obj.has("win")) {
          String cards = obj.getString("cards");
          String cards2 = obj.getString("cards2");
          int win = Integer.parseInt(obj.getString("win"));
          String date = obj.getString("date");

          SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
          Date parsingDate = dateFormat.parse(date);
          Calendar calendar = Calendar.getInstance();
          calendar.setTime(parsingDate);
          int week = calendar.get(Calendar.WEEK_OF_YEAR);
          int month = calendar.get(Calendar.MONTH) + 1; // month start from 0

          if (win == 0) {
            // for each week
            wordWeek.set("WEEK_" + week + "_" + cards2);
            context.write(wordWeek, winValue);

            // for each month
            wordMonth.set("MONTH_" + month + "_" + cards2);
            context.write(wordMonth, winValue);

            // for global
            wordGlobal.set("GLOBAL_" + cards2);
            context.write(wordGlobal, winValue);
          } else {
            // for each week
            wordWeek.set("WEEK_" + week + "_" + cards);
            context.write(wordWeek, winValue);

            // for each month
            wordMonth.set("MONTH_" + month + "_" + cards);
            context.write(wordMonth, winValue);

            // for global
            wordGlobal.set("GLOBAL_" + cards);
            context.write(wordGlobal, winValue);
          }

        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static class WinDeckReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      Text word = new Text();
      word.set(key.toString() + "," + sum);
      context.write(word, null);
    }
  }

  public static void mainDeck(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "WinDeck");
    job.setNumReduceTasks(1);
    job.setJarByClass(WinDeck.class);
    job.setMapperClass(WinDeckMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(WinDeckReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}