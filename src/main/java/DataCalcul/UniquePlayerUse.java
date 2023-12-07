package DataCalcul;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.HashSet;

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
import org.codehaus.jettison.json.JSONObject;

public class UniquePlayerUse {

    public static class UniquePlayerUseMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();
        private Text player = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JSONObject obj = new JSONObject(value.toString());
    
                if (obj.has("cards") && obj.has("cards2") && obj.has("player") && obj.has("player2") && obj.has("date")) {
                    String cards1 = SortedDeck.sortDeck(obj.getString("cards"));
                    String cards2 = SortedDeck.sortDeck(obj.getString("cards2"));
                    String player1 = obj.getString("player");
                    String player2 = obj.getString("player2");
                    String date = obj.getString("date");
    
                    LocalDateTime dateTime = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));

                    int month = dateTime.getMonthValue();
                    int week = dateTime.get(WeekFields.ISO.weekOfWeekBasedYear());


                    player.set(player1);
    
                    word.set("WEEK_" + week + "_" + cards1);
                    context.write(word, player);
    
                    word.set("MONTH_" + month + "_" + cards1);
                    context.write(word, player);
    
                    word.set("GLOBAL_" + cards1);
                    context.write(word, player);


                    player.set(player2);
    
                    word.set("WEEK_" + week + "_" + cards2);
                    context.write(word, player);

                    word.set("MONTH_" + month + "_" + cards2);
                    context.write(word, player);
    
                    word.set("GLOBAL_" + cards2);
                    context.write(word, player);
                }
    
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static class UniquePlayerUseReducer extends Reducer<Text, Text, Text, IntWritable> {

        Text word = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            HashSet<String> uniquePlayers = new HashSet<>();

            for (Text value : values) {
                uniquePlayers.add(value.toString());
            }

            word.set(key.toString() + "," + (double) uniquePlayers.size());
            context.write(word, null);

        }
    }


    public static void JobUniquePlayerUse(String input, String output) throws Exception {
    
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "UniquePlayerUse");

      job.setJarByClass(UniquePlayerUse.class);
      job.setMapperClass(UniquePlayerUseMapper.class);
      job.setReducerClass(UniquePlayerUseReducer.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      FileInputFormat.addInputPath(job, new Path(input));
      FileOutputFormat.setOutputPath(job, new Path(output));

      job.waitForCompletion(true);

    }


}


