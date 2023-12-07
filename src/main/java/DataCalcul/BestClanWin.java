package DataCalcul;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;


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


public class BestClanWin {

    public static class BestClanWinMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text word = new Text();
        private IntWritable clanLvl = new IntWritable();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JSONObject obj = new JSONObject(value.toString());
    
                if (obj.has("cards") && obj.has("cards2") && obj.has("clanTr") && obj.has("clanTr2") && obj.has("win") && obj.has("date")) {
                    String cards;
                    int clanTr1 = obj.getInt("clanTr");
                    int clanTr2 = obj.getInt("clanTr2");
                    int win = obj.getInt("win");
                    String date = obj.getString("date");
    
                    LocalDateTime dateTime = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));

                    int month = dateTime.getMonthValue();
                    int week = dateTime.get(WeekFields.ISO.weekOfWeekBasedYear());

                    if (win == 1){ //le player 1 gagne
                        clanLvl.set(clanTr1);
                        cards = SortedDeck.sortDeck(obj.getString("cards"));

                    }else{
                        clanLvl.set(clanTr2);
                        cards = SortedDeck.sortDeck(obj.getString("cards2"));
                    }
    
                    word.set("WEEK_" + week + "_" + cards);
                    context.write(word, clanLvl);
    
                    word.set("MONTH_" + month + "_" + cards);
                    context.write(word, clanLvl);
    
                    word.set("GLOBAL_" + cards);
                    context.write(word, clanLvl);

                }
    
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static class BestClanWinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        Text word = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            
            double maxLvl = 0;

            for (IntWritable value : values) {
                maxLvl = Math.max(maxLvl, value.get());
            }

            word.set(key.toString() + "," + (double) maxLvl);
            context.write(word, null);

        }
    }


    public static void JobBestClanWin(String input, String ouput) throws Exception {
    
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "UniquePlayerUse");
  
        job.setJarByClass(BestClanWin.class);
        job.setMapperClass(BestClanWinMapper.class);
        job.setReducerClass(BestClanWinReducer.class);
  
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
  
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(ouput));
  
        job.waitForCompletion(true);
  
      }
    
}
