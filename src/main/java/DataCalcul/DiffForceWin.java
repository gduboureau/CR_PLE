package DataCalcul;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jettison.json.JSONObject;


public class DiffForceWin {

    public static class DiffForceWinMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text word = new Text();
        private DoubleWritable diffForce = new DoubleWritable();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JSONObject obj = new JSONObject(value.toString());
    
                if (obj.has("cards") && obj.has("cards2") && obj.has("deck") && obj.has("deck2") && obj.has("win") && obj.has("date")) {
                    String cards;
                    double deck1 = obj.getInt("deck");
                    double deck2 = obj.getInt("deck2");
                    int win = obj.getInt("win");
                    String date = obj.getString("date");
    
                    LocalDateTime dateTime = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));

                    int month = dateTime.getMonthValue();
                    int week = dateTime.get(WeekFields.ISO.weekOfWeekBasedYear());

                    if (win == 1){ //le player 1 gagne
                        cards = obj.getString("cards");
                        diffForce.set(Math.abs(deck1 - deck2));


                    }else{
                        // clanLvl.set(clanTr2);
                        cards = obj.getString("cards2");
                        diffForce.set(Math.abs(deck2 - deck1));
                    }
    
                    word.set("WEEK_" + week + "_" + cards);
                    context.write(word, diffForce);
    
                    word.set("MONTH_" + month + "_" + cards);
                    context.write(word, diffForce);
    
                    word.set("GLOBAL_" + cards);
                    context.write(word, diffForce);

                }
    
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static class DiffForceWinReducer extends Reducer<Text, DoubleWritable, Text, IntWritable> {

        Text word = new Text();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            
            int cpt = 0;
            double sum = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                cpt++;
            }

            word.set(key.toString() + "," + (sum / cpt));
            context.write(word, null);

        }
    }


    public static void JobDiffForceWin(String input, String ouput) throws Exception {
    
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "UniquePlayerUse");
  
        job.setJarByClass(DiffForceWin.class);
        job.setMapperClass(DiffForceWinMapper.class);
        job.setReducerClass(DiffForceWinReducer.class);
  
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
  
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(ouput));
  
        job.waitForCompletion(true);
  
      }
    
}
