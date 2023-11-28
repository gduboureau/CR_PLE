import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONObject;

public class FilterData {

      public static class FilterMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		  try {
            JSONObject obj = new JSONObject(value.toString());
            if (obj.getString("cards") != null && obj.getString("cards2") != null 
                && obj.getString("clanTr") != null && obj.getString("clanTr2") != null) {
                    context.write(NullWritable.get(), value);
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
	  }
  }
  public static class FilterReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(NullWritable.get(), value);
        }
    }
  }
    
}
