import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.ArrayList;
import java.util.List;

public class ClickDistance {
    public static class CDMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String source = tokens[0];
            String dest = tokens[1];
            context.write(new Text(source), new Text(dest+",-1"));

            context.write(new Text(dest), new Text(source+",1"));
        }
    }

    public static class CDReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count0 = 0, count1 = 0;
            List<String> to = new ArrayList<>();
            List<String> from = new ArrayList<>();

            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                if(tokens[1].equals("1")){
                    count1++;
                    from.add(tokens[0]);
                } else{
                    count0++;
                    to.add(tokens[0]);
                }
            }
            if(count0 >= 1 && count1 >= 1){
                for(String w: from){
                    for(String ws: to){
                        context.write(new Text(w+"-"+ws),new Text("("+key+")"));
                    }
                }
            }
        }
    }

  public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("clickDistance", args[0]);
      Job job = Job.getInstance(conf, "click distance");
      job.setJarByClass(ClickDistance.class);
      job.setMapperClass(CDMapper.class);
      job.setReducerClass(CDReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(args[1]));
      FileOutputFormat.setOutputPath(job, new Path(args[2]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}