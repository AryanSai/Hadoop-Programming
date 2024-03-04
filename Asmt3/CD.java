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

public class CD {
    public static class CDMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String source = tokens[0].trim();
            String dest = tokens[1].trim();
            context.write(new Text(source), new Text(dest+",0"));
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
                        context.write(new Text(w+","),new Text(ws));
                    }
                }
            }
        }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("clickDistance", args[0]);
    int clickDistance = Integer.parseInt(args[0]);

    for (int i = 1; i < clickDistance; i++) {
        Job job = Job.getInstance(conf, "click distance " + (i + 1));
        job.setJarByClass(CD.class);
        job.setMapperClass(CDMapper.class);
        job.setReducerClass(CDReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        if (i == 1) {
            FileInputFormat.addInputPath(job, new Path(args[1]));
        } else {
            FileInputFormat.addInputPath(job, new Path(args[2] + "_" + i));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[2] + "_" + (i + 1)));

        job.waitForCompletion(true);
    }
  }
}