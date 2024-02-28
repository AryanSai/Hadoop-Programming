import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ClickDistance {
  public static class ClickDistanceMapper extends Mapper<Object, Text, Text, Text> {
      private Text source = new Text();
      private Text destination = new Text();
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          String[] tokens = value.toString().split(",");
          source.set(tokens[0]);
          destination.set(tokens[1]);
          context.write(source, destination);
      }
  }
  public static class ClickDistanceReducer extends Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int clickDistance = Integer.parseInt(context.getConfiguration().get("clickDistance"));
      List<String> destinations = new ArrayList<>();
      for (Text value : values) {
          destinations.add(value.toString());
      }
      if (clickDistance == 1) {
          for (String dest : destinations) {
              context.write(new Text(key.toString() + "-" + dest), new Text());
          }
      } else if (clickDistance > 1) {
          List<String> intermediateNodes = new ArrayList<>(destinations);
          List<String> allIntermediates = new ArrayList<>();
          for (int i = 1; i < clickDistance; i++) {
              List<String> newIntermediates = new ArrayList<>();
              for (String node : intermediateNodes) {
                  if (destinations.contains(node)) {
                      newIntermediates.add(node);
                      allIntermediates.add(node);
                  }
              }
              intermediateNodes = newIntermediates;
          }
          StringBuilder intermediateStr = new StringBuilder("(");
          for (String intermediate : allIntermediates) {
              intermediateStr.append(intermediate).append(",");
          }
          if (intermediateStr.length() > 1) {
              intermediateStr.setLength(intermediateStr.length() - 1);
          }
          intermediateStr.append(")");
          for (String dest : destinations) {
              context.write(new Text(key.toString() + "-" + dest), new Text(intermediateStr.toString()));
          }
      }
  }
}

  public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("clickDistance", args[0]);
      Job job = Job.getInstance(conf, "click distance");
      job.setJarByClass(ClickDistance.class);
      job.setMapperClass(ClickDistanceMapper.class);
      job.setReducerClass(ClickDistanceReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(args[1]));
      FileOutputFormat.setOutputPath(job, new Path(args[2]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}