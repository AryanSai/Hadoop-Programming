import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class ChainBill {
    public static class FirstMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private static IntWritable billId = new IntWritable();
        private static IntWritable billAmount = new IntWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] result = value.toString().split(",");
            int branch_id = Integer.parseInt(result[0]);
            int bill_amount = Integer.parseInt(result[1]);
            billId.set(branch_id);
            billAmount.set(bill_amount);
            context.write(billId, billAmount);
        }
    }

    public static class AverageReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0, count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public static class SecondMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable branchId = new IntWritable();
        private IntWritable average = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] result = value.toString().split(",");
            int branchIdValue = Integer.parseInt(result[0]);
            int averageValue = Integer.parseInt(result[1]);

            branchId.set(branchIdValue);
            average.set(averageValue);

            context.write(branchId, average);
        }   
    }

    public static class SecondReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(val, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "FirstJob");

        job1.setJarByClass(ChainBill.class);
        job1.setMapperClass(FirstMapper.class);
        job1.setReducerClass(AverageReducer.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "SecondJob");

        job2.setJarByClass(ChainBill.class);
        job2.setMapperClass(SecondMapper.class);
        job2.setReducerClass(SecondReducer.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
