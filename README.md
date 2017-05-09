# assignmentmap4.2
package mapreduce;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalUnits {

public static class MapperClass extends
  Mapper<Text, Text, Text, IntWritable> {
 public void map(Text key, Text tvRecord, Context con)
   throws IOException, InterruptedException {
  String[] words = tvRecord.toString().split("|");
  String CompName= words[0];
  try {
   int count = Integer.parseInt(CompName);
   con.write(new Text(CompName), new IntWritable(count));
  } catch (Exception e) {
   e.printStackTrace();
  }
 }
}

public static class ReducerClass extends
  Reducer<Text, IntWritable, Text, Text> {
 public void reduce(Text key, Iterable<IntWritable> valueList,
   Context con) throws IOException, InterruptedException {
  try {

   int tcount = 0;   
   int count= 0;
   for (IntWritable var : valueList) {
        count = var.get();
          tcount++;
   }
    String out =  "Count: " + tcount;
   con.write(key, new Text(out));
  } catch (Exception e) {
   e.printStackTrace();
  }
 }
}

public static void main(String[] args) {
 Configuration conf = new Configuration();
 try {
  Job job = Job.getInstance(conf, "TotalUnits");
  job.setJarByClass(AverageAndTotalSalaryCompute.class);
  job.setMapperClass(MapperClass.class);
  job.setReducerClass(ReducerClass.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);
   Path p1 = new Path(args[0]);
   Path p2 = new Path(args[1]);
   FileInputFormat.addInputPath(job, p1);
   FileOutputFormat.setOutputPath(job, p2);
   FileInputFormat.addInputPath(job, p1);
  FileOutputFormat.setOutputPath(job, p2);
  System.exit(job.waitForCompletion(true) ? 0 : 1);
 } catch (IOException e) {
  e.printStackTrace();
 } catch (ClassNotFoundException e) {
  e.printStackTrace();
 } catch (InterruptedException e) {
  e.printStackTrace();
 }

}
}
