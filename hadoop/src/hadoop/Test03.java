package hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Test03 extends Configuration implements Tool {
	
	private static Configuration conf = new Configuration();
	
	public static class MapTest03 extends Mapper<LongWritable, Text, Text, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}
	}
	
	/*public static class ReducerTest03 extends Reducer<Text, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Text arg0, Iterable<NullWritable> arg1, Context arg2)
				throws IOException, InterruptedException {
			arg2.write(arg0, NullWritable.get());
		}
	}*/
 	
	public static class keyTest03 implements RawComparator<Text> {

		@Override
		public int compare(Text o1, Text o2) {
			String str1[] = o1.toString().split(" ");
			String str2[] = o2.toString().split(" ");
			if(Integer.parseInt(str1[0]) != Integer.parseInt(str2[0])) {
				return Integer.parseInt(str1[0]) > Integer.parseInt(str2[0]) ? 1 : -1;
			} else if (Integer.parseInt(str1[1]) != Integer.parseInt(str2[1])) {
				return Integer.parseInt(str1[1]) > Integer.parseInt(str2[1]) ? 1 : -1;
			} else {
				return 0;
			}
		}

		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
				int arg4, int arg5) {
			Text int1 = new Text();
			Text int2 = new Text();
			try {
				DataInputBuffer dib = new DataInputBuffer();
				dib.reset(arg0, arg1, arg2);
				int1.readFields(dib);
				dib.reset(arg3, arg4, arg5);
				int2.readFields(dib);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return compare(int1, int2);
		}
	}
	
	public static class ValueTest03 implements RawComparator<Text> {
		@Override
		public int compare(Text o1, Text o2) {
			String str1[] = o1.toString().split(" ");
			String str2[] = o2.toString().split(" ");
			return Integer.parseInt(str1[0]) > Integer.parseInt(str2[0]) ? 1 : -1;
		}

		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
				int arg4, int arg5) {
			Text int1 = new Text();
			Text int2 = new Text();
			try {
				DataInputBuffer dib = new DataInputBuffer();
				dib.reset(arg0, arg1, arg2);
				int1.readFields(dib);
				dib.reset(arg3, arg4, arg5);
				int2.readFields(dib);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return compare(int1, int2);
		}
	}
	
	public static class Test03Partioner extends Partitioner<Text, NullWritable> {

		@Override
		public int getPartition(Text arg0, NullWritable arg1, int arg2) {
			return Math.abs(Integer.parseInt(arg0.toString().split(" ")[0])*127)%arg2;
		}
		
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		conf = arg0;
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(Test03.class);
		job.setJobName("Test03");
		job.setMapperClass(MapTest03.class);
		job.setSortComparatorClass(keyTest03.class);
		job.setGroupingComparatorClass(ValueTest03.class);
		job.setPartitionerClass(Test03Partioner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://master:8020/user/hadoop/test02"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:8020/user/hadoop/test02output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) {
		try {
			System.exit(ToolRunner.run(new Test03(), args));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
