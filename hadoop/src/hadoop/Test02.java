package hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Test02 extends Configuration implements Tool {
	
	private Configuration conf = new Configuration();
	
	public static class MapTest02 extends Mapper<LongWritable, Text, MyNumber, IntWritable> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String outValue = value.toString();
			String strs[] = outValue.split(" ");
			MyNumber number = new MyNumber();
			number.setFirst(Integer.parseInt(strs[0]));
			number.setSecond(Integer.parseInt(strs[1]));
			context.write(number, new IntWritable(Integer.parseInt(strs[1])));
		}
	}
	
	public static class ReduceTest02 extends Reducer<MyNumber, IntWritable, Text, IntWritable> {
		
		@Override
		protected void reduce(MyNumber key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
			for(IntWritable v : value) {
				context.write(new Text(Integer.toString(key.getFirst())), v);
			}
		}
		
	}
	
	public static class Test02KeyCompare implements RawComparator<MyNumber> {

		@Override
		public int compare(MyNumber o1, MyNumber o2) {
			if(o1.getFirst() != o2.getFirst()){
				return o1.getFirst() > o2.getFirst() ? 1 : -1;
			}else if(o1.getSecond() != o2.getSecond()){
				return o1.getSecond() > o2.getSecond() ? -1 : 1;
			}else{
				return 0;
			}
		}

		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
				int arg4, int arg5) {
			MyNumber key1 = new MyNumber();
			MyNumber key2 = new MyNumber();
			DataInputBuffer in = new DataInputBuffer();
			try {
				in.reset(arg0, arg1, arg2);
				key1.readFields(in);
				in.reset(arg3, arg4, arg5);
				key2.readFields(in);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return compare(key1, key2);
		}
		
	}
	
	public static class Test02ValueCompare implements RawComparator<MyNumber> {

		@Override
		public int compare(MyNumber o1, MyNumber o2) {
			if(o1.getFirst() == o2.getFirst()){
				return 0;
			}else if(o1.getFirst() > o2.getFirst()){
				return 1;
			}else{
				return -1;
			}
		}

		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
				int arg4, int arg5) {
			MyNumber key1 = new MyNumber();
			MyNumber key2 = new MyNumber();
			DataInputBuffer in = new DataInputBuffer();
			try {
				in.reset(arg0, arg1, arg2);
				key1.readFields(in);
				in.reset(arg3, arg4, arg5);
				key2.readFields(in);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return compare(key1, key2);
		}
		
	}
	
	public static class FirstPartitioner extends Partitioner<MyNumber, IntWritable> {

		@Override
		public int getPartition(MyNumber arg0, IntWritable arg1, int arg2) {
			return Math.abs(arg0.getFirst()*127)%arg2;
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
		Configuration conf = getConf();
		Job job = new Job(conf, "Test02");
		job.setJarByClass(Test02.class);
		job.setMapperClass(MapTest02.class);
		job.setReducerClass(ReduceTest02.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setSortComparatorClass(Test02KeyCompare.class);
		job.setGroupingComparatorClass(Test02ValueCompare.class);
		job.setMapOutputKeyClass(MyNumber.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(MyNumber.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://master:8020/user/hadoop/test02"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:8020/user/hadoop/test02output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) {
		try {
			int res = ToolRunner.run(new Test02(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
