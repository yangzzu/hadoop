package hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configuration implements Tool {
	
	private static Configuration conf = new Configuration();
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			IntWritable one = new IntWritable(1);
			Text word = new Text();
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				System.out.println("key=" + key + ",line=" + line + ",word=" + word);
				context.write(word, one);
			}
		}
		
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1, Context arg2)
				throws IOException, InterruptedException {
			int sum = 0;
			while (arg1.iterator().hasNext()){
				arg1.iterator().next();
				sum = sum + 1;
			}
			System.out.println("key=" + arg0 + ",sum=" + sum);
			arg2.write(arg0, new IntWritable(sum));
		}
		
	}
	
	public static class KeyCompare extends WritableComparator {
		
		public KeyCompare() {
			super(Text.class,true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text aT = (Text) a;
			Text bT = (Text) b;
			return -super.compare(aT, bT);
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
		Job job = new  Job(getConf());
		job.setJarByClass(WordCount.class);
		job.setJobName("WordCount");
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setSortComparatorClass(KeyCompare.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setCombinerClass(Reduce.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://master:8020/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:8020/output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) {
		try {
			System.exit(ToolRunner.run(new WordCount(), args));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
