package hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Test01 extends Configuration implements Tool {
	
	private static Configuration conf = new Configuration();
	
	public static class MapTest01 extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String outValue = value.toString();
			String strs[] = outValue.split(" ");
			LongWritable num = new LongWritable(Long.valueOf(strs[1]));
			Text text = new Text();
			text.set(strs[0]);
			context.write(num, text);
		}
	}
	
	public static class KeyCompare implements RawComparator<LongWritable> {

		@Override
		public int compare(LongWritable o1, LongWritable o2) {
			return -o1.compareTo(o2);
		}

		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
				int arg4, int arg5) {
			LongWritable key1 = new LongWritable();
			LongWritable key2 = new LongWritable();
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
		Job job = new Job(conf);
		job.setMapperClass(MapTest01.class);
		job.setJobName("Test01");
		job.setJarByClass(Test01.class);
		job.setSortComparatorClass(KeyCompare.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://master:8020/test"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:8020/testoutput"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) {
		try {
			int result = ToolRunner.run(new Test01(), args);
			System.exit(result);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
