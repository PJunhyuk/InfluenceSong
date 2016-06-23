
import java.io.IOException;
// import java.io.InputStreamReader;
// import java.io.Reader;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Step2 {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text customer_no = new Text();
		private Text songndata = new Text();
		
		public MyMapper() {
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] songncustomerndata = value.toString().split("\t");
			String[] songncustomer = songncustomerndata[0].split("@");
			this.customer_no.set(songncustomer[1]);
			this.songndata.set(songncustomer[0] + "=" + songncustomerndata[1]);
			context.write(this.customer_no, this.songndata);
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private Text songncustomer = new Text();
		private Text datansongincustomer = new Text();
		
		public MyReducer() {
		}
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int songincustomer = 0;
			Map<String, Integer> temp = new HashMap<String, Integer>();
			for (Text val : values) {
				String[] songndata = val.toString().split("=");
				temp.put(songndata[0], Integer.valueOf(songndata[1]));
				songincustomer += Integer.parseInt(val.toString().split("=")[1]);
			}
			for (String songkey : temp.keySet()) {
				this.songncustomer.set(songkey + "@" + key.toString());
				this.datansongincustomer.set(temp.get(songkey) + "/" + songincustomer);
				context.write(this.songncustomer, this.datansongincustomer);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		conf.set("mapred.textoutputformat.separator", "@");
		
		Job job = new Job(conf, "WordCount");

		job.setJarByClass(Step2.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// if mapper outputs are different, call setMapOutputKeyClass and
		// setMapOutputValueClass
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// An InputFormat for plain text files. Files are broken into lines.
		// Either linefeed or carriage-return are used to signal end of
		// Keys are the position in the file, and values are the line of text..
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
