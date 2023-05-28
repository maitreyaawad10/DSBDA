import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
	
		
	
public class LogFile {
	public static void main(String [] args) throws Exception
	{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job j = Job.getInstance(c, "Word Count");
		j.setJarByClass(LogFile.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}
	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text user = new Text();
		private IntWritable duration = new IntWritable();
		
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
	
			String data = value.toString();
			String[] tokens=data.split(",");
			String user = tokens[1];
			String ipAddress = tokens[2];
			String loginTime = tokens[6];
			
			this.user.set(user + " (" + ipAddress + ")");
			this.duration.set(1);
			con.write(this.user, this.duration);
		}
	}
	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private Text maxUser = new Text();
        private IntWritable maxDuration = new IntWritable(0);
        
		public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			int totalDuration = 0;
            for (IntWritable value : values) {
                totalDuration += value.get();
            }

            if (totalDuration > maxDuration.get()) {
                maxUser.set(key);
                maxDuration.set(totalDuration);
            }
		}
		protected void cleanup (Context con) throws IOException, InterruptedException{
			con.write(maxUser, maxDuration);
		}
	}
}