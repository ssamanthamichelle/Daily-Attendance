import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class Attendance1 {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Attendance1 <input path> <output path>");
			System.exit(-1);
		}

		final Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", "|");
		Job job = Job.getInstance(conf);

		job.setJarByClass(Attendance1.class);
		job.setJobName("Attendance 1");

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Attendance1Mapper.class);
		job.setReducerClass(Attendance1Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}