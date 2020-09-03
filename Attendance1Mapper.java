import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//first, I want to check if absent + present == enrolled
//just to see if there are any inconsistencies...
public class Attendance1Mapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		String val = value.toString();
		String[] line = val.split(",");
		String outKey = "";

		if (line[0] != "School DBN") //not header row
		{

			//check if line[2] == line[3] + line[4]
			if (line[2] != line[3] + line[4]) {
				outKey = "good_record";
			} else {
				outKey = "bad_record";
			}

			context.write(new Text(outKey), new IntWritable(1));
		}
	}
}