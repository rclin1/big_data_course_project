import java.util.StringTokenizer;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper
 extends Mapper<LongWritable, Text, Text, Text> {
    final static IntWritable one = new IntWritable(1);
    Text word = new Text();

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split(",");
        
        String out = arr[0] + "," + arr[1] + "," + arr[2] + "," + arr[3];
        context.write(new Text(""), new Text(out)); //blank as key or blank as value
    }
}

