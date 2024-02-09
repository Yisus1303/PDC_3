import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\\s+");

        // Filtra las líneas que comienzan con "wallet-rest-api"
        if (tokens.length > 2 && tokens[2].equals("[INFO]") || tokens[2].equals("[SEVERE]") || tokens[2].equals("[WARN]")) {
            word.set(tokens[2]);
            context.write(word, one);
        }
    }
}
