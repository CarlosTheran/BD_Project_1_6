package Part6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Created by carlos on 03-25-17.
 */
public class TwitterMessageByUserReducer extends Reducer<Text, Text,Text, Text>  {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        String result = "[ ";

        for (Text value : values){
            result = result + value;
            result = result + " |New massage| ";
            count = count +1;
        }

        Integer.toString(count);
        result = result + count;
        result = result + " ]";

        context.write(key, new Text(result));

    }


}
