package Part6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.*;

import java.io.IOException;
/**
 * Created by carlos on 03-25-17.
 */
public class TwitterMessageByUserMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String tweetScreenName = status.getUser().getScreenName();
            String tweet = status.getText();
            //long originaluserid = originalTweet.getUser().getId();

            context.write(new Text(tweetScreenName), new Text(tweet));


        }
        catch(TwitterException e){

        }

    }


}
