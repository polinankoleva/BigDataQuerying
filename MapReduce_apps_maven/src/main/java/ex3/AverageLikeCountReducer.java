package ex3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reducer class that reduces a set of intermediate values which share a key
 * to a smaller set of values. It receives an input formatted as <defaultKey
 * <#likes per user1 #likes per user2 ...  #likes per userN>>. The key which
 * comes in is the same of each parsed from the mapper statements and it does
 * not carry any useful information. Its goal is just to send all #likes in one
 * reducer. The values coming in are the numbers of likes per user. We collect
 * them and sum them. Apart from that we count their number and use it as s
 * representation of how many users we have. Finally, we calculate the average
 * number of likes per user by dividing the total number of likes by #users.
 * 
 * @author Polina Koleva
 *
 */
public class AverageLikeCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	// variable where avergare likes per user will be stored
	private IntWritable averageLikesPerUser = new IntWritable();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int usersCount = 0;
		int likesCount = 0;
		// all parsed from the mapper statements will have the same fake key
		// -> reach the same reducer
		Iterator<IntWritable> it = values.iterator();
		while (it.hasNext()) {
			// for each parsed value - add its number of likes to the total
			// number
			// and increment the total #user by 1. Each #likes is connected with
			// one user.
			int next = it.next().get();
			likesCount += next;
			usersCount++;
		}
		averageLikesPerUser.set(likesCount / usersCount);
		context.write(new Text("Average count per user: "), averageLikesPerUser);
	}
}
