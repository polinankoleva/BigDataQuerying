package ex1a;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * One reducer per each user. It formats all friends of an user in a list as
 * (user1, user2, ....). The output is just an user mapped to a list of its
 * friends.
 * 
 * @author Polina Koleva
 *
 */
public class FriendsOfUserRetrievalReducer extends
		Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// just format all friends of an user like the pattern (user2, user3, user4,...)
		String outputValue = formatOutputValue(values);
		// set as a key userId
		context.write(key, new Text(outputValue));
	}

	// format data as the output pattern - (user1, user2, user3, ..)
	public String formatOutputValue(Iterable<Text> values) {
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		Iterator<Text> iterator = values.iterator();
		while (iterator.hasNext()) {
			Text next = iterator.next();
			if (!iterator.hasNext()) {
				sb.append(next.toString());
			} else {
				sb.append(next.toString() + ", ");
			}
		}
		sb.append(")");
		return sb.toString();
	}
}
