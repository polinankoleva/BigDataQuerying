package ex2a;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reducer class that reduces a set of intermediate values which share a key
 * to a smaller set of values. It receives an input formatted as <userM <user2
 * user4 .. .userR>>. The key which comes in is the user name of the user for
 * whom we want to retrieve all his/her friends. The value coming in is a list
 * with its friends. We collect them and combine them into a list. Finally, we
 * set as a key out the user name of the user and as a value out a list with all
 * its friends that we have already retrieved.
 * 
 * @author Polina Koleva
 *
 */
public class FriendsRetrievalReducer extends Reducer<Text, Text, Text, Text> {

	// variable where we store all friends of an user
	private ArrayList<String> friends = new ArrayList<>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// key will be the name of specified user during the starting point
		// iterate over all its friends
		Iterator<Text> friendsValues = values.iterator();
		while (friendsValues.hasNext()) {
			Text nextFriend = friendsValues.next();
			friends.add(nextFriend.toString());
		}
		// add key and value as a result. key - user name , value - array with
		// user's friends
		context.write(key, new Text(listToString(friends)));
	}

	/**
	 * Gets a list of strings and convert it to one string. The values in the
	 * final string will be separated by space. For example, if we have a list
	 * <"user1","user2", "user3"> as an input. The output will be the string
	 * "user1 user2 user3".
	 * 
	 * @param list
	 *            a list that will be formatted into a string
	 * @return a formatted list into string
	 */
	private String listToString(ArrayList<String> list) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < list.size(); i++) {
			sb.append(list.get(i)).append(" ");
		}
		return sb.toString();
	}
}
