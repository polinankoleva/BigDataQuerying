package ex5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reducer class that reduces a set of intermediate values which share a key
 * to a smaller set of values. This reduce is used for reduce-side join. The
 * mapper class {@link FirstPhaseFoafRetrievalMapper} processes data in a way
 * that it is ready for the join. The reducer is responsible to join the two
 * data sets and to produce the result list containing records <user <friends of
 * its friends>>. For example, if we have from the first data set <user1 user2>
 * and from the second data set <<user2 user14>, <user2 user3>, <user2 user4>>,
 * the reducer links "user1" to all friends of "user2". These friends will count
 * as indirect friends of "user1". The final result of these records will be
 * <user1 <user14 user3 user4>>. Unfortunately, this is not the final list with
 * indirect friends of "user1". On this stage of the job, the direct friends of
 * an user are not taken into account. For example, if the "user1" is direct
 * friend with "user3", the latter should be excluded from the user1' s final
 * list of indirect friends. For this task another reducer is used. Direct
 * friend of an user means that there is a rdf statement
 * "user foaf:knows userM", so both users are connect only with one edge.
 * 
 * Note: Indirect friend means that the user and its indirect friend are
 * connected with two edges "foaf:knows". For example, "user1 foaf:knows user2"
 * and "user2 foaf:knows user3", finally "user3" is indirect friend of "user1".
 * 
 * @author Polina Koleva
 *
 */
public class FirstPhaseFoafRetrievalReducer extends
		Reducer<CompositeKeyWritable, Text, Text, Text> {

	private Text friendsOfFriendsList = new Text();
	// a variable where the reducer key will be stored
	private Text reducerKey = new Text();

	@Override
	public void reduce(CompositeKeyWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		ArrayList<String> firstTableFriends = new ArrayList<String>();
		ArrayList<String> secondTableFriends = new ArrayList<String>();
		// we know that the entries for "the first join" data set (sourceIndex =
		// 0)
		// will come first
		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			Text next = it.next();
			// parse the records - they contain only subject and object
			StringTokenizer rdfStatement = new StringTokenizer(next.toString());
			String subject = rdfStatement.nextToken();
			String object = rdfStatement.nextToken();
			if (key.getSourceIndex() == 0) {
				// if we have "user1 user2" from the first data set this means
				// that
				// we should produce a list with indirect friends of user1
				// which are friends of user2
				firstTableFriends.add(subject);
			} else if (key.getSourceIndex() == 1) {
				// if we have "user2 user1" for the second data set - this means
				// that
				// for each direct friend of "user2" we should try to add
				// "user1"
				// as its indirect friends
				secondTableFriends.add(object);
			}
		}

		// for all already parsed direct friends - link each of them with
		// list of indirect friends
		for (int i = 0; i < firstTableFriends.size(); i++) {
			String friendUsername = firstTableFriends.get(i);
			ArrayList<String> friends = new ArrayList<String>(
					secondTableFriends);
			// because the friendship is bi-directional which means that if
			// user1 "is-a-friend-of" user2
			// user2 is also "a-friend-of" user2, it is logical that a list of
			// indirect friends for "user1"
			// will contain "user1", so we just remove it
			friends.remove(friendUsername);
			if (!friends.isEmpty()) {
				reducerKey.set(friendUsername);
				friendsOfFriendsList.set(listToString(friends));
				// finally we produce <user <list of indirect friends (friends
				// of their friends)>>
				context.write(reducerKey, friendsOfFriendsList);
			}
		}
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
		StringBuilder stringBuilder = new StringBuilder();
		for (int i = 0; i < list.size(); i++) {
			stringBuilder.append(list.get(i) + " ");
		}
		return stringBuilder.toString();
	}
}
