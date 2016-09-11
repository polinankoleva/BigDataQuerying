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
 * mapper classes {@link SecondPhaseFirstFoafRetrievalMapper} and
 * {@link SecondPhaseSecondFoafRetrievalMapper} process data in a way that it is
 * ready for the join. The reducer is responsible to join the two data sets and
 * to produce the result list containing records <user <friends of its
 * friends>>. For example, if we have from the first data set <user1 user2> ,
 * this will mean that "user2" is direct friend of "user1" and "user1" will be
 * used as a key. From the second data set, we receive <user1 <all friends of
 * friends of an user>>. The key will be again "user1", so both piece of
 * information will reach the same reducer - once receiving all direct friends
 * and once receiving "all friends of friends of an user". Unfortunately, the
 * latter is not the final list with indirect friends of "user1" because with
 * some of them "user1" can be a direct friend as well. So this reducer just
 * removes all direct friends from the list with
 * "<all friends of friends of an user>". For example, if the "user1" is direct
 * friend with "user3" and "<all friends of friends of an user>" contains
 * "user3" the latter should be excluded from the user1' s final list of
 * indirect friends.
 * 
 * Note: Indirect friend means that the user and its indirect friend are
 * connected with two edges "foaf:knows". For example, "user1 foaf:knows user2"
 * and "user2 foaf:knows user3", finally "user3" is indirect friend of "user1".
 * 
 * @author Polina Koleva
 *
 */
public class SecondPhaseFoafRetrievalReducer extends
		Reducer<CompositeKeyWritable, Text, Text, Text> {

	// a variable that will store final list of friends of the friends of an
	// user
	private Text friendsOfFriends = new Text();
	// use user name for a key and store it in this variable
	private Text reducerKey = new Text();

	@Override
	public void reduce(CompositeKeyWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		ArrayList<String> directFriends = new ArrayList<String>();
		ArrayList<String> iniallyIndirectFriends = new ArrayList<String>();
		// we know that the entries for "the first join table" (sourceIndex = 0)
		// will come first
		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			Text next = it.next();
			StringTokenizer rdfStatement = new StringTokenizer(next.toString());
			String subject = rdfStatement.nextToken();
			// if the data is coming from the first data set - add a friend as a
			// direct one
			if (key.getSourceIndex() == 0) {
				String directFriend = rdfStatement.nextToken();
				directFriends.add(directFriend);
				// if the data is coming from the second set where the format is
				// <user <list of initially indirect friends>>
				// add these to the indirect friends
			} else if (key.getSourceIndex() == 1) {
				while (rdfStatement.hasMoreElements()) {
					String undirectFriend = rdfStatement.nextToken();
					iniallyIndirectFriends.add(undirectFriend);
				}
			}
		}
		reducerKey.set(key.getJoinKey());
		// remove the direct friends from the initially determined as indirect
		// and return the final list
		ArrayList<String> finalUndirectFriends = getFinalUndirectFriends(
				directFriends, iniallyIndirectFriends);
		friendsOfFriends.set(listToString(finalUndirectFriends));
		context.write(reducerKey, friendsOfFriends);
	}

	/**
	 * Remove the direct friends of an user from the list with its initial
	 * indirect friends. Direct friend of an user means that there is a rdf
	 * statement "user foaf:knows userM", so both users are connect only with
	 * one edge. Indirect friend means that the user and its indirect friend are
	 * connected with two edges "foaf:knows". For example,
	 * "user1 foaf:knows user2" and "user2 foaf:knows user3", finally "user3" is
	 * indirect friend of "user1". The initialIndirectFriends contains all
	 * friends of the friends of an user and because the user itself can be a
	 * friend with some of them, they should be removed.
	 * 
	 * 
	 * @param directFriends
	 *            list with direct friends of an user
	 * @param indirectFriends
	 *            list with initially determined indirect friends of an user
	 * @return the final list with indirect friends of an user
	 */
	private ArrayList<String> getFinalUndirectFriends(
			ArrayList<String> directFriends,
			ArrayList<String> initalIndirectFriends) {
		ArrayList<String> finalindirectFriends = new ArrayList<String>();
		for (int i = 0; i < initalIndirectFriends.size(); i++) {
			String indirectFriend = initalIndirectFriends.get(i);
			if (!directFriends.contains(indirectFriend)
					&& !finalindirectFriends.contains(indirectFriend)) {
				finalindirectFriends.add(indirectFriend);
			}
		}
		return finalindirectFriends;
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
