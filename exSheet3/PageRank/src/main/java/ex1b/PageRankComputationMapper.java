package ex1b;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Second mapper. It distributes all portion of page rank using all outgoing
 * edges of a node. In this case, all other nodes to which the page rank have to
 * be distributed are friends of an user.
 * 
 * @author Polina Koleva
 *
 */
public class PageRankComputationMapper extends Mapper<Object, Text, Text, Text> {

	Text userIdKey = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// we have two options depending on which iteration of our page rank
		// algorithm we are
		// if we are on first iteration, all statements have a format <userId
		// (list of user's friends)>
		// if we are on the second or more iteration, all statements have a
		// format <userId pageRank (list of user's friends)>
		// initial page rank is just 1/#users(nodes)
		String[] parsedStatement = value.toString().split("\t");
		// the first part of a statement is always userId
		String userId = parsedStatement[0];
		float pageRank;
		String friends;
		String[] friendsList;
		// if the statement is <userId (list of user's friends)>
		if (parsedStatement.length == 2) {
			// uses initial page rank same for each user(node)
			pageRank = context.getConfiguration().getFloat(
					"initialPageRank", 0);
			// get the list of friends and convert it to easier to manipulate
			// structure
			friends = parsedStatement[1];
			friendsList = parseFriends(friends);
		} else {
			// the statement is <userId pageRank (list of user's friends)>
			pageRank = Float.parseFloat(parsedStatement[1]);
			// get the list of friends and convert it to easier to manipulate
			// structure
			friends = parsedStatement[2];
			friendsList = parseFriends(friends);
		}
		// compute how the current user's rank will be distributed over all of
		// its
		// outgoing edges (friends)
		// the final distributed part is received by dividing the page rank into
		// #outgoing edges
		float distributedPageRank = pageRank / friendsList.length;
		for (int i = 0; i < friendsList.length; i++) {
			// sends this final distributed part of user's page rank to
			// all its friends
			// set as a key friend's id - because finally in the reducer
			// each user (node) receives page ranks from all its incoming edges
			context.write(new Text(friendsList[i]),
					new Text(String.valueOf(distributedPageRank)));
		}
		// sends to each reducer by user id - a set with the user's friends
		// in this way not only the update page rank of each user(node) can be
		// computed
		// in the reducer, but also its list with friends will reach the reducer
		// and it has all information needed for the next iteration
		userIdKey.set(userId);
		context.write(userIdKey, new Text(friends));
	}

	// convert a string in format (user1, user3, user4,...) to
	// an array contains all users from the string
	// for example - [user1, user3, user4,....]
	public String[] parseFriends(String friendsList) {
		friendsList = friendsList.replace("(", "");
		friendsList = friendsList.replace(")", "");
		friendsList = friendsList.replaceAll(",", "");
		return friendsList.split(" ");
	}
}
