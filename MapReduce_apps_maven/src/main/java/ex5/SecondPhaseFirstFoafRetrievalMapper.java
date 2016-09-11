package ex5;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper class that represents individual task and transforms input records
 * into a intermediate records. It goes over all rdf statements, parse them and
 * search for specific predicate "foaf:knows". This mapper will operate over the
 * initial social graph network file. If a predicate "foaf:knows" is found , its
 * subject is add as a key and the whole rdf statement without the predicate as
 * a value out. This mapper just prepares the data from the initial social graph
 * file for the reduce-side join. For example, for each rdf statement <user1
 * foaf:knows user2>, to the reducer will be sent <user1 <user1 user2>>. In this
 * way we send all direct friedns of an user to the same reducer.
 * 
 * Note: Direct friend of an user means that there is a rdf statement
 * "user foaf:knows userM", so both users are connect only with one edge.
 * Indirect friend means that the user and its indirect friend are connected
 * with two edges "foaf:knows". For example, "user1 foaf:knows user2" and
 * "user2 foaf:knows user3", finally "user3" is indirect friend of "user1".
 * 
 * @author Polina Koleva
 *
 */
public class SecondPhaseFirstFoafRetrievalMapper extends
		Mapper<Object, Text, CompositeKeyWritable, Text> {

	// the searched predicate
	private static final String FRIENDS_RELATIONSHIP = "foaf:knows";

	// source index for the first data set
	private static int sourceIndex = 0;
	// key processed to a reducer
	private CompositeKeyWritable compositeKey = new CompositeKeyWritable();
	// a variable stored the value processed to a reducer
	private Text statementText = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// parse the rdf statement as subject predicate object
		StringTokenizer rdfStatement = new StringTokenizer(value.toString());
		String subject = rdfStatement.nextToken();
		String predicate = rdfStatement.nextToken();
		String object = rdfStatement.nextToken();
		if (predicate.equals(FRIENDS_RELATIONSHIP)) {
			// remove the predicate because it is not longer needed
			statementText.set(subject + " " + object);
			// set as a key the subject. For example, if we have rdf statement
			// <user1 foaf:knows user2>
			// user1 will be set as a key
			compositeKey.setJoinKey(subject);
			// we assume that the data parsed in this mapper comes from the
			// first join data set or source index = 0
			compositeKey.setSourceIndex(sourceIndex);
			context.write(compositeKey, statementText);
		}
	}
}
