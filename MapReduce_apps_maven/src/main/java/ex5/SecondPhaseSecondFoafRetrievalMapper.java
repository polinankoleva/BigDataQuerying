package ex5;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper class that represents individual task and transforms input records
 * into a intermediate records. The input file used by this mapper is the file
 * produced from the previous job. This file has statements with format <user
 * <list with initial indirect friends>>. Because some of these indirect friends
 * can be directly connected with "user", we need to remove them. So the mapper
 * just goes over all statements and parse them. It sets as a key the user name
 * of the user and as a value the whole statement. This mapper just prepares the
 * data for reduce-side join. The idea is that from the first data set the
 * direct friends of an user will be retrieve and will be sent to a reducer with
 * a key user name of an user. From this mapper, the initial indirect friends
 * are sent to the same reducer again with key user name of the user. Finally,
 * the reducer has a role to combine them and receive the final list. The user
 * reducer is {@link SecondPhaseFoafRetrievalReducer}. The another used mapper
 * is {@link SecondPhaseFirstFoafRetrievalMapper}.
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
public class SecondPhaseSecondFoafRetrievalMapper extends
		Mapper<Object, Text, CompositeKeyWritable, Text> {

	// source index for the second data set
	private static int sourceIndex = 1;
	// the composite key used to send data to the reducer
	private CompositeKeyWritable compositeKey = new CompositeKeyWritable();
	// a variable which will store the value send to the reducer
	private Text statementText = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// parse the statement <user <list of indirect>>
		StringTokenizer rdfStatement = new StringTokenizer(value.toString());
		String username = rdfStatement.nextToken();
		// set "user" as a join key
		compositeKey.setJoinKey(username);
		// set that the data is coming from the second data set included in the
		// join
		compositeKey.setSourceIndex(sourceIndex);
		statementText.set(value.toString());
		context.write(compositeKey, statementText);
	}
}
