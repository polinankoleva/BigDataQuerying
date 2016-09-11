package ex4;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper class that represents individual task and transforms input records
 * into a intermediate records. It goes over all rdf statements, parses them and
 * searches for specific predicate "foaf:knows". This mapper will operate over
 * the initial social graph network file. If a predicate "foaf:knows" is found ,
 * its subject is add as a key and the rdf statements a value out. This mapper
 * just prepares the data from the initial social graph file for the reduce-side
 * join. For example, for each rdf statement <user1 foaf:knows user2>, to the
 * reducer will be sent <user1 <user1 foaf:knows user2>>. In this case, we send
 * to a reducer all possible edges which a path like <fromUser foaf:knows
 * ....foaf:knows user1> can continue with.
 * 
 * 
 * @author Polina Koleva
 *
 */
public class SecondPhaseFirstFoafPathFinderMapper extends
		Mapper<Object, Text, CompositeKeyWritable, Text> {

	// the searched predicate
	private final String FRIENDS_RELATIONSHIP = "foaf:knows";

	// source index for the second data set
	private int sourceIndex = 1;
	// key processed to a reducer
	private CompositeKeyWritable compositeKey = new CompositeKeyWritable();
	// a variable stored the value processed to a reducer
	private Text statementText = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// parse the rdf statement into subject, predicate, object
		// the initial input file is processed(social graph network)
		StringTokenizer rdfStatement = new StringTokenizer(value.toString());
		String subject = rdfStatement.nextToken();
		String predicate = rdfStatement.nextToken();
		// if a predicate is "foaf:knows"
		// we search for all triples like <userN foaf:knows userM>
		if (predicate.equals(FRIENDS_RELATIONSHIP)) {
			// remove unneeded characters
			String valueFinal = value.toString().replace(".", "");
			statementText.set(valueFinal.toString());
			// set subject as a key - in this case we can continue building our
			// path
			// in each reducer paths like these from the second mapper will be
			// received <fromUser foaf:knows ..... foaf:knows userN>
			// from this mapper, we send all possible triples <userN foaf:knows
			// userM> with which these paths can continue
			compositeKey.setJoinKey(subject);
			compositeKey.setSourceIndex(sourceIndex);
			context.write(compositeKey, statementText);
		}
	}
}
