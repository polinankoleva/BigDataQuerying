package ex1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * First Mapper. It retrieves all posts and all users whose created something.
 * It sends all retrieved information to one reducer.
 * 
 * @author Polina Koleva
 *
 */
public class PostRetrievalMapper extends
		Mapper<Object, Text, CompositeKeyWritable, Text> {

	// the searched predicates
	private final String PREDICATE_CREATOR = "sioc:creator_of";
	private final String PREDICATE_TYPE = "a";
	private final String POST_TYPE = "sib:Post";
	// a variable where composite key will be stored
	private CompositeKeyWritable compositeKey = new CompositeKeyWritable();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Break statement into subject, predicate and object
		String[] rdfParts = value.toString().split(
				" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		String predicate = rdfParts[1];
		String object = rdfParts[2];
		// if we have a statement like <userId sioc:creator_of itemId>
		if (predicate.equals(PREDICATE_CREATOR)) {
			// send the statement to our one/default reducer
			// set source index 1 because we want to receive all such triples
			// after triples like <itemId a sib:Post>
			compositeKey.setSourceIndex(1);
			context.write(compositeKey, value);
			// if the statement likes <itemId s sib:Post>
			// we are searching only for items of type post
		} else if (predicate.equals(PREDICATE_TYPE) && object.equals(POST_TYPE)) {
			// we set source index to 0 because we want to receive all posts
			// in the reducer first
			compositeKey.setSourceIndex(0);
			context.write(compositeKey, value);
		}
	}
}
