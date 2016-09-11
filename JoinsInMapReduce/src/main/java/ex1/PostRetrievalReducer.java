package ex1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * First reducer. It processes all posts and all users created something. It
 * checks if an user creates a post and for each created post from an user,
 * outputs <userId 1>
 * 
 * @author Polina Koleva
 *
 */
public class PostRetrievalReducer extends
		Reducer<CompositeKeyWritable, Text, Text, IntWritable> {

	private final String PREDICATE_CREATOR = "sioc:creator_of";
	private final String PREDICATE_TYPE = "a";
	// variable where we'll store the key and value out
	private Text userIdKey = new Text();
	private IntWritable one = new IntWritable(1);

	@Override
	public void reduce(CompositeKeyWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		ArrayList<String> items = new ArrayList<String>();
		// we know that the triples indicated with source index 0
		// will come first
		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			Text next = it.next();
			// Break statement into subject, predicate and object
			String[] rdfParts = next.toString().split(
					" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			String subject = rdfParts[0];
			String predicate = rdfParts[1];
			String object = rdfParts[2];
			// process all triples <itemId a sib:Post> and
			// store all ids into a list
			if (key.getSourceIndex() == 0 && predicate.equals(PREDICATE_TYPE)) {
				items.add(subject);
			} else if (key.getSourceIndex() == 1
					&& predicate.equals(PREDICATE_CREATOR)) {
				// receive triple of type <userId sioc:creator itemId>
				// if the itemId is contained into out list with posts
				// then add <userId 1> which will indicate that the user creates
				// one post
				if (items.contains(object)) {
					userIdKey.set(subject);
					context.write(userIdKey, one);
				}
			}
		}

	}

}
