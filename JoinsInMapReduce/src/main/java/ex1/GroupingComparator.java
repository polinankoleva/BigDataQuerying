package ex1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A class that is used for controlling the grouping within the reducer. The
 * class {@link CustomPartitioner} can guarantee that the data with the same key
 * will end up in the same reducer. But it does not change the fact that a
 * reducer group its data by key(in our case <keyAtt, sourceIndex>). So we group
 * the data again only by the key attribute. Thus, we will see all the records
 * with the same key attribute within the same reducer group.
 * 
 * @author Polina Koleva
 *
 */
public class GroupingComparator extends WritableComparator {

	protected GroupingComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
		return key1.getKey().compareTo(key2.getKey());
	}
}
