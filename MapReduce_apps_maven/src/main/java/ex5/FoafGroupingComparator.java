package ex5;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A class that is used for controlling the grouping within the reducer. The
 * class {@link FoafPartitioner} can guarantee that the data with the same join
 * key will end up in the same reducer. But it does not change the fact that a
 * reducer group its data by key(in our case <joinAtt, sourceIndex>). So we
 * group the data again only by the join attribute. Thus, we will see all the
 * records with the same join attribute within the same reducer group
 * 
 * @author Polina Koleva
 *
 */
public class FoafGroupingComparator extends WritableComparator {

	protected FoafGroupingComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		// The grouping comparator is the joinKey (username)
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
		return key1.getJoinKey().compareTo(key2.getJoinKey());
	}
}
