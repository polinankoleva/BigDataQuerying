package ex5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Usually, keys are formed by only one parameter. In this case, we create out
 * custom composite key which contains two parameters needed for our
 * mappers/reducers: 1) join key attribute 2) source index. Because we use the
 * composite key when a join between two data sets is done, apart from the join
 * attribute by which the join is performed, we use source index attribute which
 * is used as an indication from which data sets data is coming. It can be also
 * use for specific order. For example, if we join two data sets, one with
 * source index 0 , one with source index 1, if we specify an order, the data
 * from data set with source index 0 can reach the reducer first. Therefore, we
 * can guarantee the ordering of incoming data in a reducer.
 * 
 * @author Polina Koleva
 *
 */
public class CompositeKeyWritable implements Writable,
		WritableComparable<CompositeKeyWritable> {

	// we will use as a join key user name
	// For example, we have "user1 foaf:knows user2". The value of the join
	// attribute here will be
	// "user2" as we want to search for all friends of the friends of "user1"
	private String joinKey;
	// depends on which table we want to receive first
	// so it our case we will use just 0 or 1. Therefore, the record (because of
	// the one to many relation)
	// from the table marked with 0 will reach the reducer first. Additionally,
	// the records from the table
	// marked with 1 will reach the reducer after.
	private int sourceIndex;

	public CompositeKeyWritable() {
	}

	public CompositeKeyWritable(String joinKey, int sourceIndex) {
		this.joinKey = joinKey;
		this.sourceIndex = sourceIndex;
	}

	@Override
	public String toString() {

		return (new StringBuilder().append(joinKey).append("\t")
				.append(sourceIndex)).toString();
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		joinKey = WritableUtils.readString(dataInput);
		sourceIndex = WritableUtils.readVInt(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, joinKey);
		WritableUtils.writeVInt(dataOutput, sourceIndex);
	}

	@Override
	public int compareTo(CompositeKeyWritable keyPair) {

		int result = joinKey.compareTo(keyPair.joinKey);
		if (result == 0) {
			result = Double.compare(sourceIndex, keyPair.sourceIndex);
		}
		return result;
	}

	public String getJoinKey() {
		return joinKey;
	}

	public void setJoinKey(String joinKey) {
		this.joinKey = joinKey;
	}

	public int getSourceIndex() {
		return sourceIndex;
	}

	public void setSourceIndex(int sourceIndex) {
		this.sourceIndex = sourceIndex;
	}
}
