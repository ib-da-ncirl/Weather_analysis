package ie.ibuttimer.weather.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//


/**
 * 
 * CompositeKey: represents a pair of 
 * (String name, long timestamp).
 * 
 * 
 * We do a primary grouping pass on the name field to get all of the data of
 * one type together, and then our "secondary sort" during the shuffle phase
 * uses the timestamp long member to sort the timeseries points so that they
 * arrive at the reducer partitioned and in sorted order.
 * 
 * 
 * @author Mahmoud Parsian
 * Original code available at https://github.com/mahmoudparsian/data-algorithms-book/blob/master/src/main/java/org/dataalgorithms/chap06/secondarysort/CompositeKey.java
 *
 * 11/08/2020 Ian Buttimer
 * Modifications to use ICompositeKey
 */
public class CompositeKey implements ICompositeKey<CompositeKey, String, Long> {
    // natural key is (name)
    // composite key is a pair (name, timestamp)
	private String name;
	private long timestamp;

	public CompositeKey(String name, long timestamp) {
		set(name, timestamp);
	}
	
	public CompositeKey() {
	}

	@Override
	public String getMainKey() {
		return this.name;
	}

	@Override
	public Long getSubKey() {
		return this.timestamp;
	}

	@Override
	public void setMainKey(String mainKey) {
		this.name = mainKey;
	}

	@Override
	public void setSubKey(Long subKey) {
		this.timestamp = subKey;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.timestamp = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.name);
		out.writeLong(this.timestamp);
	}

	@Override
	public int compareTo(CompositeKey other) {
		// 11/08/2020 IB Refactored
		if (this.name == null) {
			System.out.println("Name --------");
		}

		int result = this.name.compareTo(other.name);
		if (result == 0) {
			if (this.timestamp != other.timestamp) {
				result = this.timestamp < other.timestamp ? -1 : 1;
			}
		} 
		return result;
	}

//	public static class CompositeKeyComparator extends WritableComparator {
//		public CompositeKeyComparator() {
//			super(CompositeKey.class);
//		}
//
//		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//			return compareBytes(b1, s1, l1, b2, s2, l2);
//		}
//	}
//
//	static { // register this comparator
//		WritableComparator.define(CompositeKey.class, new CompositeKeyComparator());
//	}

}
