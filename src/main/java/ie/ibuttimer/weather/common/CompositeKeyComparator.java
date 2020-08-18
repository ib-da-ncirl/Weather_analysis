package ie.ibuttimer.weather.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * CompositeKeyComparator
 *
 * The purpose of this class is to enable comparison of two CompositeKey(s).
 *
 * @author Mahmoud Parsian
 * Original code available at https://github.com/mahmoudparsian/data-algorithms-book/blob/master/src/main/java/org/dataalgorithms/chap06/secondarysort/CompositeKeyComparator.java
 *
 * 18/08/2020 Ian Buttimer
 * Modified to remove redundant code
 */
public class CompositeKeyComparator extends WritableComparator {

    protected CompositeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey key1 = (CompositeKey) w1;
        CompositeKey key2 = (CompositeKey) w2;

        return key1.compareTo(key2);

//        int comparison = key1.getName().compareTo(key2.getName());
//        if (comparison == 0) {
//            // names are equal here
//            if (key1.getTimestamp() == key2.getTimestamp()) {
//                return 0;
//            } else if (key1.getTimestamp() < key2.getTimestamp()) {
//                return -1;
//            } else {
//                return 1;
//            }
//        }
//        else {
//            return comparison;
//        }
    }
}
