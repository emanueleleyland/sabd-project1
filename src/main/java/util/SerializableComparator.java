package util;

import java.io.Serializable;
import java.util.Comparator;

/**
 * a serializable comparator
 *
 * @param <T> the comparator type
 */
public interface SerializableComparator<T> extends Comparator<T>, Serializable {

	/**
	 * trick to allow custom comparator in Spark "sortByKey"
	 *
	 * @param comparator the one to make it serializable
	 * @param <T> the comparator type
	 * @return a serializable comparator
	 */
	static <T> SerializableComparator<T> serialize(SerializableComparator<T> comparator) {
		return comparator;
	}

}
