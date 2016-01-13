package org.domainobject.orm.bind;

import org.domainobject.orm.core.Column;

/**
 * Abstract base class for the number binders in this package. All number
 * binders share the same NULL semantics, which is implemented in this base
 * class. A number value is regarded as SQL NULL if
 * <ul>
 * <li>it is a primitive number wrapper and it is null</li>
 * <li>it is 0 (zero) <i>and</a> the column it is coming from or going to is NOT
 * NULL</i>
 * </ul>
 * Otherwise the value is regarded as not SQL NULL.
 */
public abstract class NumberBinder implements IBinder {

	public boolean isSQLNull(Object value, Column column)
	{
		if (value == null)
			return true;
		if (((Number) value).doubleValue() == 0 && column.isNullable())
			return true;
		return false;
	}

}
