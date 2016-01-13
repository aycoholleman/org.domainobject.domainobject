package org.domainobject.orm.map;

import java.lang.reflect.Field;

/**
 * Interface specifying methods for mapping class names to table names and field
 * names to column names.
 * 
 * @author Ayco Holleman
 *
 */
public interface IMappingAlgorithm {

	/**
	 * Maps the specified class to a table name.
	 * 
	 * @param cls
	 * @return
	 */
	String mapClassToEntityName(Class<?> cls);

	/**
	 * Maps the specified field to a column name. The class for which the
	 * mapping takes place is also passed to this method, because the field may
	 * not belong to that class (it could belong to a super class).
	 * 
	 * @param field
	 * @param cls
	 * @return
	 */
	String mapFieldToColumnName(Field field, Class<?> cls);

}
