package org.domainobject.orm.core;

/**
 * A Column object describes the columns in an {@link Entity}.
 */
public interface Column {

	/**
	 * Get the entity (table, view or nested query) containing the column.
	 * 
	 * @return The entity
	 */
	Entity getEntity();


	/**
	 * Get the name of the column.
	 * 
	 * @return The name of the column
	 */
	String getName();


	/**
	 * Get the datatype of the column.
	 * 
	 * @return The datatype of the column
	 */
	int getDataType();


	/**
	 * Get the maximum width of the column.
	 * 
	 * @return The maximum width of the column
	 */
	int getColumnSize();


	/**
	 * Get the position of the column within the table, view or entity.
	 * 
	 * @return The position of the column
	 */
	int getOrdinalPosition();


	/**
	 * Whether or not the column is NOT NULL.
	 * 
	 * @return Whether or not the column is NOT NULL
	 */
	boolean isNullable();


	boolean isAutoIncrement();

}