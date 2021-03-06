package org.domainobject.orm.core;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * <p>
 * An {@code Entity} represents the database entity that a {@link Persistable}
 * class maps to. Each concrete {@code Persistable} class maps to a single
 * entity, and each instance of that class maps to a single row within that
 * entity. An Entity can be a table, a view, or a "live" SQL query. The latter
 * type of entity is represented by the {@link NestedQueryEntity} class, while
 * both tables and views are represented by the {@link TableEntity} class. With
 * a "live" SQL query as your physical datasource, all persistency operations
 * are done "on top of" a nested SELECT query (
 * {@code SELECT ... FROM (SELECT ...) WHERE ...}). Consequently, if the
 * {@code Entity} is a SQL query, only SELECT-type operations will work (you
 * cannot INSERT into a SQL query).
 * </p>
 * 
 * 
 * <h3>Provided implementations</h3>
 * <p>
 * The domainobject package itself provides three implementations. Unless you
 * specify otherwise (see {@link Context#setEntityClass(Class)
 * Context.setEntityClass()}) it is going to use the {@link TableEntity} class.
 * You can also choose to use the {@link ForeignKeyAwareTableEntity} , which is
 * useful if you have created database constraints for all or nearly all foreign
 * key relationships between tables. If the Entity is a table or view, either
 * the one or the other should do. Only if your DBMS or JDBC Driver is
 * unreliable with respect to exposing data dictionary data should you consider
 * writing your own implementation. If the Entity is a query, you can extend the
 * {@link NestedQueryEntity} class.
 * </p>
 * 
 * <h3>Implementing your own Entity class</h3>
 * <p>
 * When you write your own implementation of Entity, your class <b>must</b>
 * provide a constructor that takes two arguments in the following order: a
 * {@link java.lang.String} and a {@link java.sql.Connection}. This constructor
 * will be invoked by the {@link org.domainobject.orm.core.Context} when it is
 * requested to assemble a {@link org.domainobject.PersistableMetaData metadata
 * object}. The String argument passed to the constructor will be the simple
 * name of the Entity. The Connection argument will be the JDBC Connection that
 * lets you access c.q. execute the table, view or query.
 * </p>
 * 
 * <p>
 * Also, in order for your implementation to replace the default implementation,
 * you must call {@link org.domainobject.orm.core.Context#setEntityClass(Class)
 * Context.setTableMetaDataClass()} before you call
 * {@link org.domainobject.orm.core.Context#createMetaData(Class)
 * Context.createMetaData()}.
 * </p>
 */
public interface Entity {

	public static enum Type
	{
		TABLE, VIEW, NESTED_QUERY
	};


	/**
	 * Get the name that identifies the Entity. If the Entity is a table or a
	 * view, it will be the simple name of the table c.q. view (without the
	 * schema prefix, and without RDBMS-specific quotes). If the Entity is a
	 * query, it will be a name that supposedly allows you to identify the query
	 * to be used. An {@code Entity}'s name is inferred from the name of a
	 * persistent class using a mapping algorithm implemented by a
	 * {@link _Mapper}.
	 * 
	 * @return The name of the Entity.
	 */
	String getName();


	/**
	 * Get the expression to appear in the FROM clause. With tables and views
	 * this will be their fully qualified name; with dynamic entities it will be
	 * the query to be nested within the FROM clause.
	 * 
	 * @return The expression to appear in the FROM clause.
	 */
	String getFrom();


	/**
	 * Get the JDBC Connection through which the table is accessed.
	 * 
	 * @return The JDBC Connection
	 */
	Connection getConnection();


	/**
	 * Get entity's columns.
	 * 
	 * @return The columns
	 */
	Column[] getColumns();


	/**
	 * Get the columns constituting the primary key of the entity. If the
	 * {@code Entity} has no primary key, an empty {@link Column} array must be
	 * returned; if the primary key cannot be determined, null must be returned.
	 * 
	 * @return The columns constituting the primary key or null if the primary
	 *         key canot be determined.
	 */
	Column[] getPrimaryKeyColumns();


	/**
	 * Set the primary key manually. This may be necessary if the database does
	 * not provide this information (e.g. for views or nested queries), even
	 * though the entity does have a <i>de facto</i> primary key.
	 * 
	 * @param columns
	 */
	void setPrimaryKeyColumns(Column[] columns);


	/**
	 * <p>
	 * Get the columns whose value is generated by some sort of key-generating
	 * mechanism. Only applicable when dealing with INSERT queries. No
	 * assumption is made about how the key-generating mechanism is used or
	 * implemented. It could an Oracle-ish sequence or a MySQL-ish
	 * auto-increment mechanism. In all but the strangest circumstances there is
	 * just one column with a generated key, and that column is the primary key
	 * of a table with a non-compound, numerical primary key (i.e. a primary key
	 * consisting of just one numerical column). Use
	 * {@link #setGeneratedKeyColumns(Column[])} to deal with uncommon
	 * situations.
	 * </p>
	 * <p>
	 * Under the hood, the columns returned by this method are passed as the
	 * second argument to {@link Connection#prepareStatement(String, String[])},
	 * and they are used to retrieve the generated values with
	 * {@link PreparedStatement#getGeneratedKeys()}.
	 * <p>
	 * 
	 * @return The columns whose value is generated by some sort of
	 *         key-generating mechanism
	 */
	Column[] getGeneratedKeyColumns();


	/**
	 * Set the columns whose value is generated by some sort of key-generating
	 * mechanism.
	 * 
	 * @see #getGeneratedKeyColumns()
	 * 
	 * @param columns
	 *            The columns whose value is generated by some sort of
	 *            key-generating mechanism
	 */
	void setGeneratedKeyColumns(Column[] columns);


	/**
	 * Get the columns constituting the foreign key to another entity
	 * 
	 * @param parent
	 *            The {@code Entity} that is the relational parent of this
	 *            {@code Entity}
	 * @return The foreign columns
	 */
	Column[] getForeignKeyColumns(Entity parent);


	/**
	 * Set the foreign key to some other {@code Entity} manually. This may be
	 * necessary if, for example, no foreign key constraints have been defined,
	 * even though there are <i>de facto</i> foreign key relationships in the
	 * data model.
	 * 
	 * @param parent
	 */
	void setForeignKeyColumns(Entity parent, Column[] columns);

}
