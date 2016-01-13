package org.domainobject.orm.bind;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.domainobject.orm.core.Column;
import org.domainobject.orm.core.MetaDataConfigurator;

/**
 * <p>
 * A {@code Binder} is responsible for transferring data between a field and a
 * column. When a value is transferred from a {@link ResultSet} to an object
 * it's called <i>receiving</i>; when a value is transferred from an object to a
 * {@link PreparedStatement} it's called sending. A binder may do nothing but
 * copy data to or from the object, but it may also may also transform the data.
 * The binder to be used for each of an object's persistent fields is determined
 * when that object's metadata object is assembled (See
 * {@link MetaDataConfigurator#setBinder(Class, IBinder)}). You can influence
 * this process in various ways. By default binders are sourced from the
 * {@link DefaultBinderRepository}.
 * </p>
 * <p>
 * In general, binders are there for the following reasons:
 * <ol>
 * <li>
 * In cases where JDBC's type mapping system does not work. For example, you
 * cannot simply set a {@code StringBuilder} field using
 * {@code ResultSet#getString(int) ResultSet.getString}:
 * {@code StringBuilder sb = rs.getString(1)} will not work!</li>
 * <li>
 * To let you bind common types (like int) in alternative (and possibly
 * uncommon) ways.</li>
 * <li>
 * To let you persist uncommon types (like {@link java.io.File} or your own
 * classes) to the database in a way that makes sense for your application.</li>
 * <li>
 * To ensure consistency across RDBMSes and JDBC implementations when converting
 * between common types. For example, by using the {@link StringyBinder}, you
 * can be assured that when a String must be converted to a boolean, or vice
 * versa, "true", "on", "1", "y" and "yes" will count as true and the rest as
 * false, whichever JDBC implementation you use.</li>
 * </ol>
 * </p>
 */
public interface IBinder {

	/**
	 * Whether or not the specified value must be regarded as an SQL NULL value.
	 * Relevant when evaluating, for example, empty strings and 0 (zero).
	 * 
	 * @param value
	 *            The value to be evaluated.
	 * @param column
	 *            The column that the value came from or is going to.
	 * @return Whether or not the specified value must be regarded as an SQL
	 *         NULL value.
	 * 
	 */
	boolean isSQLNull(Object value, Column column);

	/**
	 * Transfer the specified value to the specified {@code PreparedStatement}.
	 * 
	 * @param value
	 *            The value to be transfered
	 * @param column
	 *            The database column that the value will end up in.
	 * @param ps
	 *            The {@code PreparedStatement}
	 * @param parameterIndex
	 *            the {@code PreparedStatement}' parameter index for the value.
	 * @throws Exception
	 *             Any exception thrown while transferring. Implementations
	 *             should generally just rethrow the exception.
	 */
	public void send(Object value, Column column, PreparedStatement ps, int parameterIndex)
			throws Exception;

	/**
	 * Transfer a value from the {@code ResultSet} to the caller of this method.
	 * 
	 * @param rs
	 *            The {@code ResultSet}
	 * @param columnIndex
	 *            The {@code ResultSet}'s column index for the value
	 * @return The value
	 * 
	 * @throws Exception
	 *             Any exception thrown while transferring. Implementations
	 *             should generally just rethrow the exception.
	 */
	public Object receive(ResultSet rs, int columnIndex) throws Exception;

}
