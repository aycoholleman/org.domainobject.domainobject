package org.domainobject.orm.util;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.domainobject.orm.core.Column;
import org.domainobject.orm.core.DataExchangeUnit;
import org.domainobject.orm.core.MetaData;
import org.domainobject.orm.exception.AlienMetaDataException;
import org.domainobject.orm.exception.DomainObjectException;
import org.domainobject.orm.exception.DomainObjectSQLException;
import org.domainobject.orm.exception.ReflectionException;
import org.domainobject.orm.exception.UnmappedColumnException;
import org.domainobject.orm.exception.UnmappedFieldException;
import org.domainobject.util.ArrayUtil;
import org.domainobject.util.convert.Stringifier;

/**
 * The Util class is a container for static methods used by various other
 * classes in the domainobject package. It can also be useful when implementing
 * one of the many interfaces in the domainobject package.
 */
public class Util {

	public static String createSelectClause(final MetaData metadata)
	{
		return createSelectClause(metadata, metadata.getAttributeFieldNames());
	}

	public static String createSelectClause(final MetaData metadata, String[] fieldNames)
	{
		return ArrayUtil.implode(fieldNames, new Stringifier() {

			public String execute(Object object, Object... options)
			{
				return getQuotedColumnForField(metadata, (String) object);
			}
		});
	}

	public static String createSelectClauseForColumns(final MetaData metadata, String[] columns)
	{
		return ArrayUtil.implode(columns, new Stringifier() {

			public String execute(Object object, Object... options)
			{
				return quote(metadata, (String) object);
			}
		});
	}

	public static String getQuotedColumnForField(MetaData metadata, String field)
	{
		DataExchangeUnit deu = metadata.getDataExchangeUnit(field);
		if (deu == null) {
			throw new UnmappedFieldException(metadata.getForClass(), field);
		}
		return quote(metadata, deu.getColumn().getName());
	}

	public static String quote(MetaData metadata, String identifier)
	{
		try {
			final String quote = metadata.getEntity().getConnection().getMetaData()
					.getIdentifierQuoteString();
			return new StringBuilder(identifier.length() + 2).append(quote).append(identifier)
					.append(quote).toString();
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
	}

	public static String quote(Connection connection, String identifier)
	{
		try {
			final String quote = connection.getMetaData().getIdentifierQuoteString();
			return new StringBuilder(identifier.length() + 2).append(quote).append(identifier)
					.append(quote).toString();
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
	}

	public static Object[] read(Object object, MetaData metadata, String... fields)
	{
		if (object.getClass() != metadata.getForClass()) {
			throw new AlienMetaDataException(metadata, object.getClass());
		}
		Object[] result = new Object[fields.length];
		for (int i = 0; i < fields.length; ++i) {
			String field = fields[i];
			DataExchangeUnit deu = metadata.getDataExchangeUnit(field);
			if (deu == null) {
				throw new UnmappedFieldException(metadata.getForClass(), field);
			}
			try {
				result[i] = deu.getField().get(object);
			}
			catch (IllegalArgumentException e) {
				throw new ReflectionException(e);
			}
			catch (IllegalAccessException e) {
				throw new ReflectionException(e);
			}
		}
		return result;
	}

	/**
	 * Get the primary key of a Persistable object.
	 * 
	 * @param persistable
	 *            The Persistable object
	 * 
	 * @return An array of values constituting the primary key.
	 * 
	 */
	public static Object[] getId(Object persistable, MetaData metadata)
	{
		Field[] primaryKeyFields = metadata.getPrimaryKeyFields();
		if (primaryKeyFields.length == 0) {
			throw new DomainObjectException("Table " + metadata.getEntity().getName()
					+ " has no primary key");
		}
		Object[] values = new Object[primaryKeyFields.length];
		try {
			for (int i = 0; i < primaryKeyFields.length; ++i) {
				values[i] = primaryKeyFields[i].get(persistable);
			}
		}
		catch (IllegalAccessException e) {
			throw new DomainObjectException(e);
		}
		return values;
	}

	/**
	 * Set the primary key of a persistent object.
	 * 
	 * @param object
	 *            The persistent object
	 * @param metadata
	 *            The metadata object for the persistent object
	 * @param value
	 *            the primary key
	 */
	public static void setId(Object object, MetaData metadata, Object... value)
	{
		Field[] primaryKeyFields = metadata.getPrimaryKeyFields();
		if (primaryKeyFields.length == 0) {
			throw new DomainObjectException("Primary key does not exist or has not been set for "
					+ object.getClass());
		}
		if (value.length != primaryKeyFields.length) {
			throw new DomainObjectException(
					"Number of values passed to setId() must match number of fields in primary key");
		}
		try {
			for (int i = 0; i < value.length; ++i) {
				primaryKeyFields[i].set(object, value[i]);
			}
		}
		catch (Throwable t) {
			DomainObjectException.rethrow(t);
		}
	}

	public static void populate(Object object, MetaData metadata, ResultSet rs)
	{
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int numCols = rsmd.getColumnCount();
			for (int i = 1; i <= numCols; ++i) {
				DataExchangeUnit deu = metadata
						.getDataExchangeUnitForColumn(rsmd.getColumnLabel(i));
				if (deu != null) {
					deu.receive(object, rs, i);
				}
			}
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
	}

	public static Field[] getFields(MetaData metadata, String... fieldNames)
	{
		Field[] fields = new Field[fieldNames.length];
		for (int i = 0; i < fieldNames.length; ++i) {
			if (null == (fields[i] = metadata.getDataExchangeUnit(fieldNames[i]).getField())) {
				throw new UnmappedFieldException(metadata.getForClass(), fieldNames[i]);
			}
		}
		return fields;
	}

	public static String[] getFieldNames(MetaData metadata, Column... columns)
	{
		String[] fields = new String[columns.length];
		for (int i = 0; i < columns.length; ++i) {
			if (null == (fields[i] = metadata.getDataExchangeUnitForColumn(columns[i].getName())
					.getField().getName())) {
				throw new UnmappedColumnException(metadata.getForClass(), columns[i].getName());
			}
		}
		return fields;
	}

	public static String[] getFieldNames(MetaData metadata, String... columnNames)
	{
		String[] fields = new String[columnNames.length];
		for (int i = 0; i < columnNames.length; ++i) {
			if (null == (fields[i] = metadata.getDataExchangeUnitForColumn(columnNames[i])
					.getField().getName())) {
				throw new UnmappedColumnException(metadata.getForClass(), columnNames[i]);
			}
		}
		return fields;
	}

	public static boolean isPersistentField(MetaData metadata, String field)
	{
		return metadata.getDataExchangeUnit(field) != null;
	}

	public static String[] getColumnNames(MetaData metadata, String... fieldNames)
			throws UnmappedFieldException
	{
		String[] columns = new String[fieldNames.length];
		for (int i = 0; i < fieldNames.length; ++i) {
			if (null == (columns[i] = metadata.getDataExchangeUnit(fieldNames[i]).getColumn()
					.getName())) {
				throw new UnmappedFieldException(metadata.getForClass(), fieldNames[i]);
			}
		}
		return columns;
	}

	public static String[] getColumnNames(MetaData metadata, Column... columns)
	{
		String[] colNames = new String[columns.length];
		for (int i = 0; i < columns.length; ++i) {
			colNames[i] = columns[i].getName();
		}
		return colNames;
	}

	public static String[] getColumnNames(MetaData metadata, Field... fields)
			throws UnmappedFieldException
	{
		String[] columns = new String[fields.length];
		for (int i = 0; i < fields.length; ++i) {
			columns[i] = metadata.getDataExchangeUnit(fields[i]).getColumn().getName();
		}
		return columns;
	}

	private static final int hashMultiplier = 31;

	public static int getQueryId(Object sqlAdapter, String sqlAdapterMethod, Object... methodArgs)
	{
		int hash = sqlAdapter == null ? 0 : sqlAdapter.hashCode();
		hash = (hash * hashMultiplier)
				+ (sqlAdapterMethod == null ? 0 : sqlAdapterMethod.hashCode());
		hash = (hash * hashMultiplier) + hashArguments(methodArgs);
		return hash;
	}

	private static int hashArguments(Object... arguments)
	{
		// Make the number of arguments part of the hash.
		int hash = arguments.length;

		// Make the type of the arguments part of the hash.
		for (Object value : arguments) {
			hash *= hashMultiplier;
			if (value != null) {
				hash += value.getClass().hashCode();
			}
		}

		// Make the value of the arguments part of the hash.
		for (Object arg : arguments) {
			hash *= hashMultiplier;
			if (arg != null) {
				hash += arg.getClass().isArray() ? hashArguments((Object[]) arg) : arg.hashCode();
			}
		}

		return hash;

	}

}
