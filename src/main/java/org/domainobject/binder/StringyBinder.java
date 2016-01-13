package org.domainobject.binder;

import static java.sql.Types.BOOLEAN;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.domainobject.core.Column;
import org.domainobject.core.MetaData;

/**
 * The StringishBinder can be used for all "stringish" fields in a
 * {@link Object} object. In other words, it can be used for all fields that
 * should be bound using {@link ResultSet#getString(int)} and
 * {@link PreparedStatement#setString(int, String)}. For example char, char[],
 * and StringBuilder fields can all be bound using the StringishBinder.
 * {@code String}s should be bound using the purpose-built {@link StringBinder}
 * though.
 * 
 */
public class StringyBinder implements Binder {

	/**
	 * Extract the {@code String} from the stringy object. If it cannot simply
	 * be extracted using toString(), then override this method.
	 * 
	 * @param value
	 *            An instance of a stringy class
	 * 
	 * @return The {@code String} representation of the stringish object
	 */
	protected String getValueAsString(Object value)
	{
		return value.toString();
	}


	public boolean isFieldNull(Object instance, MetaData metadata, Field field) throws IllegalAccessException
	{
		Column column = metadata.getDataExchangeUnit(field).getColumn();
		String value = getValueAsString(field.get(instance));
		if (column.isNullable() && (value == null || value.equals(StringBinder.EMPTY_STRING))) {
			return true;
		}
		return false;
	}


	public void send(Object instance, MetaData metadata, Field field, PreparedStatement ps, int parameterIndex) throws SQLException, IllegalAccessException
	{
		Column column = metadata.getDataExchangeUnit(field).getColumn();
		String value = getValueAsString(field.get(instance));
		if (column.getDataType() == BOOLEAN) {
			StringBinder.setBooleanFromString(ps, parameterIndex, value);
		}
		else if (column.isNullable() && (value == null || value.equals(StringBinder.EMPTY_STRING))) {
			ps.setNull(parameterIndex, column.getDataType());
		}
		else if (!column.isNullable() && value == null) {
			ps.setString(parameterIndex, "");
		}
		else {
			ps.setString(parameterIndex, value);
		}
	}


	public void receive(Object instance, MetaData metadata, Field field, ResultSet rs, int columnIndex) throws SQLException, IllegalAccessException
	{
		Column column = metadata.getDataExchangeUnit(field).getColumn();
		if (column.getDataType() == BOOLEAN) {
			boolean value = rs.getBoolean(columnIndex);
			if (rs.wasNull() || !value) {
				field.set(instance, "false");
			}
			else {
				field.set(instance, "true");
			}
		}
		else {
			field.set(instance, rs.getString(columnIndex));
		}
	}


	/**
	 * This implementation regards a String value as SQL NULL if it is null, or
	 * if it is as empty String <i>and</a> the column it is coming from or going
	 * to is NOT NULL.
	 */
	public boolean isSQLNull(Object value, Column column)
	{
		if (column.isNullable() && (value == null || value.equals(StringBinder.EMPTY_STRING))) {
			return true;
		}
		return false;
	}


	public void send(Object value, Column column, PreparedStatement ps, int parameterIndex) throws Exception
	{
		if (value == null) {
			if (column.isNullable()) {
				ps.setNull(parameterIndex, column.getDataType());
			}
			else {
				ps.setString(parameterIndex, StringBinder.EMPTY_STRING);
			}
		}
		else {
			String val = getValueAsString(value);
			if (column.getDataType() == BOOLEAN) {
				StringBinder.setBooleanFromString(ps, parameterIndex, val);
			}
			else if (val.equals(StringBinder.EMPTY_STRING) && column.isNullable()) {
				ps.setNull(parameterIndex, column.getDataType());
			}
			else {
				ps.setString(parameterIndex, val);
			}
		}
	}


	public Object receive(ResultSet rs, int columnIndex) throws Exception
	{
		// TODO Auto-generated method stub
		return null;
	}

}
