package org.domainobject.orm.bind;

import static java.sql.Types.BOOLEAN;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.domainobject.orm.core.Column;
import org.domainobject.util.StringUtil;

/**
 * The default binder for String values.
 */
public class StringBinder implements IBinder {

	static final String EMPTY_STRING = "";

	static void setBooleanFromString(PreparedStatement ps, int paramIndex, String value)
			throws SQLException
	{
		ps.setBoolean(paramIndex, StringUtil.isTrue(value, false));
	}

	/**
	 * Returns {@code true} if the specified value is {@code null}, or if it is
	 * an empty string <b>and</b> the target column is a NOT NULL column.
	 */
	public boolean isSQLNull(Object value, Column column)
	{
		if (column.isNullable() && (value == null || value.equals(EMPTY_STRING))) {
			return true;
		}
		return false;
	}

	public void send(Object value, Column column, PreparedStatement ps, int parameterIndex)
			throws SQLException
	{
		if (value == null) {
			if (column.isNullable()) {
				ps.setNull(parameterIndex, column.getDataType());
			}
			else {
				ps.setString(parameterIndex, EMPTY_STRING);
			}
		}
		else {
			String val = (String) value;
			if (column.getDataType() == BOOLEAN) {
				setBooleanFromString(ps, parameterIndex, val);
			}
			else if (val.equals(EMPTY_STRING) && column.isNullable()) {
				ps.setNull(parameterIndex, column.getDataType());
			}
			else {
				ps.setString(parameterIndex, val);
			}
		}
	}

	public String receive(ResultSet rs, int columnIndex) throws SQLException
	{
		int sqlType = rs.getMetaData().getColumnType(columnIndex);
		if (sqlType == BOOLEAN) {
			boolean value = rs.getBoolean(columnIndex);
			if (rs.wasNull() || !value) {
				return "false";
			}
			return "true";
		}
		return rs.getString(columnIndex);
	}

}
