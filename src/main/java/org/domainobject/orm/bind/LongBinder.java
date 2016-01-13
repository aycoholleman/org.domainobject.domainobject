package org.domainobject.orm.bind;


import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.domainobject.orm.core.Column;
import org.domainobject.orm.core.MetaData;

/**
 * The default binder for long and Long values.
 */
public final class LongBinder extends NumberBinder
{

	public void receive(Object instance, MetaData metadata, Field field, ResultSet rs, int columnIndex) throws SQLException, IllegalAccessException
	{
		int sqlType = rs.getMetaData().getColumnType(columnIndex);
		switch (sqlType) {
			case Types.DATE:
				field.setLong(instance, rs.getDate(columnIndex).getTime());
			default:
				field.setLong(instance, rs.getLong(columnIndex));
		}
	}

	public void send(Object instance, MetaData metadata, Field field, PreparedStatement ps, int parameterIndex) throws SQLException, IllegalAccessException
	{
		ps.setLong(parameterIndex, field.getLong(instance));
	}

	public void send(Object value, Column column, PreparedStatement ps, int parameterIndex) throws Exception
	{
		int sqlType = column.getDataType();
		switch (sqlType) {
			case Types.DATE:
				ps.setDate(parameterIndex, new java.sql.Date((Long) value));
				break;
			default:
				ps.setLong(parameterIndex, (Long) value);
		}
	}

	public Long receive(ResultSet rs, int columnIndex) throws Exception
	{
		int sqlType = rs.getMetaData().getColumnType(columnIndex);
		switch (sqlType) {
			case Types.DATE:
				return rs.getDate(columnIndex).getTime();
		}
		return rs.getLong(columnIndex);
	}

}
