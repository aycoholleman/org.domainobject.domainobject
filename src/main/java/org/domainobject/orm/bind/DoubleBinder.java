package org.domainobject.orm.bind;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.domainobject.orm.core.Column;
import org.domainobject.orm.core.MetaData;

/**
 * The default binder for double and Double values.
 */
public final class DoubleBinder extends NumberBinder {

	public void send(Object instance, MetaData metadata, Field field, PreparedStatement ps, int parameterIndex) throws SQLException, IllegalAccessException
	{
		ps.setDouble(parameterIndex, field.getDouble(instance));
	}


	public void send(Object val, Column column, PreparedStatement ps, int parameterIndex) throws SQLException
	{
		ps.setDouble(parameterIndex, ((Double) val).doubleValue());
	}


	public void receive(Object instance, MetaData metadata, Field field, ResultSet rs, int columnIndex) throws SQLException, IllegalAccessException
	{
		int sqlType = rs.getMetaData().getColumnType(columnIndex);
		switch (sqlType) {
			case Types.DATE:
				field.setDouble(instance, (double) rs.getDate(columnIndex).getTime());
			default:
				field.setDouble(instance, rs.getDouble(columnIndex));
		}
	}


	public Double receive(ResultSet rs, int columnIndex) throws Exception
	{
		int sqlType = rs.getMetaData().getColumnType(columnIndex);
		switch (sqlType) {
			case Types.DATE:
				return (double) rs.getDate(columnIndex).getTime();
			default:
				return rs.getDouble(columnIndex);
		}
	}

}
