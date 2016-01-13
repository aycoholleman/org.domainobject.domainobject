package org.domainobject.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.domainobject.core.Column;

/**
 * The default binder used when no other binder seems suitable. Uses
 * {@code ResultSet.getObject} for sending and
 * {@code PreparedStatement.getObject} for receiving.
 */
public class FallBackBinder implements Binder {

	public boolean isSQLNull(Object value, Column column)
	{
		return value == null;
	}


	public void send(Object value, Column column, PreparedStatement ps, int parameterIndex) throws Exception
	{
		ps.setObject(parameterIndex, value);
	}


	public Object receive(ResultSet rs, int columnIndex) throws Exception
	{
		return rs.getObject(columnIndex);
	}
}