package org.domainobject.orm.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.domainobject.orm.core.Column;

public final class DateBinder implements Binder {

	public boolean isSQLNull(Object value, Column column)
	{
		// TODO Auto-generated method stub
		return false;
	}

	public void send(Object value, Column column, PreparedStatement ps, int parameterIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		
	}

	public Object receive(ResultSet rs, int columnIndex) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}


}
