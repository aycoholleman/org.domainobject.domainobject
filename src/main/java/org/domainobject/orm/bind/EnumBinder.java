package org.domainobject.orm.bind;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.domainobject.orm.core.Column;

/**
 * The default binder for enum values.
 */
public final class EnumBinder implements IBinder {

	public boolean isSQLNull(Object value, Column column)
	{
		// TODO Auto-generated method stub
		return false;
	}

	public void send(Object value, Column column, PreparedStatement ps, int parameterIndex) throws Exception
	{
		// TODO Auto-generated method stub
		
	}

	public Object receive(ResultSet rs, int columnIndex) throws Exception
	{
		// TODO Auto-generated method stub
		return null;
	}

}
