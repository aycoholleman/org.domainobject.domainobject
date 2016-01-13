package org.domainobject.orm.bind;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.domainobject.orm.core.Column;

/**
 * The default binder for float and Float values.
 */
public final class FloatBinder extends NumberBinder {

	public void send(Object value, Column column, PreparedStatement ps, int parameterIndex) throws Exception
	{
		// TODO Auto-generated method stub

	}


	public Float receive(ResultSet rs, int columnIndex) throws Exception
	{
		// TODO Auto-generated method stub
		return null;
	}

}
