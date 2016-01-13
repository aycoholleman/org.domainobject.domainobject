package org.domainobject.orm.core;

import java.sql.Connection;

public abstract class DefaultObjectFactory implements IObjectFactory {

	@Override
	public InformationSchema createInformationSchema(Connection conn)
	{
		return new InformationSchema(conn);
	}

}
