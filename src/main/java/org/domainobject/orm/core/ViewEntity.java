package org.domainobject.orm.core;

import java.sql.Connection;

/**
 * {@code Entity} representing database views.
 */
class ViewEntity extends StaticEntity {

	public ViewEntity(String name, Connection conn)
	{
		super(name, conn);
	}


	public ViewEntity(String name, String schema, Connection conn)
	{
		super(name, schema, conn);
	}

}
