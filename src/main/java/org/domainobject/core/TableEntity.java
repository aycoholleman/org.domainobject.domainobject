package org.domainobject.core;

import java.sql.Connection;

/**
 * {@code Entity} representing database tables.
 */
class TableEntity extends StaticEntity {

	public TableEntity(String name, Connection conn)
	{
		super(name, conn);
	}


	public TableEntity(String name, String schema, Connection conn)
	{
		super(name, schema, conn);
	}

}
