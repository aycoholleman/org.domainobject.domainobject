package org.domainobject.orm.core;

import java.sql.Connection;

/**
 * {@code Entity} representing database tables.
 */
class TableEntity extends StaticEntity {

	public TableEntity(String name, String schema, Connection conn)
	{
		super(name, schema, conn);
	}

}
