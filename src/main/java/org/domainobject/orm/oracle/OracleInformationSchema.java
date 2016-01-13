package org.domainobject.orm.oracle;

import java.sql.Connection;

import org.domainobject.orm.core.InformationSchema;


public class OracleInformationSchema extends InformationSchema {

	public OracleInformationSchema(Connection conn)
	{
		super(conn);
	}

}
