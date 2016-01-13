package org.domainobject.orm.core;

import java.sql.Connection;

public interface IObjectFactory {

	InformationSchema createInformationSchema(Connection conn);

}
