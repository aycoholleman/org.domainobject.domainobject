package org.domainobject.generator;

import org.domainobject.core.MetaData;
import org.domainobject.core.Query;
import org.domainobject.core.QuerySpec;
import org.domainobject.exception.SQLGenerationException;
import org.domainobject.util.SQLString;

class StandardQueryGenerator__MySQL extends StandardQueryGenerator {

	public <T> Query<T> sqlSelect(MetaData<T> metadata, QuerySpec qs)
	{
		Query<T> query = Query.createQuery(this, "sqlSelect", metadata, qs);
		
		if (!query.isNew()) {
			return query;
		}
		
		try {

			StringBuilder sql = new StringBuilder();

			// SELECT
			sql.append(qs.getSelectClause(metadata, QuerySpec.SOURCE_TABLE_ALIAS));

			// FROM
			sql.append(SQLString.FROM.delimited());
			sql.append(getFrom(metadata, QuerySpec.SOURCE_TABLE_ALIAS));

			// WHERE
			sql.append(qs.getWhereClause(metadata, QuerySpec.SOURCE_TABLE_ALIAS));

			// ORDER BY
			sql.append(qs.getOrderByClause(metadata, QuerySpec.SOURCE_TABLE_ALIAS));

			// LIMIT
			sql.append(getLimitClause(qs));

			query.initialize(sql.toString(), metadata, qs);
			return query;
		}
		catch (Exception e) {
			throw new SQLGenerationException(e);
		}
	}







}
