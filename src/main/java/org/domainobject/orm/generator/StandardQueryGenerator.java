package org.domainobject.orm.generator;

import static org.domainobject.orm.core.QuerySpec.SOURCE_TABLE_ALIAS;
import static org.domainobject.orm.core.QuerySpec.TABLE_ALIAS_3;
import static org.domainobject.orm.core.QuerySpec.TARGET_TABLE_ALIAS;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.HashMap;

import org.domainobject.orm.core.Context;
import org.domainobject.orm.core.MetaData;
import org.domainobject.orm.core.Query;
import org.domainobject.orm.core.QuerySpec;
import org.domainobject.orm.exception.DomainObjectException;
import org.domainobject.orm.exception.SQLGenerationException;
import org.domainobject.orm.util.SQLString;
import org.domainobject.orm.util.Util;
import org.domainobject.util.ArrayUtil;
import org.domainobject.util.convert.Stringifier;

/**
 * An abstract implementation of the SQLAdapter interface implementing most of
 * its methods.
 */
public class StandardQueryGenerator {

	private static final HashMap<Context, StandardQueryGenerator> impls = new HashMap<>(4);

	private static final Stringifier PARAMETER_GENERATOR = new Stringifier() {

		public String execute(Object value, Object... args)
		{
			return "?";
		}
	};

	public static StandardQueryGenerator getGenerator(Context ctx)
	{

		// TODO move to ObjectFactory
		return null;

		// StandardQueryGenerator sqg = impls.get(ctx);
		//
		// if (sqg == null) {
		//
		// Class<StandardQueryGenerator> base = StandardQueryGenerator.class;
		//
		// String match0 = base.getName();
		// String match1 = new StringBuilder(95).append(match0).append("__")
		// .append(ctx.getDatabaseVendor()).toString();
		// String match2 = new StringBuilder(95).append(match1).append('_')
		// .append(ctx.getDatabaseMajorVersion()).toString();
		// String match3 = new StringBuilder(95).append(match2).append('_')
		// .append(ctx.getDatabaseMinorVersion()).toString();
		//
		// Class<?> cls;
		//
		// try {
		// cls = Class.forName(match3);
		// }
		// catch (ClassNotFoundException e3) {
		// try {
		// cls = Class.forName(match2);
		// }
		// catch (ClassNotFoundException e2) {
		// try {
		// cls = Class.forName(match1);
		// }
		// catch (ClassNotFoundException e1) {
		// cls = base;
		// }
		// }
		// }
		// try {
		// sqg = (StandardQueryGenerator) cls.newInstance();
		// impls.put(ctx, sqg);
		// }
		// catch (Exception e) {
		// throw new ReflectionException(e);
		// }
		// }
		// return sqg;
	}

	public <T> Query<T> sqlQuery(MetaData<T> metadata, QuerySpec qs)
	{

		Query<T> query = Query.createQuery(this, "sqlQuery", metadata, qs);
		if (!query.isNew()) {
			return query;
		}

		if (qs.getOffset() != 0 || qs.getMaxRecords() != 0) {
			throw new DomainObjectException(
					"Cannot generate query with offset or maximum records in result set");
		}
		try {

			StringBuilder sql = new StringBuilder(64);

			// SELECT
			sql.append(qs.getSelectClause(metadata, TARGET_TABLE_ALIAS));

			// FROM
			sql.append(SQLString.FROM.delimited());
			sql.append(getFrom(metadata, TARGET_TABLE_ALIAS));

			// WHERE
			sql.append(qs.getWhereClause(metadata, TARGET_TABLE_ALIAS));

			// ORDER BY
			sql.append(qs.getOrderByClause(metadata, TARGET_TABLE_ALIAS));

			query.initialize(sql.toString(), metadata, qs);
			return query;
		}
		catch (Exception e) {
			// throw new SQLGenerationException(e);
			throw DomainObjectException.rethrow(e);
		}
	}

	public <T> Query<T> sqlSelect(MetaData<T> metadata, QuerySpec qs)
	{
		throw new DomainObjectException("Operation not supported by this SQL adapter");
	}

	public <T> Query<T> sqlCountStar(MetaData<T> metadata, QuerySpec qs)
	{
		Query<T> query = Query.createQuery(this, "sqlCountStar", metadata, qs);
		if (!query.isNew()) {
			return query;
		}
		StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ");
		sql.append(getFrom(metadata, TARGET_TABLE_ALIAS));
		sql.append(qs.getWhereClause(metadata, TARGET_TABLE_ALIAS));
		query.initialize(sql.toString(), metadata, qs);
		return query;
	}

	public <T> Query<T> insert(MetaData<T> metadata)
	{
		Query<T> query = Query.createQuery(this, "insert", metadata);
		if (!query.isNew()) {
			return query;
		}
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(metadata.getEntity().getFrom());
		sql.append('(');
		sql.append(Util.createSelectClause(metadata));
		sql.append(")VALUES(");
		sql.append(ArrayUtil.implode(metadata.getAttributeFieldNames(), PARAMETER_GENERATOR));
		sql.append(')');
		query.initialize(sql.toString(), metadata, null);
		return query;
	}

	public <T> Query<T> sqlUpdate(final MetaData<T> metadata, QuerySpec qs)
	{
		Query<T> query = Query.createQuery(this, "update", metadata);
		if (!query.isNew()) {
			return query;
		}
		StringBuilder sql = new StringBuilder("UPDATE ");
		sql.append(getFrom(metadata, TARGET_TABLE_ALIAS));
		sql.append("SET ");
		sql.append(qs.getUpdateClause(metadata, TARGET_TABLE_ALIAS));
		sql.append(' ');
		sql.append(qs.getWhereClause(metadata, TARGET_TABLE_ALIAS));
		query.initialize(sql.toString(), metadata, qs);
		return query;
	}

	public <T> Query<T> sqlDelete(MetaData<T> metadata, QuerySpec qs)
	{
		Query<T> query = Query.createQuery(this, "sqlDelete", metadata, qs);
		if (!query.isNew()) {
			return query;
		}
		StringBuilder sb = new StringBuilder(SQLString.DELETE_FROM.delimited());
		try {
			sb.append(metadata.getEntity().getFrom()).append(' ');
			sb.append(qs.getWhereClause(metadata, null));
			query.initialize(sb.toString(), metadata, qs);
			return query;
		}
		catch (Exception e) {
			throw new SQLGenerationException(e);
		}
	}

	public <T> Query<T> sqlManyToMany(MetaData<?> source, MetaData<?> intersection,
			MetaData<T> target, String[] fkToSource, String[] fkToTarget, QuerySpec qs)
	{

		Query<T> query = Query.createQuery(this, "sqlManyToMany", source, target, qs);

		if (!query.isNew()) {
			return query;
		}

		try {

			StringBuilder sql = new StringBuilder();

			// SELECT
			sql.append(qs.getSelectClause(target, TARGET_TABLE_ALIAS));

			// FROM
			sql.append(SQLString.FROM.delimited());
			sql.append(getFrom(source, QuerySpec.SOURCE_TABLE_ALIAS));
			sql.append(',').append(getFrom(intersection, TABLE_ALIAS_3));
			sql.append(',').append(getFrom(target, TARGET_TABLE_ALIAS));

			// Join intersection to target
			Field[] primayKeyFields = target.getPrimaryKeyFields();
			for (int i = 0; i < primayKeyFields.length; ++i) {
				sql.append((i == 0) ? " WHERE " : " AND ");
				sql.append(quoteColumn(target, primayKeyFields[i].getName(), TARGET_TABLE_ALIAS));
				sql.append('=');
				sql.append(quoteColumn(intersection, fkToTarget[i], TABLE_ALIAS_3));
			}

			// Join intersection to source
			primayKeyFields = source.getPrimaryKeyFields();
			for (int i = 0; i < primayKeyFields.length; ++i) {
				sql.append(" AND ");
				sql.append(quoteColumn(source, primayKeyFields[i].getName(), SOURCE_TABLE_ALIAS));
				sql.append('=');
				sql.append(quoteColumn(intersection, fkToSource[i], TABLE_ALIAS_3));
			}

			// Extract subsequent conditions, if any, from the QuerySpec object
			sql.append(qs.getWhereClause(target, TARGET_TABLE_ALIAS, SQLString.AND));

			// Constrain target objects to those related (via intersection) to
			// source object.
			String[] pkFieldNames = source.getPrimaryKeyFieldNames();
			sql.append(SQLString.AND).append(
					createSQLConstraints(source, TARGET_TABLE_ALIAS, pkFieldNames));

			// ORDER BY
			sql.append(qs.getOrderByClause(target, TARGET_TABLE_ALIAS));

			// LIMIT
			sql.append(getLimitClause(qs));

			query.initialize(sql.toString(), target, qs);
			return query;

		}
		catch (Exception e) {
			throw new SQLGenerationException(e);
		}
	}

	protected static String getFrom(MetaData<?> metadata, String alias)
	{
		return metadata.getEntity().getFrom() + ' ' + alias + ' ';
	}

	private static StringBuilder quoteColumn(MetaData<?> metadata, String field, String tableAlias)
			throws SQLException
	{
		return new StringBuilder(tableAlias).append('.').append(
				Util.getQuotedColumnForField(metadata, field));
	}

	protected static StringBuilder getLimitClause(QuerySpec qs)
	{
		StringBuilder sql = new StringBuilder();
		if (qs.getMaxRecords() > 0) {
			sql.append(" LIMIT ");
			if (qs.getOffset() > 0) {
				sql.append(qs.getOffset()).append(',').append(qs.getMaxRecords());
			}
			else {
				sql.append(qs.getMaxRecords());
			}
		}
		return sql;
	}

	private static StringBuilder createSQLConstraints(MetaData<?> metadata, String tableAlias,
			String... properties) throws SQLException
	{
		StringBuilder sql = new StringBuilder();
		for (int i = 0; i < properties.length; ++i) {
			if (i > 0) {
				sql.append(SQLString.AND);
			}
			sql.append('(').append(tableAlias).append('.');
			sql.append(Util.getQuotedColumnForField(metadata, properties[i]));
			sql.append("=?)");
		}
		return sql;
	}

}
