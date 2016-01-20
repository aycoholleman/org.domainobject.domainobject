package org.domainobject.orm.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.domainobject.orm.exception.QuerySpecException;
import org.domainobject.orm.exception.UnresolvableQuerySpecException;
import org.domainobject.orm.util.SQLString;
import org.domainobject.orm.util.Util;

public class Select extends QuerySpec {

	private static class OOSelect {

		final String field;

		OOSelect(String field)
		{
			this.field = field;
		}

		public boolean equals(Object obj)
		{
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			return field.equals(((OOSelect) obj).field);
		}

		public int hashCode()
		{
			return field.hashCode();
		}
	}

	private List<Object> select;

	/**
	 * Add one or more more fields to the SELECT clause. The should belong to
	 * the persistent class corresponding to the table you are SELECTing data
	 * from.
	 * 
	 * @param fields
	 *            One or more property names.
	 * 
	 * @return this QuerySpec instance.
	 */
	public Select select(String... fields)
	{
		if (fields == null || fields.length == 0)
			throw new QuerySpecException("At least one field required");
		if (select == null)
			select = new ArrayList<>(fields.length);
		for (String field : fields)
			select.add(new OOSelect(field));
		return this;
	}

	/**
	 * Add one or more raw SQL expressions to the SELECT clause. Do not include
	 * the SELECT keyword itself. Although you can pass plain column names to
	 * this method, this method is especially meant to add more complex SQL
	 * expressions to the SELECT clause (e.g. expressions involving SQL
	 * functions).
	 * 
	 * @param expressions
	 *            One or more SQL expressions to be added to the SELECT clause.
	 * 
	 * @return This QuerySpec instance.
	 */
	public QuerySpec sql(String... expressions)
	{
		if (expressions == null || expressions.length == 0)
			throw new QuerySpecException("At least one SQL expression required");
		if (select == null)
			select = new ArrayList<>();
		select.addAll(Arrays.asList(expressions));
		return this;
	}

	/**
	 * Extract a SELECT clause from this QuerySpec.
	 * 
	 * @param metadata
	 *            The metadata object through which to translate property names
	 *            into column names. You may pass null if you somehow know that
	 *            the SELECT clause was constructed solely from calls to
	 *            {@link #sqlSelect(String...)} and never to
	 *            {@link #select(String...)}.
	 * @param tableAlias
	 *            A table prefix for columns resulting from calls to
	 *            {@link #select(String...)}. So you would get something like
	 *            <code>table_alias.column_name</code>. You may pass null if you
	 *            don't need fully qualified column names.
	 * 
	 * @return The SELECT clause.
	 * 
	 * @throws SQLException
	 */
	public String getClause(MetaData<?> metadata, String tableAlias) throws SQLException
	{

		StringBuilder sb = new StringBuilder(SQLString.SELECT.delimited());

		if (select == null) {
			if (tableAlias != null) {
				sb.append(tableAlias).append('.');
			}
			return sb.append('*').toString();
		}

		int i = 0;

		for (Object element : select) {

			if (i++ > 0) {
				sb.append(',');
			}

			if (element.getClass() == OOSelect.class) {
				if (metadata == null) {
					throw new UnresolvableQuerySpecException();
				}
				OOSelect s = (OOSelect) element;
				if (tableAlias != null) {
					sb.append(tableAlias).append('.');
				}
				sb.append(Util.getQuotedColumnForField(metadata, s.field));
			}
			else {
				sb.append((String) element);
			}
		}

		return sb.toString();

	}
}
