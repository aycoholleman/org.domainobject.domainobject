package org.domainobject.orm.exception;

import org.domainobject.util.ArrayUtil;
import org.domainobject.util.convert.Stringifier;

public final class NoSuchQueryException extends DomainObjectException {

	private static final long serialVersionUID = 813925866533178200L;

	private final Object sqlAdapter;
	private final String sqlAdapterMethod;
	private final Class<?>[] argTypes;

	public NoSuchQueryException(Object sqlAdapter, String sqlAdapterMethod, Class<?>[] argTypes)
	{
		this.sqlAdapter = sqlAdapter;
		this.sqlAdapterMethod = sqlAdapterMethod;
		this.argTypes = argTypes;
	}

	public String getMessage()
	{
		StringBuilder sb = new StringBuilder(96);
		sb.append("SQL generator not found: ").append(sqlAdapter.getClass().getSimpleName());
		sb.append('.').append(sqlAdapterMethod).append('(');
		sb.append(ArrayUtil.implode(argTypes, ", ", new Stringifier() {

			public String execute(Object object, Object... options)
			{
				return ((Class<?>) object).getSimpleName();
			}
		}));
		sb.append(')');
		return sb.toString();
	}

}
