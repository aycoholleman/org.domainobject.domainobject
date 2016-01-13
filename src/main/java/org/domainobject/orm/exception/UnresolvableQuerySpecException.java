package org.domainobject.orm.exception;

/**
 * Thrown when a {@link QuerySpec} is requested to generate a SELECT, WHERE or
 * ORDER BY clause without being passed a {@link Persistable} object, while the
 * QuerySpec was built using property names (rather than just column names or raw
 * SQLString expressions).
 */
public class UnresolvableQuerySpecException extends QuerySpecException {

	private static final long serialVersionUID = 1L;


	public UnresolvableQuerySpecException()
	{
	}


	public UnresolvableQuerySpecException(String arg0)
	{
		super(arg0);
	}


	public UnresolvableQuerySpecException(Throwable cause)
	{
		super(cause);
	}


	public UnresolvableQuerySpecException(String message, Throwable cause)
	{
		super(message, cause);
	}

}
