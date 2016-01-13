package org.domainobject.orm.exception;

public class UnresolvableQueryException extends DomainObjectException {

	private static final long serialVersionUID = -5202160109521390539L;


	public UnresolvableQueryException()
	{
	}


	public UnresolvableQueryException(String arg0)
	{
		super(arg0);
	}


	public UnresolvableQueryException(Throwable cause)
	{
		super(cause);
	}


	public UnresolvableQueryException(String message, Throwable cause)
	{
		super(message, cause);
	}

}
