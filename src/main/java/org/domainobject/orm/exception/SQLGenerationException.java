package org.domainobject.orm.exception;

public class SQLGenerationException extends DomainObjectException {

	private static final long serialVersionUID = 1L;


	public SQLGenerationException()
	{
	}


	public SQLGenerationException(String arg0)
	{
		super(arg0);
	}


	public SQLGenerationException(Throwable cause)
	{
		super(cause);
	}


	public SQLGenerationException(String message, Throwable cause)
	{
		super(message, cause);
	}

}
