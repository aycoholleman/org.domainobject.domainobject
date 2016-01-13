package org.domainobject.orm.exception;

/**
 * Thrown when a required column appears to be unmapped.
 */
public final class UnmappedColumnException extends DomainObjectException {

	private static final long serialVersionUID = -5869062571227232777L;

	private final Class<?> cls;
	private final String column;


	public UnmappedColumnException(final Class<?> cls, final String column)
	{
		this.cls = cls;
		this.column = column;
	}


	@Override
	public String getMessage()
	{
		return String.format("Non-existent or unmapped column in %s: %s", cls.getSimpleName(), column);
	}

}
