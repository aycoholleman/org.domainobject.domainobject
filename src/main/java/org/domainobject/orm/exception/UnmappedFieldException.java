package org.domainobject.orm.exception;

/**
 * Thrown when you specify a non-existent or non-persistent property. A
 * non-persistent property is a property that is not mapped to any column.
 */
public final class UnmappedFieldException extends DomainObjectException {

	private static final long serialVersionUID = 7207920174735486250L;

	private final Class<?> cls;
	private final String fieldName;


	public UnmappedFieldException(final Class<?> cls, final String fieldName)
	{
		this.cls = cls;
		this.fieldName = fieldName;
	}

	@Override
	public String getMessage()
	{
		return String.format("Non-existent or unmapped field in %s: %s", cls.getName(), fieldName);
	}

}
