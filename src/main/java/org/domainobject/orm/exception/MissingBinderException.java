package org.domainobject.orm.exception;

import java.lang.reflect.Field;

@SuppressWarnings("serial")
public class MissingBinderException extends MetaDataAssemblyException {

	private final Field field;

	public MissingBinderException(Field field)
	{
		this.field = field;
	}

}
