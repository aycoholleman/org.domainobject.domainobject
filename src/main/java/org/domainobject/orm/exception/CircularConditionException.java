package org.domainobject.orm.exception;

import org.domainobject.orm.core.Condition;

/**
 * Thrown when one {@link Condition} is added as a sibling to another {@code Condition},
 * while that condition was already added as a sibling to the former
 * {@code Condition}.
 */
public class CircularConditionException extends InvalidConditionException {

	private static final long serialVersionUID = 7107760609890900224L;


	public CircularConditionException(Condition withSiblings, Condition inSiblings)
	{

	}

}
