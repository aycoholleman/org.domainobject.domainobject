package org.domainobject.orm.exception;

import org.domainobject.orm.core.Condition;

/**
 * <p>
 * An AmbiguousConditionException is thrown when a {@link Persistable} object's
 * property is null when read during the evaluation of a {@link Condition}. To
 * illustrate why this is problematic, take the following example (with the
 * Employee class implementing _Persistable): <br>
 * <br>
 * 
 * <pre>
 * Employee employee = new Employee();
 * Condition condition = new Condition(&quot;lastName&quot;, EQUALS);
 * String sql = condition.evaluate(employee);
 * </pre>
 * 
 * <br>
 * <br>
 * The Condition was instantiated without specifying a value argument (see
 * {@link Condition#Condition(String, String)}. Therefore the Condition cannot
 * itself determine the value to compare the lastName with. And therefore, when
 * the Condition is evaluated, it will turn to the employee argument and read
 * <i>its</i> lastName property. The lastName property of the employee object,
 * however, is also null (assuming it is not initialized in the constructor of
 * Employee). It might seem simple and obvious enough to translate this
 * situation into something like <code>lastName IS NULL</code> , just like if
 * the Condition was created while specifying null for the value argument: <br>
 * <br>
 * 
 * <pre>
 * Employee employee = new Employee();
 * Condition condition = new Condition(&quot;lastName&quot;, EQUALS, null);
 * String sql = condition.evaluate(employee);
 * // sql now is something like &quot;lastName IS NULL&quot;
 * </pre>
 * 
 * <br>
 * <br>
 * The reason we cannot do this when we need to inspect the _Persistable object,
 * is that one Condition could then result in two different
 * <code>PreparedStatement</code>s. If the employee's lastName would be null,
 * the condition becomes <code>lastName IS NULL</code> , otherwise you get
 * <code>lastName = ?</code>. In other words, in the first case you get a
 * non-parametrized condition, while in the second you get a parametrized
 * condition. For the RDBMS the resulting <code>PreparedStatement</code>s are
 * completely different. Thus you would not be able to identify a
 * PreparedStatement solely by looking at the <code>Condition</code>s that went
 * into it. You would always also take into account the volatile state of the
 * _Persistable object that happened to be passed to
 * {@link Condition#resolve(Persistable, String)}. This is very undesirable,
 * because domainobject heavily relies on a cache of
 * <code>PreparedStatement</code>s, so you need to have some stable mechanism of
 * creating cache keys. Therefore we simply forbid that a _Persistable object's
 * property is null when read during Condition evaluation.
 * </p>
 * 
 * @see Condition
 * @see Condition#Condition(String, String)
 * @see Condition#resolve(Persistable, String)
 * @see PersistableMetaData#getPrepatedStatement(int)
 */
public class AmbiguousConditionException extends InvalidConditionException {

	private static final long serialVersionUID = 4140690253680872856L;

	private final Object persistable;
	private final String property;


	public AmbiguousConditionException(Object persistable, String property)
	{
		this.persistable = persistable;
		this.property = property;
	}


	public String getMessage()
	{
		return persistable.getClass().getSimpleName() + "." + property + " must not be null during Condition evaluation";
	}

}
