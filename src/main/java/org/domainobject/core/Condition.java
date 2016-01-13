package org.domainobject.core;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.domainobject.exception.CircularConditionException;
import org.domainobject.exception.InvalidConditionException;
import org.domainobject.exception.UnmappedFieldException;
import org.domainobject.exception.UnresolvableQueryException;
import org.domainobject.util.Util;

/**
 * <p>
 * A {@code Condition} represents a condition in a SQL WHERE clause. Working
 * with {@code Condition}s gives your code a more object oriented flavor, since
 * you specify them in terms of fields of persistent classes rather than in
 * terms of the columns they map to. You can add {@code Condition}s to
 * {@link QuerySpec} objects by calling {@link QuerySpec#where(Condition...)},
 * and various methods of domainobject's standard persisters also accept
 * {@code Condition} objects.
 * </p>
 * 
 * <h3>Sibling conditions</h3>
 * 
 * <p>
 * A {@code Condition} maintains a list of other {@code Condition} objects,
 * which you can add through the various {@code and} and {@code or} methods.
 * These conditions are taken to be sibling conditions, so all toghether they
 * get surrounded by a pair of parentheses when the {@code Condition} is
 * translated into raw SQL.
 * </p>
 * 
 * <h3>Instantiation</h3>
 * <p>
 * You can instantiate a {@code Condition} in 3 ways:
 * <ol>
 * 
 * <li>By providing a field name, a comparison operator and a value:<br>
 * {@code Condition condition = new Condition("name", LIKE, "%son");}</li>
 * 
 * <li>By providing only a field name and a comparison operator:<br>
 * {@code Condition condition = new Condition("age", LESS_THAN);}<br>
 * The value will then determined when the {@code Condition} is bound to a
 * {@code Query}. See {@link Query#bind(Object, Condition[])}. Thus, when you
 * create a {@code Condition} this way, you <i>must</i> bind it using
 * {@link Query#bind(Object, Condition[])} rather than using
 * {@link Query#bind(Condition[])}. Otherwise you will an
 * {@link UnresolvableQueryException} will be thrown. Also, when you specify a
 * {@code Condition} this way, the object you specify as the first argument to
 * {@link Query#bind(Object, Condition[])} <i>must</i> have a non-null value for
 * the field specified by the {@code Condition}.</li>
 * 
 * <li>By providing only a field name:<br>
 * {@code Condition condition = new Condition("age");}<br>
 * If you only provide a property, the operator is set to {@link #EQUALS} and
 * the constraining value is determined as described above.</li>
 * 
 * </ol>
 * </p>
 * 
 * 
 * <h3>Allowed operators</h3>
 * 
 * <p>
 * The operators you are allowed to use are declared as constants in the
 * Condition class (e.g. {@link #EQUALS}, {@link #GREATER_THAN} or
 * {@link #IS_NOT_NULL}). However, if you think
 * 
 * <pre>
 * new Condition(&quot;age&quot;, &quot;&gt;&quot;, 21)
 * </pre>
 * 
 * is easier on the eye than<br>
 * <br>
 * 
 * <pre>
 * new Condition(&quot;age&quot;, GREATER_THAN, 21)
 * </pre>
 * 
 * you are free to choose the first version.
 * </p>
 * 
 * 
 * <h3>Allowed operator-value combinations</h3>
 * 
 * <p>
 * You can generally combine any operator with any value, with the following
 * exceptions:
 * <ol>
 * 
 * <li>With operators {@link #IS_NULL} and {@link #IS_NOT_NULL} you must use the
 * two-arguments constructor. Otherwise an {@link InvalidConditionException} is
 * thrown.</li>
 * 
 * <li>With operators {@link #IN} and {@link #NOT_IN} you must use the
 * three-arguments constructor. The value argument you specify must not be null,
 * and, if it is an array, must contain only non-null values. Otherwise an
 * {@link InvalidConditionException} is thrown.</li>
 * 
 * <li>If you use the three-arguments constructor and you specify null for the
 * value argument, the only allowed operators are {@link #EQUALS} and
 * {@link #NOT_EQUALS}. This is equivalent to invoking the two-arguments
 * constructor with the {@link #IS_NULL} c.q. {@link #IS_NOT_NULL} operator.</li>
 * 
 * </ol>
 * </p>
 * 
 * 
 * <h3>Condition evaluation</h3>
 * 
 * <p>
 * The process of translating a {@code Condition} into raw SQL is called
 * evaluating or resolving a {@code Condition}. While a {@code Condition} is
 * ultimately evaluated for just one persistent object, it is not created for
 * any persistent object in particular. For example, if Employee and Department
 * both have a "name" field, then
 * 
 * <pre>
 * Condition condition = new Condition(&quot;name&quot;, LIKE, &quot;%ab%&quot;);
 * </pre>
 * 
 * can be used for both Employee and Department queries. Consequently, it is
 * only when the {@code Condition} gets evaluated that you might get an
 * {@link UnmappedFieldException}.
 * </p>
 * 
 */
public final class Condition {

	// Constant indicating that the value of a {@code Condition} must be
	// regarded as not set (i.e. not even null). If a condition's value is
	// NOT_SET, then the condition can only be resolved by supplying an instance
	// of a class containing the field specified by the condition. The value
	// will then be retrieved from that instance.
	static final Object NOT_SET = new Object();
	
	/**
	 * SQL Operator <code>=</code>
	 */
	public static final String EQUALS = "=";
	/**
	 * SQL Operator <code>!=</code> (or <code>&lt;&gt;</code>)
	 */
	public static final String NOT_EQUALS = "!=";
	/**
	 * SQL Operator <code>&lt;</code>
	 */
	public static final String LESS_THAN = "<";
	/**
	 * SQL Operator <code>&lt;=</code>
	 */
	public static final String LESS_EQUAL = "<=";
	/**
	 * SQL Operator <code>&gt;</code>
	 */
	public static final String GREATER_THAN = ">";
	/**
	 * SQL Operator <code>&gt;=</code>
	 */
	public static final String GREATER_EQUAL = ">=";
	/**
	 * SQL Operator <code>LIKE</code>
	 */
	public static final String LIKE = "LIKE";
	/**
	 * SQL Operator <code>NOT LIKE</code>
	 */
	public static final String NOT_LIKE = "NOT LIKE";
	/**
	 * SQL Operator <code>IS NULL</code>. Since IS NULL is a unary operator, you
	 * <b>must</b> not use the three-arguments constructor.
	 */
	public static final String IS_NULL = "IS NULL";
	/**
	 * SQL Operator <code>IS NOT NULL</code>. Since IS NOT NULL is a unary
	 * operator, you <b>must</b> not use the three-arguments constructor.
	 */
	public static final String IS_NOT_NULL = "IS NOT NULL";
	/**
	 * SQL Operator <code>IN</code>. When using this operator, you <b>must</b>
	 * use the three-arguments constructor.
	 */
	public static final String IN = "IN";
	/**
	 * SQL Operator <code>NOT IN</code>. When using this operator, you
	 * <b>must</b> use the three-arguments constructor.
	 */
	public static final String NOT_IN = "NOT IN";
	/**
	 * SQL Operator <code>BETWEEN</code>. When using this operator, you
	 * <b>must</b> use the three-arguments constructor, and the value (3d)
	 * argument to the constructor must be an array consisting of exactly two
	 * non-null elements.
	 */
	public static final String BETWEEN = "BETWEEN";
	/**
	 * SQL Operator <code>NOT BETWEEN</code>. When using this operator, you
	 * <b>must</b> use the three-arguments constructor, and the value (3d)
	 * argument to the constructor must be an array consisting of exactly two
	 * non-null elements.
	 */
	public static final String NOT_BETWEEN = "NOT BETWEEN";

	private static enum Conjugator
	{
		NONE, AND, OR
	};

	// The initial capacity of the siblings list
	private static final int INIT_NUM_SIBLINGS = 4;

	private final Conjugator conjugator;
	private final String field;
	private final String operator;
	private final Object value;

	private final Condition createdFrom;
	private ArrayList<Condition> siblings;


	/**
	 * Create a condition for the specified field.
	 * 
	 * @param field
	 *            The name of a field, presumed to belong to a persistent class,
	 *            although this is not checked by the constructor.
	 */
	public Condition(String field)
	{
		this(Conjugator.NONE, field, EQUALS, NOT_SET, false);
	}


	/**
	 * Create a condition for the specified field and operator.
	 * 
	 * @param field
	 *            The name of a field, presumed to belong to a persistent class.
	 * @param operator
	 *            One of the comparison operators defined as constants in the
	 *            Condition class. E.g. {@link #LESS_THAN}.
	 */
	public Condition(String field, String operator)
	{
		this(Conjugator.NONE, field, operator, NOT_SET, true);
	}


	/**
	 * Create a condition for the specified field, operator and value.
	 * 
	 * @param field
	 *            The name of a field, presumed to belong to a persistent class.
	 * @param operator
	 *            One of the operators defined as constants in the Condition
	 *            class. E.g. {@link #LESS_THAN}.
	 * @param value
	 *            The value to constrain on.
	 * 
	 * @throws InvalidConditionException
	 */
	public Condition(String field, String operator, Object value)
	{
		this(Conjugator.NONE, field, operator, value, true);
	}


	private Condition(Conjugator conjugator, Condition other)
	{
		this.createdFrom = other;
		this.conjugator = conjugator;
		this.field = other.field;
		this.operator = other.operator;
		this.value = other.value;
		this.siblings = other.siblings;
	}


	/**
	 * Internally used constructor for the and() and or() methods.
	 * 
	 * @param conjugator
	 *            The logical operator ("AND" or "OR") that must be used to
	 *            append this condition to the preceding one.
	 * @param property
	 *            The property that is the subject of this Condition.
	 * @param operator
	 *            The comparison operator in this Condition.
	 * @param value
	 *            The contraining value in this Condition.
	 * @param isUserSuppliedOperator
	 *            Whether or not to normalize and check the comparison operator.
	 */
	private Condition(Conjugator conjugator, String property, String operator, Object value, boolean isUserSuppliedOperator)
	{
		this.createdFrom = null;
		this.conjugator = conjugator;
		this.field = property;
		this.operator = isUserSuppliedOperator ? normalizeOperator(operator) : operator;
		this.value = value;
		if (isUserSuppliedOperator) {
			checkCondition();
		}
	}


	/**
	 * Add a sibling Condition to this Condition using the AND operator. Watch
	 * out for circular conditions. For example:
	 * 
	 * <pre>
	 * Condition condition1 = new Condition(&quot;name&quot;, EQUALS, &quot;Smith&quot;);
	 * Condition condition2 = new Condition(&quot;age&quot;, LESS_THAN, 40);
	 * condition1.and(condition2); // OK
	 * condition2.and(condition1); // Illegal! condition2 already a sibling of condition1
	 * </pre>
	 * 
	 * @param condition
	 *            Another condition.
	 * 
	 * @return this Condition.
	 * 
	 * @throws CircularConditionException
	 *             If this Condition is amongst the siblings of the specified
	 *             Condition.
	 * 
	 */
	public Condition and(Condition condition)
	{
		if (siblings == null) {
			siblings = new ArrayList<Condition>(INIT_NUM_SIBLINGS);
		}
		if (condition.siblings != null) {
			for (Condition c0 : condition.siblings) {
				for (Condition c1 = c0; c1 != null; c1 = c1.createdFrom) {
					if (c1 == this) {
						throw new CircularConditionException(condition, this);
					}
				}
			}
		}
		// We do not add the specified condition directly to the list of siblings!
		// Instead we clone it and add the clone to the list of siblings. This is
		// because the conjugator (AND or OR) of a Condition is immutable.
		// So we can't do:
		//
		// condition.conjugator = AND;
		// siblings.add(condition);
		//
		// We do keep a reference though to the original Condition in the cloned
		// Condition through the createdFrom field. This is so we can detect circular
		// conditions.
		//
		// If the conjugator would not be immutable, you could not add one condition
		// to more than one parent condition; if you could modify the condition's
		// conjugator (e.g. from AND to OR), you would change it for all parent
		// conditions to which this condition was added.
		siblings.add(new Condition(Conjugator.AND, condition));
		return this;
	}


	/**
	 * Add a sibling Condition to this Condition using the OR operator.
	 * 
	 * @see #and(Condition)
	 * 
	 * @param condition
	 *            Another condition.
	 * 
	 * @return this Condition.
	 * 
	 * @throws CircularConditionException
	 *             If this Condition is amongst the siblings of the specified
	 *             Condition.
	 */
	public Condition or(Condition condition)
	{
		if (siblings == null) {
			siblings = new ArrayList<Condition>(INIT_NUM_SIBLINGS);
		}
		if (condition.siblings != null) {
			for (Condition c0 : condition.siblings) {
				for (Condition c1 = c0; c1 != null; c1 = c1.createdFrom) {
					if (c1 == this) {
						throw new CircularConditionException(condition, this);
					}
				}
			}
		}
		siblings.add(new Condition(Conjugator.OR, condition));
		return this;
	}


	/**
	 * Add a sibling Condition to this Condition using the AND operator. The
	 * sibling Condition is created from the specified field. See
	 * {@link #Condition(String)}).
	 * 
	 * @param field
	 *            The field from which to create the sibling Condition.
	 * 
	 * @return this Condition.
	 */
	public Condition and(String field)
	{
		if (siblings == null) {
			siblings = new ArrayList<Condition>(INIT_NUM_SIBLINGS);
		}
		siblings.add(new Condition(Conjugator.AND, field, EQUALS, NOT_SET, false));
		return this;
	}


	/**
	 * Add a sibling Condition to this Condition using the OR operator. The
	 * sibling Condition is created from the specified field. See
	 * {@link #Condition(String)}).
	 * 
	 * @param field
	 *            The field from which to create the sibling Condition.
	 * 
	 * @return this Condition.
	 */
	public Condition or(String field)
	{
		if (siblings == null) {
			siblings = new ArrayList<Condition>(INIT_NUM_SIBLINGS);
		}
		siblings.add(new Condition(Conjugator.OR, field, EQUALS, NOT_SET, false));
		return this;
	}


	/**
	 * Add a sibling condition to this {@code Condition} using the AND operator.
	 * The sibling condition is created from the specified field and operator.
	 * See {@link #Condition(String,String)}).
	 * 
	 * @param field
	 *            The field from which to create the sibling Condition.
	 * 
	 * @param operator
	 *            The operator from which to create the sibling Condition.
	 * 
	 * @return this Condition.
	 */
	public Condition and(String field, String operator)
	{
		if (siblings == null) {
			siblings = new ArrayList<Condition>(INIT_NUM_SIBLINGS);
		}
		siblings.add(new Condition(Conjugator.AND, field, operator, NOT_SET, true));
		return this;
	}


	/**
	 * Add a sibling Condition to this Condition using the OR operator. The
	 * sibling Condition is created from the specified field and operator. See
	 * {@link #Condition(String,String)}).
	 * 
	 * @param field
	 *            The field from which to create the sibling Condition.
	 * 
	 * @param operator
	 *            The operator from which to create the sibling Condition.
	 * 
	 * @return this Condition.
	 */
	public Condition or(String field, String operator)
	{
		if (siblings == null) {
			siblings = new ArrayList<Condition>(INIT_NUM_SIBLINGS);
		}
		siblings.add(new Condition(Conjugator.OR, field, operator, NOT_SET, true));
		return this;
	}


	/**
	 * Add a sibling Condition to this Condition using the AND operator. The
	 * sibling Condition is created from the specified field, operator and
	 * value. See {@link #Condition(String,String,Object)}).
	 * 
	 * @param field
	 *            The field from which to create the sibling Condition.
	 * 
	 * @param operator
	 *            The operator from which to create the sibling Condition.
	 * 
	 * @param value
	 *            The value from which to create the sibling Condition.
	 * 
	 * @return this Condition.
	 */
	public Condition and(String field, String operator, Object value)
	{
		if (siblings == null) {
			siblings = new ArrayList<Condition>(INIT_NUM_SIBLINGS);
		}
		siblings.add(new Condition(Conjugator.AND, field, operator, value, true));
		return this;
	}


	/**
	 * Add a sibling Condition to this Condition using the OR operator. The
	 * sibling Condition is created from the specified field, operator and
	 * value. See {@link #Condition(String,String,Object)}).
	 * 
	 * @param field
	 *            The field from which to create the sibling Condition.
	 * 
	 * @param operator
	 *            The operator from which to create the sibling Condition.
	 * 
	 * @param value
	 *            The value from which to create the sibling Condition.
	 * 
	 * @return this Condition.
	 */
	public Condition or(String field, String operator, Object value)
	{
		if (siblings == null) {
			siblings = new ArrayList<Condition>(INIT_NUM_SIBLINGS);
		}
		siblings.add(new Condition(Conjugator.OR, field, operator, value, true));
		return this;
	}


	/**
	 * Get the name of the field for which this {@code Condition} was made.
	 * 
	 * @return The name of the property
	 */
	public String getField()
	{
		return field;
	}


	/**
	 * Get the comparison operator used in this Condition. If no operator was
	 * specified (i.e. if the one-argument constructor was used), the operator
	 * will be {@link #EQUALS}.
	 * 
	 * @return The operator
	 */
	public String getOperator()
	{
		return operator;
	}


	/**
	 * Get the constraining value in this Condition. If no value was specified
	 * (i.e. if the one- or two-arguments constructor was used), the value will
	 * be {@link #NOT_SET}.
	 * 
	 * @return The value.
	 */
	public Object getValue()
	{
		return value;
	}


	/**
	 * Get the sibling {@code Condition}s of this {@code Condition}.
	 * 
	 * @return The sibling {@code Condition}s
	 */
	public List<Condition> getSiblings()
	{
		return siblings;
	}


	/**
	 * Get the SQL parameter name that is going to be used for this
	 * {@code Condition}. When embedded within a SQL query, a {@code Condition}
	 * that is created like this: {@code new Condition("name", EQUALS, "John")}
	 * is translated into something like: {@code name = :name_13fb076a}. The
	 * value of the {@code Condition} is only later bound to the parameter (
	 * {@code :name_13fb076a} in this case).
	 * <p>
	 * For operators {@code BETWEEN} and {@code NOT_BETWEEN}, the
	 * {@code Condition} is translated using two parameters, e.g.
	 * {@code age BETWEEN :age_3b42f1 AND :age_3b42f1_2}. In this case, the name
	 * of the first parameter is returned. The name of the second parameter can
	 * be deduced by appending "_2" to the name of the first parameter.
	 * </p>
	 * <p>
	 * For operators {@code IN} and {@code NOT_IN}, the {@code Condition} is
	 * translated using an arbitrary amount of parameters, e.g.
	 * {@code name IN(:name_c9512d,:name_c9512d_2,:name_c9512d_3,:name_c9512d_4)}
	 * . In this case the second parameter has suffix "_2", the third "_3", etc.
	 * </p>
	 * 
	 * @return The name of the parameter used for this {@code Condition}.
	 */
	public String getParameterName()
	{
		//@formatter:off
		return new StringBuilder(24)
			.append(field)
			.append('_')
			.append(Integer.toHexString(System.identityHashCode(this)))
			.toString();
		//@formatter:on
	}


	/**
	 * Two {@code Condition}s are equal if they have the same field, operator
	 * and siblings {@code Condition}s, and if these siblings are conjugated
	 * indentically (using AND or OR). The value of the condition is not taken
	 * into consideration when establishing equality. This is because equality
	 * is established with respect to the SQL they are resolved into. Because
	 * domainobject only works with parametrized queries (using "?" placeholders
	 * within the SQL), the {@code Condition}'s value is irrelevant.
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		Condition other = (Condition) obj;
		if (!equals(this.field, other.field)) {
			return false;
		}
		if (this.operator != other.operator) {
			return false;
		}
		if (this.conjugator != other.conjugator) {
			return false;
		}
		if (this.siblings == null) {
			return (other.siblings == null);
		}
		return this.siblings.equals(other.siblings);
	}


	/**
	 * Overrides {@link Object#hashCode()} to reflect the peculiarities of
	 * establishing {@code Condition} equality. See {@link #equals(Object)}.
	 */
	@Override
	public int hashCode()
	{
		int hash = 1;
		hash = (hash * 31) + field.hashCode();
		hash = (hash * 31) + operator.hashCode();
		hash = (hash * 31) + conjugator.ordinal();
		if (siblings != null) {
			hash = (hash * 31) + siblings.hashCode();
		}
		return hash;
	}


	@Override
	public String toString()
	{

		if (siblings == null) {
			return selfToString(new StringBuilder()).toString();
		}

		StringBuilder sb = new StringBuilder();

		sb.append('(');

		selfToString(sb);

		for (Condition condition : siblings) {
			condition.selfToString(sb);
		}

		sb.append(')');

		return sb.toString();
	}


	/**
	 * Translates this {@code Condition} and its siblings into raw SQL.
	 */
	<T> String resolve(MetaData<T> metadata, String tableAlias)
	{
		return resolve(new StringBuilder(32), metadata, tableAlias).toString();
	}


	/**
	 * Translates this {@code Condition} into raw SQL.
	 */
	<T> String resolveSelf(MetaData<T> metadata, String tableAlias)
	{
		return resolveSelf(new StringBuilder(32), metadata, tableAlias).toString();
	}


	private static String normalizeOperator(String string)
	{
		if (string.equals(EQUALS)) {
			return EQUALS;
		}
		if (string.equals(NOT_EQUALS) || string.equals("<>")) {
			return NOT_EQUALS;
		}
		if (string.equals(LESS_THAN)) {
			return LESS_THAN;
		}
		if (string.equals(LESS_EQUAL)) {
			return LESS_EQUAL;
		}
		if (string.equals(GREATER_THAN)) {
			return GREATER_THAN;
		}
		if (string.equals(GREATER_EQUAL)) {
			return GREATER_EQUAL;
		}
		if (string.equalsIgnoreCase(LIKE)) {
			return LIKE;
		}
		if (string.equalsIgnoreCase(NOT_LIKE)) {
			return NOT_LIKE;
		}
		if (string.equalsIgnoreCase(IS_NULL)) {
			return IS_NULL;
		}
		if (string.equalsIgnoreCase(IS_NOT_NULL)) {
			return IS_NOT_NULL;
		}
		if (string.equalsIgnoreCase(IN)) {
			return IN;
		}
		if (string.equalsIgnoreCase(NOT_IN)) {
			return NOT_IN;
		}
		if (string.equalsIgnoreCase(BETWEEN)) {
			return BETWEEN;
		}
		if (string.equalsIgnoreCase(NOT_BETWEEN)) {
			return NOT_BETWEEN;
		}
		throw new InvalidConditionException("Invalid operator: " + string);
	}


	private static boolean equals(Object obj1, Object obj2)
	{
		if (obj1 == null) {
			return obj2 == null ? true : false;
		}
		return obj2 == null ? false : obj1.equals(obj2);
	}


	private <T> StringBuilder resolve(final StringBuilder sql, final MetaData<T> metadata, final String tableAlias)
	{

		if (siblings == null) {
			sql.append('(');
			resolveSelf(sql, metadata, tableAlias);
			sql.append(')');
			return sql;
		}

		sql.append('(');
		sql.append('(');
		resolveSelf(sql, metadata, tableAlias);
		sql.append(')');
		for (Condition condition : siblings) {
			sql.append(condition.conjugator);
			condition.resolve(sql, metadata, tableAlias);
		}
		sql.append(')');

		return sql;
	}


	//////////////////////////////////////////////////////////////////////////////////////////////
	///  NB: The logic of this method MUST exactly mirror Query#bind(Object, Condition[]) !!!  ///
	//////////////////////////////////////////////////////////////////////////////////////////////
	private <T> StringBuilder resolveSelf(StringBuilder sql, MetaData<T> metadata, String tableAlias)
	{

		if (tableAlias != null) {
			sql.append(tableAlias).append('.');
		}

		sql.append(Util.getQuotedColumnForField(metadata, field));

		if (operator == IS_NULL || operator == IS_NOT_NULL) {
			sql.append(' ').append(operator);
		}

		else if (operator == IN || operator == NOT_IN) {
			sql.append(' ').append(operator);
			sql.append("(:").append(getParameterName());
			if (value.getClass().isArray()) {
				for (int i = 1; i < Array.getLength(value); ++i) {
					sql.append(':').append(getParameterName()).append('_').append(i + 1);
				}
			}
			sql.append(')');
		}

		else if (operator == BETWEEN || operator == NOT_BETWEEN) {
			sql.append(' ').append(operator);
			sql.append(" :").append(getParameterName());
			sql.append(" AND :").append(getParameterName()).append("_2");
		}

		else if (value == null) {
			// Translate "= null" into "IS NULL" and "!= null" into "IS NOT NULL".
			if (operator == EQUALS) {
				sql.append(' ').append(IS_NULL);
			}
			else if (operator == NOT_EQUALS) {
				sql.append(' ').append(IS_NOT_NULL);
			}
			// No other operators allowed by constructor when value == null
		}

		else {
			sql.append(operator);
			sql.append(" :").append(getParameterName());
		}

		return sql;
	}
	
	private void checkCondition()
	{
		if (operator == IS_NULL || operator == IS_NOT_NULL) {
			if (value != NOT_SET) {
				throw new InvalidConditionException("Value must not be set when using IS NULL or IS NOT NULL (use the two-argument constructor of Condition)");
			}
		}
		else if (operator == IN || operator == NOT_IN) {
			if (value == NOT_SET) {
				throw new InvalidConditionException("You must specify a value when using IN or NOT IN (use the three-argument constructor of Condition)");
			}
			if (value == null) {
				throw new InvalidConditionException("You must specify a non-null value when using IN or NOT IN");
			}
			if (value instanceof Object[]) {
				Object[] array = (Object[]) value;
				if (array.length == 0) {
					throw new InvalidConditionException("Array must contain at least one element when using IN or NOT IN");
				}
				for (Object obj : array) {
					if (obj == null) {
						throw new InvalidConditionException("Array may not contain null values when using IN or NOT IN");
					}
				}
			}
		}
		else if (operator == BETWEEN || operator == NOT_BETWEEN) {
			if (value == NOT_SET) {
				throw new InvalidConditionException("You must specify a value when using BETWEEN or NOT BETWEEN (use the three-argument constructor of Condition)");
			}
			if (!(value instanceof Object[])) {
				throw new InvalidConditionException("You must specify a 2-element array when using BETWEEN or NOT BETWEEN");
			}
			Object[] array = (Object[]) value;
			if (array.length != 2) {
				throw new InvalidConditionException("You must specify a 2-element array when using BETWEEN or NOT BETWEEN");
			}
			for (Object obj : array) {
				if (obj == null) {
					throw new InvalidConditionException("Array may not contain null values when using BETWEEN or NOT BETWEEN");
				}
			}
		}
		else if (value == null && operator != EQUALS && operator != NOT_EQUALS) {
			throw new InvalidConditionException("Value null only allowed with operator EQUALS or NOT_EQUALS");
		}
	}


	private StringBuilder selfToString(StringBuilder sb)
	{
		if (conjugator != null) {
			sb.append(' ').append(conjugator).append(' ');
		}
		sb.append('(');
		sb.append(field);
		sb.append(' ');
		sb.append(operator);
		if (operator == IS_NULL || operator == IS_NOT_NULL) {
			//
		}
		else if (operator == IN || operator == NOT_IN) {
			sb.append(" (");
			sb.append(valueToString(value));
			sb.append(')');
		}
		else {
			sb.append(' ').append(valueToString(value));
		}
		sb.append(')');
		return sb;
	}


	private static String valueToString(Object value)
	{
		if (value == NOT_SET) {
			return "@NOT SET@";
		}
		if (value == null) {
			return "null";
		}
		if (value instanceof CharSequence) {
			return new StringBuilder().append('"').append(value).append('"').toString();
		}
		if (value.getClass().isArray()) {
			StringBuilder sb = new StringBuilder();
			sb.append('[');
			for (int i = 0; i < Array.getLength(value); ++i) {
				if (i > 0) {
					sb.append(',');
				}
				sb.append(valueToString(Array.get(value, i)));
			}
			sb.append(']');
			return sb.toString();
		}
		return value.toString();
	}

}
