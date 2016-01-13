package org.domainobject.persister;

import java.sql.ResultSet;

import org.domainobject.core.Condition;
import org.domainobject.core.Context;
import org.domainobject.core.Cursor;
import org.domainobject.core.MetaData;
import org.domainobject.core.Query;
import org.domainobject.core.QuerySpec;
import org.domainobject.generator.StandardQueryGenerator;
import org.domainobject.util.Util;

/**
 * A {@code StandardPersister} performs common persistency operations on
 * persistent objects. It features exactly the same operations as its sister
 * class {@link StandardPersisterDynamic} and {@link StandardPersisterDedicated}
 * However, with this class the persistency operations are always done on object
 * belonging to a specific {@link Context}. Therefore it less flexible than the
 * {@code StandardPersisterDynamic}, but it has a cleaner API, because it can
 * retrieve a persistent object's metadata object from the {@code Context} with
 * which it was instantiated. On the other hand, it is more flexible than the
 * {@code StandardPersisterDedicated}, which operates on just one class of
 * persistent objects. Yet it has exactly the same API as its dedicated sister.
 * The only difference is that it will have to look up the metadata objects for
 * the persistent objects that you pass to its methods. This is a simple HashMap
 * lookup that most likely incurs negligable overhead.
 */
public class StandardPersister {

	private final StandardPersisterDynamic spd;
	private final Context ctx;


	/**
	 * Creates a StandardPersister without any {@link Persistable} object inside
	 * it. If you use this constructor, you <i>must</i> call
	 * {@link #setPersistable(Persistable) setPersistable()} at least once
	 * before calling any other method or you will get a
	 * {@link NullPointerException}.
	 */
	public StandardPersister(Context context)
	{
		StandardQueryGenerator sqg = StandardQueryGenerator.getGenerator(context);
		this.spd = new StandardPersisterDynamic(sqg);
		this.ctx = context;
	}


	public StandardPersister(Context context, StandardQueryGenerator sqg)
	{
		this.spd = new StandardPersisterDynamic(sqg);
		this.ctx = context;
	}


	/**
	 * <p>
	 * Populate the Persistable object with values from the specified
	 * {@link java.sql.ResultSet}. Although ideally <code>ResultSet</code>s are
	 * both produced and consumed somewhere inside StandardPersister or its
	 * subclasses, you are not <i>prevented</i> from producing the Resultset in
	 * some arbitrary other class.
	 * </p>
	 * 
	 * <p>
	 * Being the only method in the domainobject package that accepts a
	 * "hardcore" JDBC ResultSet, populate() enables you to gently steer your
	 * code back towards domainobject-style persistency handling. Suppose, for
	 * example, that your Employee class extends {@link DomainObject}, which
	 * itself extends StandardPersister. Your Employee class needs some
	 * capability that is not provided by the StandardPersister class. So you
	 * add a new method that executes some mind-blowing SQLString. But the
	 * SQLString still, in the end, just returns a single row from the EMPLOYEE
	 * table (or a set of values you think can appropriately be regarded as
	 * constituting an employee). You could then feed the ResultSet coming back
	 * from the query to populate() and all that remains is a simple Employee
	 * object that can be treated just like any other DomainObject.
	 * </p>
	 * <p>
	 * Note that populate() does <i>not</i> call
	 * {@link java.sql.ResultSet#next()}, so you will have to do that yourself
	 * before passing the ResultSet to populate().
	 * </p>
	 * 
	 * @param rs
	 *            The {@link java.sql.ResultSet ResultSet} with which to
	 *            populate the Persistable object
	 */
	public void populate(Object object, ResultSet rs)
	{
		Util.populate(object, getMetaData(object), rs);
	}


	/**
	 * Populate the specified object from the record with the specified id.
	 * Handy shortcut if the id consists of a single int or Integer field.
	 * 
	 * @param object
	 *            The object to populate.
	 * @param id
	 *            The id of the record to load.
	 * @param fields
	 *            Zero or more persistent fields belonging to the specified
	 *            object. If you pass zero fields (or null), all persistent
	 *            fields will be populated, otherwise only those you specify.
	 * 
	 * @return true if a row was found, false otherwise
	 */
	public <T> T load(T object, int id, String... fields)
	{
		Util.setId(object, getMetaData(object), id);
		return load(object, fields);
	}


	/**
	 * Populate the specified object from the record with the specified id.
	 * Handy shortcut if the id consists of a single long or Long field.
	 * 
	 * @param object
	 *            The object to populate.
	 * @param id
	 *            The id of the record to load.
	 * @param fields
	 *            Zero or more persistent fields belonging to the specified
	 *            object. If you pass zero fields (or null), all persistent
	 *            fields will be populated, otherwise only those you specify.
	 * 
	 * @return true if a row was found, false otherwise
	 */
	public <T> T load(T object, long id, String... select)
	{
		Util.setId(object, getMetaData(object), id);
		return load(object, select);
	}


	/**
	 * Populate the specified object with the record identified by the object's
	 * id. Example:
	 * 
	 * <pre>
	 * Employee emp = new Employee();
	 * emp.setId(10);
	 * StandardPersister persister = new StandardPersister();
	 * persister.load(emp);
	 * </pre>
	 * 
	 * @param select
	 *            Zero or more persistent properties of the Persistable object.
	 *            If you don't specify any property, <i>all</i> persistent
	 *            properties will be SELECTed, otherwise only the properties you
	 *            specify.
	 * 
	 * @return true if a row was found, false otherwise
	 */
	public <T> T load(T object, String... select)
	{
		return spd.load(object, getMetaData(object), select);
	}


	/**
	 * Populate the Persistable object with the record containing the specified
	 * key-value pair. If you specify a property (for the key parameter) that
	 * does not map to a UNIQUE column, the first row coming back from the
	 * database is used to populate the Persistable object. Example:
	 * 
	 * <pre>
	 * // Employee implements Persistable
	 * Employee emp = new Employee();
	 * emp.getPersister().loadWithKeyValue(&quot;name&quot;, &quot;Smith&quot;);
	 * </pre>
	 * 
	 * @param key
	 *            The name of a persistent property.
	 * @param value
	 *            The value to search for.
	 * @param select
	 *            Zero or more persistent properties of the Persistable object.
	 *            If you don't specify any property, <i>all</i> persistent
	 *            properties will be SELECTed, otherwise only the properties you
	 *            specify.
	 * 
	 * @return true if a row was found, false otherwise
	 */
	public <T> T loadWithKeyValue(T object, String key, Object value, String... select)
	{
		return spd.loadUsingKeyWithValue(object, getMetaData(object), key, value, select);
	}


	public <T> T loadWithKeyValue(T object, String[] key, Object[] value, String... select)
	{
		return spd.loadUsingKeyWithValue(object, getMetaData(object), key, value, select);
	}


	public <T> T loadParent(Object object, T parent, String... foreignKey)
	{
		return spd.loadParent(object, getMetaData(object), parent, getMetaData(parent), foreignKey);
	}


	public <T> T loadParent(Object object, T parent, String[] select, String... foreignKey)
	{
		return spd.loadParentUsingKey(object, getMetaData(object), parent, getMetaData(parent), foreignKey, select);
	}


	public <T> Cursor<T> loadAll(Class<T> clazz, String... selectFields)
	{
		return query(clazz, new QuerySpec().select(selectFields));
	}


	public <T> Cursor<T> query(Class<T> clazz, Condition condition, String... select)
	{
		return query(clazz, new QuerySpec().select(select).where(condition));
	}


	public <T> Cursor<T> query(Class<T> clazz, QuerySpec qs)
	{
		return spd.query(getMetaData(clazz), qs);
	}


	public <T> Cursor<T> query(Class<T> clazz, String sql, String[] parameters, Object[] values)
	{
		Query<T> query = Query.createQuery(this, sql);
		if (query.isNew()) {
			QuerySpec qs = new QuerySpec();
			qs.bind(parameters, values);
			query.initialize(sql, getMetaData(clazz), qs);
		}
		return query.createCursor();
	}


	public <T> Cursor<T> loadChildren(Object object, Class<T> childClass, String... fkFields)
	{
		return spd.loadChildren(object, getMetaData(object), getMetaData(childClass), fkFields);
	}


	public <T> Cursor<T> loadChildren(Object object, Class<T> childClass, QuerySpec querySpec, String... fkFields)
	{
		return spd.loadChildren(object, getMetaData(object), getMetaData(childClass), querySpec, fkFields);
	}


	//	public <T> Cursor<T> loadManyToMany(Object object, Class<?> intersectionClass, Class<T> targetClass)
	//	{
	//		return loadManyToMany(object, intersectionClass, targetClass);
	//	}
	//
	//
	//	public <T> Cursor<T> loadManyToMany(Object object, Class<?> intersection, Class<T> target, QuerySpec querySpec, String[] fkToSource, String[] fkToTarget)
	//	{
	//		MetaData<?> metadata = getMetaData(object);
	//		MetaData<?> intersectionMetaData = getMetaData(intersection);
	//		MetaData<T> targetMetaData = getMetaData(target);
	//		Query<T> query = generator.sqlManyToMany(metadata, intersectionMetaData, targetMetaData, fkToSource, fkToTarget, querySpec);
	//		query.bind(querySpec).bind(object, metadata.getPrimaryKeyFields());
	//		return query.createCursor();
	//	}

	public <T> int count(Class<T> clazz)
	{
		return spd.count(getMetaData(clazz));
	}


	public <T> int count(Class<T> clazz, QuerySpec qs)
	{
		return spd.count(getMetaData(clazz), qs);
	}


	public <T> int countChildren(Object object, Class<T> childClass, String... foreignKey)
	{
		return spd.countChildren(object, getMetaData(object), getMetaData(childClass), foreignKey);
	}


	public <T> int countChildren(Object object, Class<T> childClass, QuerySpec qs, String... foreignKey)
	{
		return spd.countChildren(object, getMetaData(object), getMetaData(childClass), qs, foreignKey);
	}


	public <T> void save(T object)
	{
		spd.save(object, getMetaData(object));
	}


	public <T> boolean update(T object, String... fields)
	{
		return spd.update(object, getMetaData(object), fields);
	}


	public <T> int update(Class<T> clazz, QuerySpec qs)
	{
		return spd.update(getMetaData(clazz), qs);
	}


	public <T> int delete(T object)
	{
		return spd.delete(object, getMetaData(object));
	}


	public <T> int delete(Class<T> clazz, Condition condition)
	{
		return delete(clazz, new QuerySpec().where(condition));
	}


	public <T> int delete(Class<T> clazz, QuerySpec qs)
	{
		return spd.delete(getMetaData(clazz), qs);
	}


	public <T> int deleteChildren(QuerySpec querySpec, Class<T> childClass)
	{
		//return child.deleteByKey(querySpec, child.getForeignKeyProperties(persistable.getClass()));
		return 0;
	}


	public <T> T loadUsingKey(T object, String[] key, String... select)
	{
		return spd.loadUsingKey(object, getMetaData(object), key, select);
	}


	private <T> MetaData<T> getMetaData(T forObject)
	{
		@SuppressWarnings("unchecked")
		Class<T> clazz = (Class<T>) forObject.getClass();
		return getMetaData(clazz);
	}


	private <T> MetaData<T> getMetaData(Class<T> forClass)
	{
		return ctx.createMetaData(forClass);
	}
}
