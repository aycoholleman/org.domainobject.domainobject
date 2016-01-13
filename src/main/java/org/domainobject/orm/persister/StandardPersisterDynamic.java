package org.domainobject.orm.persister;

import java.lang.reflect.Field;

import org.domainobject.orm.core.Condition;
import org.domainobject.orm.core.Cursor;
import org.domainobject.orm.core.MetaData;
import org.domainobject.orm.core.Query;
import org.domainobject.orm.core.QuerySpec;
import org.domainobject.orm.exception.MetaDataAssemblyException;
import org.domainobject.orm.exception.ReflectionException;
import org.domainobject.orm.exception.UnmappedFieldException;
import org.domainobject.orm.generator.StandardQueryGenerator;
import org.domainobject.orm.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardPersisterDynamic {

	static Logger logger = LoggerFactory.getLogger("org.domainobject");


	private StandardQueryGenerator generator;


	public StandardPersisterDynamic(StandardQueryGenerator generator)
	{
		this.generator = generator;
	}


	public void setQueryGenerator(StandardQueryGenerator generator)
	{
		this.generator = generator;
	}


	/**
	 * Populate the specified object from a record identified by the specified
	 * id. Convenient method if the id (primary key) consists of a single int or
	 * Integer field.
	 * 
	 * @param object
	 *            A persistent object
	 * @param metadata
	 *            The metadata for the persistent object
	 * @param id
	 *            The id (primary key) of the record to
	 * @param select
	 *            Zero or more fields to populate. If you pass zero fields (or
	 *            null), all persistent fields will be populated, otherwise only
	 *            those you specify
	 * 
	 * @return The object that you passed as the first argument, or null if no
	 *         record with the specified id was found
	 */
	public <T> T load(T object, MetaData<T> metadata, int id, String... select)
	{
		Util.setId(object, metadata, id);
		return load(object, metadata, select);
	}


	/**
	 * Populate the specified object from a record identified by the specified
	 * id. Convenient method if the id (primary key) consists of a single long
	 * or Long field.
	 * 
	 * @param object
	 *            A persistent object
	 * @param metadata
	 *            The metadata for the persistent object
	 * @param id
	 *            The id (primary key) of the record to
	 * @param select
	 *            Zero or more fields to populate. If you pass zero fields (or
	 *            null), all persistent fields will be populated, otherwise only
	 *            those you specify.
	 * 
	 * @return The object that you passed as the first argument, or null if no
	 *         record with the specified id was found
	 */
	public <T> T load(T object, MetaData<T> metadata, long id, String... select)
	{
		Util.setId(object, metadata, id);
		return load(object, metadata, select);
	}


	/**
	 * Populate the specified object from a record identified by the object's
	 * id. For example:
	 * 
	 * <pre>
	 * Employee emp = new Employee();
	 * MetaData&lt;Employee&gt; metadata = Context.getDefaultContext().createMetaData(Employee.class);
	 * emp.setId(10);
	 * dynamicPersister.load(emp);
	 * </pre>
	 * 
	 * @param object
	 *            A persistent object
	 * @param metadata
	 *            The metadata for the persistent object
	 * @param select
	 *            Zero or more fields to populate. If you pass zero fields (or
	 *            null), all persistent fields will be populated, otherwise only
	 *            those you specify.
	 * 
	 * @return The object that you passed as the first argument, or null if no
	 *         record with the object's id was found
	 */
	public <T> T load(T object, MetaData<T> metadata, String... select)
	{
		return loadUsingKey(object, metadata, metadata.getPrimaryKeyFieldNames(), select);
	}


	/**
	 * Populate the specified object from a record identified by the specified
	 * key/value pair. If the specified key is not UNIQUE, the first record
	 * coming back from the database will be used to populate the object. For
	 * example:
	 * 
	 * <pre>
	 * Employee emp = new Employee();
	 * MetaData&lt;Employee&gt; metadata = Context.getDefaultContext().createMetaData(Employee.class);
	 * dynamicPersister.loadUsingKey(emp, metadata, &quot;socialSecurityNumber&quot;, &quot;123456789&quot;);
	 * </pre>
	 * 
	 * @param object
	 *            A persistent object
	 * @param metadata
	 *            The metadata for the persistent object
	 * @param key
	 *            A persistent field of the object (supposedly, but not
	 *            necessarily a UNIQUE key)
	 * @param value
	 *            The value of the key
	 * @param select
	 *            Zero or more fields to populate. If you pass zero fields (or
	 *            null), all persistent fields will be populated, otherwise only
	 *            those you specify.
	 * 
	 * @return The object that you passed as the first argument, or null if no
	 *         record for the specified key/value pair
	 */
	public <T> T loadUsingKeyWithValue(T object, MetaData<T> metadata, String key, Object value, String... select)
	{
		setField(object, metadata, key, value);
		return loadUsingKey(object, metadata, new String[] { key }, select);
	}


	/**
	 * Populate the specified object from a record identified by the specified
	 * key/value pairs. If the specified key is not UNIQUE, the first record
	 * coming back from the database will be used to populate the object.
	 * 
	 * @param object
	 *            A persistent object
	 * @param metadata
	 *            The metadata for the persistent object
	 * @param key
	 *            One or more persistent fields of the object (supposedly, but
	 *            not necessarily constituting a UNIQUE key)
	 * @param value
	 *            The value of the key
	 * @param select
	 *            Zero or more fields to populate. If you pass zero fields (or
	 *            null), all persistent fields will be populated, otherwise only
	 *            those you specify.
	 * 
	 * @return The object that you passed as the first argument, or null if no
	 *         record for the specified key/value pair
	 */
	public <T> T loadUsingKeyWithValue(T object, MetaData<T> metadata, String[] key, Object[] value, String... select)
	{
		for (String field : key) {
			setField(object, metadata, field, value);
		}
		return loadUsingKey(object, metadata, key, select);
	}


	/**
	 * Populate the specified object from a record identified by the object's
	 * value for the specified key. If the specified key is not UNIQUE, the
	 * first record coming back from the database will be used to populate the
	 * object. For example:
	 * 
	 * <pre>
	 * Employee emp = new Employee();
	 * MetaData&lt;Employee&gt; metadata = Context.getDefaultContext().createMetaData(Employee.class);
	 * emp.setSocialSecurityNumber(&quot;123456789&quot;);
	 * dynamicPersister.loadUsingKey(emp, metadata, &quot;socialSecurityNumber&quot;);
	 * </pre>
	 * 
	 * @param object
	 *            A persistent object
	 * @param metadata
	 *            The metadata for the persistent object
	 * @param key
	 *            A persistent field of the object (supposedly, but not
	 *            necessarily a UNIQUE key).
	 * @param select
	 *            Zero or more fields to populate. If you pass zero fields (or
	 *            null), all persistent fields will be populated, otherwise only
	 *            those you specify.
	 * @return The object that you passed as the first argument, or null if no
	 *         record for the specified key/value pair
	 */
	public <T> T loadUsingKey(T object, MetaData<T> metadata, String key, String... select)
	{
		QuerySpec qs = new QuerySpec();
		qs.select(select).where(key).setMaxRecords(1);
		Query<T> query = generator.sqlSelect(metadata, qs);
		return query.load(object) ? object : null;
	}


	/**
	 * Populate the specified object from a record identified by the object's
	 * value(s) for the specified key. If the specified key is not UNIQUE, the
	 * first record coming back from the database will be used to populate the
	 * object.
	 * 
	 * @param object
	 *            A persistent object
	 * @param metadata
	 *            The metadata for the persistent object
	 * @param key
	 *            One or more persistent fields of the object (supposedly, but
	 *            not necessarily constituting a UNIQUE key)
	 * @param select
	 *            Zero or more fields to populate. If you pass zero fields (or
	 *            null), all persistent fields will be populated, otherwise only
	 *            those you specify.
	 * 
	 * @return The object that you passed as the first argument, or null if no
	 *         record for the specified key/value pair
	 */
	public <T> T loadUsingKey(T object, MetaData<T> metadata, String[] key, String... select)
	{
		QuerySpec qs = new QuerySpec();
		qs.select(select).where(key).setMaxRecords(1);
		Query<T> query = generator.sqlSelect(metadata, qs);
		return query.load(object) ? object : null;
	}


	/**
	 * Load all records from a table.
	 * 
	 * @param metadata
	 *            The metadata object for the persistent class mapping to the
	 *            table
	 * @param select
	 *            Zero or more fields to populate. If you pass zero fields (or
	 *            null), all persistent fields will be populated, otherwise only
	 *            those you specify.
	 * 
	 * @return A {@code Cursor} allowing you to iterate over the resulting
	 *         objects
	 */
	public <T> Cursor<T> loadAll(MetaData<T> metadata, String... select)
	{
		return query(metadata, new QuerySpec().select(select));
	}


	/**
	 * Load all records from a table satisfying the specified {@link Condition}.
	 * 
	 * @param metadata
	 *            The metadata object for the persistent class mapping to the
	 *            table
	 * @param condition
	 *            The {@code Condition} to be satisfied
	 * @param select
	 *            Zero or more fields to populate. If you pass zero fields (or
	 *            null), all persistent fields will be populated, otherwise only
	 *            those you specify.
	 * 
	 * @return A {@code Cursor} allowing you to iterate over the resulting
	 *         objects
	 */
	public <T> Cursor<T> query(MetaData<T> metadata, Condition condition, String... select)
	{
		return query(metadata, new QuerySpec().select(select).where(condition));
	}


	/**
	 * Load records from a table using the specifications in the specified
	 * {@link QuerySpec}.
	 * 
	 * @param metadata
	 *            The metadata object for the persistent class mapping to the
	 *            table
	 * @param qs
	 *            The {@code QuerySpec}
	 * 
	 * @return A {@code Cursor} allowing you to iterate over the resulting
	 *         objects
	 */
	public <T> Cursor<T> query(MetaData<T> metadata, QuerySpec qs)
	{
		Query<T> query = generator.sqlQuery(metadata, qs);
		return query.createCursor();
	}


	/**
	 * Load records from a table using the specified "free-style" sql query.
	 * 
	 * @param metadata
	 *            The metadata object for the class of objects you want to be
	 *            created.
	 * @param sql
	 *            The query
	 * @param parameters
	 *            The names of the (one and only) parameter occurring in the
	 *            query
	 * @param values
	 *            The value to be bound to the parameter
	 * 
	 * @return A {@code Cursor} allowing you to iterate over the resulting
	 *         objects
	 */
	public <T> Cursor<T> query(MetaData<T> metadata, String sql, String parameter, Object value)
	{
		Query<T> query = Query.createQuery(this, sql);
		if (query.isNew()) {
			QuerySpec qs = new QuerySpec();
			qs.bind(parameter, value);
			query.initialize(sql, metadata, qs);
		}
		return query.createCursor();
	}


	/**
	 * Load records from a table using the specified "free-style" sql query.
	 * 
	 * @param metadata
	 *            The metadata object for the class of objects you want to be
	 *            created.
	 * @param sql
	 *            The query
	 * @param parameters
	 *            The names of the parameters occurring in the query
	 * @param values
	 *            The values to be bound to the parameters
	 * 
	 * @return A {@code Cursor} allowing you to iterate over the resulting
	 *         objects
	 */
	public <T> Cursor<T> query(MetaData<T> metadata, String sql, String[] parameters, Object[] values)
	{
		Query<T> query = Query.createQuery(this, sql);
		if (query.isNew()) {
			QuerySpec qs = new QuerySpec();
			qs.bind(parameters, values);
			query.initialize(sql, metadata, qs);
		}
		return query.createCursor();
	}


	/**
	 * Load the relational parent of the specified object. The parent is looked
	 * up using the values of the foreign key fields in the specified object.
	 * Therefore these fields must be set prior to calling this method. Example:
	 * 
	 * <pre>
	 * MetaData&lt;Employee&gt; empMetaData = Context.getDefaultContext().createMetaData(Employee.class);
	 * MetaData&lt;Department&gt; deptMetaData = Context.getDefaultContext().createMetaData(Department.class);
	 * Employee emp = dynamicPersister.load(new Employee(), empMetaData, 10);
	 * Department dept = dynamicPersister.loadParent(emp, deptMetaData, new Department(), deptMetaData);
	 * </pre>
	 * 
	 * @param object
	 *            A persistent object whose parent you want to load. The
	 *            field(s) constituting the foreign key to the parent must be
	 *            set.
	 * @param metadata
	 *            The metadata object for the persistent object whose parent you
	 *            want to load.
	 * @param parent
	 *            An instance of the parent you want to get populated from a
	 *            record in the database
	 * @param parentMetaData
	 *            The metadata object for the parent object
	 * @param select
	 *            Zero or more fields <i>in the parent object</i> to populate.
	 *            If you pass zero fields (or null), all persistent fields will
	 *            be populated, otherwise only those you specify.
	 * 
	 * @return The parent object (the 3rd argument that you passed to this
	 *         method), now populated with data from the database, or null if
	 *         the parent record was not found.
	 */
	public <T> T loadParent(Object object, MetaData<?> metadata, T parent, MetaData<T> parentMetaData, String... select)
	{
		String[] foreignKey = metadata.getForeignKeyFieldNames(parentMetaData);
		Object[] fkValues = Util.read(object, metadata, foreignKey);
		Util.setId(parent, parentMetaData, fkValues);
		return select == null ? load(parent, parentMetaData) : load(parent, parentMetaData, select);
	}


	/**
	 * Load the relational parent of the specified object. You must specify
	 * yourself which field(s) in the child object represent the foreign key to
	 * the parent object. If you specify null or a zero-length array, a
	 * {@link MetaDataAssemblyException} will be thrown.
	 * 
	 * @param object
	 *            A persistent object whose parent you want to load. The
	 *            field(s) constituting the foreign key to the parent must be
	 *            set.
	 * @param metadata
	 *            The metadata object for the persistent object whose parent you
	 *            want to load.
	 * @param parent
	 *            An instance of the parent you want to get populated from a
	 *            record in the database
	 * @param parentMetaData
	 *            The metadata object for the parent object
	 * @param foreignKey
	 *            The field(s) in the child object (the first argument to this
	 *            method)
	 * @param select
	 *            Zero or more fields <i>in the parent object</i> to populate.
	 *            If you pass zero fields (or null), all persistent fields will
	 *            be populated, otherwise only those you specify.
	 * 
	 * @return The parent object (the 3rd argument that you passed to this
	 *         method), now populated with data from the database, or null if
	 *         the parent record was not found.
	 */
	public <T> T loadParentUsingKey(Object object, MetaData<?> metadata, T parent, MetaData<T> parentMetaData, String[] foreignKey, String... select)
	{
		if (foreignKey == null || foreignKey.length == 0) {
			throw new MetaDataAssemblyException("No foreign key provided");
		}
		Object[] fkValues = Util.read(object, metadata, foreignKey);
		Util.setId(parent, parentMetaData, fkValues);
		return select == null ? load(parent, parentMetaData) : load(parent, parentMetaData, select);
	}


	public <T> Cursor<T> loadChildren(Object object, MetaData<?> metadata, MetaData<T> childMetaData, String... fkFields)
	{
		return loadChildren(object, metadata, childMetaData, new QuerySpec(), fkFields);
	}


	public <T> Cursor<T> loadChildren(Object object, MetaData<?> metadata, MetaData<T> childMetaData, QuerySpec qs, String... fkFields)
	{
		if (fkFields == null || fkFields.length == 0) {
			fkFields = childMetaData.getForeignKeyFieldNames(metadata);
		}
		qs.where(fkFields, Util.getId(object, metadata));
		return query(childMetaData, qs);
	}


	public <T> int count(MetaData<T> metadata)
	{
		return count(metadata, QuerySpec.EMPTY);
	}


	public <T> int count(MetaData<T> metadata, QuerySpec qs)
	{
		Query<T> query = generator.sqlCountStar(metadata, qs);
		return query.fetchInt();
	}


	public <T> int countChildren(Object object, MetaData<?> metadata, MetaData<T> childMetaData, String... foreignKey)
	{
		return countChildren(object, metadata, childMetaData, new QuerySpec(), foreignKey);
	}


	public <T> int countChildren(Object object, MetaData<?> metadata, MetaData<T> childMetaData, QuerySpec qs, String... foreignKey)
	{
		if (foreignKey == null || foreignKey.length == 0) {
			foreignKey = childMetaData.getForeignKeyFieldNames(metadata);
		}
		qs.where(foreignKey, Util.getId(object, metadata));
		return count(childMetaData, qs);
	}


	public <T> void save(T object, MetaData<T> metadata)
	{
		Query<T> query = generator.insert(metadata);
		query.insert(object);
	}


	public <T> boolean update(T object, MetaData<T> metadata, String... fields)
	{
		if (fields == null || fields.length == 0) {
			fields = metadata.getAttributeFieldNames();
		}
		QuerySpec qs = new QuerySpec();
		qs.update(fields);
		qs.where(metadata.getPrimaryKeyFieldNames());
		Query<T> query = generator.sqlUpdate(metadata, qs);
		System.out.println(query.getGeneratedSQL());
		System.out.println(query.getSubmittedSQL());
		return query.update(object);
	}


	public <T> int update(MetaData<T> metadata, String field, Object value, Condition condition)
	{
		return update(metadata, new String[] { field }, new Object[] { value }, condition);
	}


	public <T> int update(MetaData<T> metadata, String[] fields, Object[] values, Condition condition)
	{
		QuerySpec qs = new QuerySpec();
		qs.update(fields, values).where(condition);
		return update(metadata, qs);
	}


	public <T> int update(MetaData<T> metadata, QuerySpec qs)
	{
		Query<T> query = generator.sqlUpdate(metadata, qs);
		return query.executeUpdate();
	}


	public <T> int delete(T object, MetaData<T> metadata)
	{
		String[] pkFields = metadata.getPrimaryKeyFieldNames();
		Object[] pkValues = Util.getId(object, metadata);
		QuerySpec qs = new QuerySpec();
		qs.where(pkFields, pkValues);
		return delete(metadata, qs);
	}


	public <T> int delete(MetaData<T> metadata, QuerySpec qs)
	{
		Query<T> query = generator.sqlDelete(metadata, qs);
		return query.executeUpdate();
	}


	private <T> void setField(T object, MetaData<T> metadata, String fieldName, Object value)
	{
		Field field = metadata.getDataExchangeUnit(fieldName).getField();
		if (field == null) {
			throw new UnmappedFieldException(object.getClass(), fieldName);
		}
		try {
			field.set(this, value);
		}
		catch (IllegalArgumentException e) {
			throw new ReflectionException(e);
		}
		catch (IllegalAccessException e) {
			throw new ReflectionException(e);
		}
	}
}
