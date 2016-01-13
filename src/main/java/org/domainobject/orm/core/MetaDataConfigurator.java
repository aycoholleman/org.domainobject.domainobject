package org.domainobject.orm.core;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.domainobject.orm.bind.Binder;
import org.domainobject.orm.bind.BinderRepository;
import org.domainobject.orm.core.Entity.Type;
import org.domainobject.orm.exception.DomainObjectSQLException;
import org.domainobject.orm.exception.MetaDataAssemblyException;
import org.domainobject.orm.map.MappingAlgorithm;

/**
 * <p>
 * A metadata configurator gives you fine-grained control over the configuration
 * of a metadata object. You cannot instantiate a metadata configurator
 * directly. You obtain one from a metadata factory (see
 * {@link Context#getConfigurator(Class)}. A metadata configurator lets you
 * override any or all of the factory settings using a fluent interface, for
 * example:
 * 
 * <pre>
 * 	Context factory = new Context(...);
 * 	MetaDataConfigurator configurator = factory.getConfigurator(Employee.class);
 * 	MetaData employeeMetaData = configurator
 * 		.setConnection(myJDBCConnection)
 * 		.setMappingAlgorithm(new UpperCaseMappingAlgorithm())
 * 		.setCustomMapping("wages", "SALARY")
 * 		.setFieldBinder("wages", new MyOwnBigDecimalBinder())
 * 		.createMetaData();
 * </pre>
 * 
 * </p>
 * <p>
 * Metadata configurators should be treated as throw-away objects. After you
 * have called its {@link #createMetaData()} method, it should be dispensed
 * with.
 * </p>
 * 
 * @see Context#getConfigurator(Class)
 */
public final class MetaDataConfigurator<T> {

	private static final String ERR_ALREADY_CREATED = "You cannot change a MetaDataConfigurator after you have called its createMetaData method";
	private static final String ERR_ENTITY_ALREADY_SET = "You must either provide a complete Entity instance, or specify one through its name, schema and type";

	private final Context context;
	private final Class<T> forClass;

	private Connection connection;
	private BinderRepository binderRepository;
	private MappingAlgorithm mappingAlgorithm;

	private Entity entity;
	private String entityName;
	private String entitySchema;
	private Type entityType;

	private Map<String, String> mappings;
	private Map<String, Binder> fieldBinders;
	private Map<Class<?>, Binder> classBinders;

	private boolean invalid;


	MetaDataConfigurator(Class<T> forClass, Context context)
	{
		this.context = context;
		this.forClass = forClass;
		this.connection = context.connection;
		this.binderRepository = context.binderRepository;
		this.mappingAlgorithm = context.mappingAlgorithm;
	}


	/**
	 * Create a metadata object for the persistent class for which this
	 * configurator was created. See {@link Context#getConfigurator(Class)}.
	 * Before returning the metadata object to the caller, it is added to a
	 * metadata cache maintained by the factory from which you got this
	 * configurator. Note, though, that this method does not check this cache
	 * itself before creating a new metadata object. Since metadata assembly is
	 * a heavy-weight process, you should always first call
	 * {@link Context#findMetaData(Class)} before calling this method. It is not
	 * strictly an error to call {@code createMetaData} more than once on the
	 * same configurator, but it is pointless and expensive.
	 * 
	 * @return The metadata object
	 */
	public MetaData<T> createMetaData()
	{
		if (invalid) {
			throw new IllegalAccessError(ERR_ALREADY_CREATED);
		}
		invalid = true;
		if (connection == null) {
			throw new MetaDataAssemblyException("Cannot create metadata object without a JDBC connection");
		}
		if (entity == null) {
			entity = createEntity();
		}
		MetaData<T> metadata = new MetaData<T>(forClass, entity, getDataExchangeUnits(), context, this);
		context.cache.put(forClass, metadata);
		return metadata;
	}


	/**
	 * Set the JDBC connection to be used by the persistency operations.
	 * 
	 * @param connection
	 *            The JDBC connection
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setConnection(Connection connection)
	{
		if (invalid) {
			throw new IllegalAccessError(ERR_ALREADY_CREATED);
		}
		this.connection = connection;
		return this;
	}


	/**
	 * Set the {@code Entity} representing the table, view or nested query that
	 * the persistent class maps to.
	 * 
	 * @param entity
	 *            The {@code Entity} representing the table
	 * 
	 * @return This metadata configurator instance
	 * 
	 * @see NestedQueryEntity
	 */
	public MetaDataConfigurator<T> setEntity(Entity entity)
	{
		if (entityName != null || entitySchema != null || entityType != null) {
			throw new MetaDataAssemblyException(ERR_ENTITY_ALREADY_SET);
		}
		this.entity = entity;
		return this;
	}


	/**
	 * Set the name of the entity that representing the table, view or nested
	 * query that the persistent class maps to.
	 * 
	 * @param name
	 *            The name of the entity
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setEntityName(String name)
	{
		if (invalid) {
			throw new IllegalAccessError(ERR_ALREADY_CREATED);
		}
		if (entity != null) {
			throw new MetaDataAssemblyException(ERR_ENTITY_ALREADY_SET);
		}
		this.entityName = name;
		return this;
	}


	/**
	 * Set the database schema of the entity representing the table, view or
	 * nested query that the persistent class maps to.
	 * 
	 * @param schema
	 *            The database schema
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setEntitySchema(String schema)
	{
		if (invalid) {
			throw new IllegalAccessError(ERR_ALREADY_CREATED);
		}
		if (entity != null) {
			throw new MetaDataAssemblyException(ERR_ENTITY_ALREADY_SET);
		}
		this.entitySchema = schema;
		return this;
	}


	/**
	 * Set the type of the entity that the persistent class maps to (table, view
	 * or nested query).
	 * 
	 * @param type
	 *            The type of the entity
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setEntityType(Entity.Type type)
	{
		if (invalid) {
			throw new IllegalAccessError(ERR_ALREADY_CREATED);
		}
		if (entity != null) {
			throw new MetaDataAssemblyException(ERR_ENTITY_ALREADY_SET);
		}
		if(type == Entity.Type.NESTED_QUERY) {
			throw new MetaDataAssemblyException("");
		}
		this.entityType = type;
		return this;
	}


	/**
	 * Set the mapping algorithm to use when mapping fields to columns.
	 * 
	 * @param mappingAlgorithm
	 *            The mapping algorithm
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setMappingAlgorithm(MappingAlgorithm mappingAlgorithm)
	{
		if (invalid) {
			throw new IllegalAccessError(ERR_ALREADY_CREATED);
		}
		this.mappingAlgorithm = mappingAlgorithm;
		return this;
	}


	/**
	 * Set the binder repository when assigning {@code Binder}s to fields.
	 * 
	 * @param binderRepository
	 *            The binder repository
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setBinderRepository(BinderRepository binderRepository)
	{
		if (invalid) {
			throw new IllegalAccessError(ERR_ALREADY_CREATED);
		}
		this.binderRepository = binderRepository;
		return this;
	}


	/**
	 * Specify a custom field-to-column mapping. This method can be used if you
	 * have one or more erratic mappings that cannot be captured by your chosen
	 * mapping algorithm. Custom mappings take precedence over mappings
	 * calculated by a mapping algortihm.
	 * 
	 * @param field
	 *            The name of a field (belonging to the class described by the
	 *            metadata object)
	 * @param column
	 *            The name of the column that the field maps to
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setCustomMapping(String field, String column)
	{
		if (invalid) {
			throw new IllegalAccessError(ERR_ALREADY_CREATED);
		}
		if (mappings == null) {
			mappings = new HashMap<String, String>();
		}
		mappings.put(field, column);
		return this;
	}


	/**
	 * Set multiple field-to-column mappings. This method can be used if you
	 * have several erratic mappings that cannot be captured by your chosen
	 * mapping algorithm. Custom mappings take precedence over mappings
	 * calculated by a mapping algortihm.
	 * 
	 * @param mappings
	 *            A map with field names as keys and column names as values
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setCustomMappings(Map<String, String> mappings)
	{
		if (invalid) {
			throw new IllegalAccessError(ERR_ALREADY_CREATED);
		}
		if (this.mappings == null) {
			this.mappings = mappings;
		}
		else {
			this.mappings.putAll(mappings);
		}
		return this;
	}


	/**
	 * Specify the {@link Binder} to be used for the specified field. This way
	 * of setting a binder takes precedence over all other ways of specifying
	 * {@code Binder}s.
	 * 
	 * @param field
	 *            The field to which to assign the {@code Binder}.
	 * @param binder
	 *            The {@code Binder} to be used for the field
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setFieldBinder(String field, Binder binder)
	{
		if (invalid) {
			throw new IllegalAccessError(ERR_ALREADY_CREATED);
		}
		if (fieldBinders == null) {
			fieldBinders = new HashMap<String, Binder>();
		}
		fieldBinders.put(field, binder);
		return this;
	}


	/**
	 * Provide {@link Binder}s for multiple fields. This way of setting a binder
	 * takes precedence over all other ways of specifying {@code Binder}s.
	 * 
	 * @param fieldBinders
	 *            A map with field names as keys and {@code Binder} objects as
	 *            values.
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setFieldBinders(Map<String, Binder> fieldBinders)
	{
		if (invalid) {
			throw new IllegalAccessError(ERR_ALREADY_CREATED);
		}
		if (this.fieldBinders == null) {
			this.fieldBinders = fieldBinders;
		}
		else {
			this.fieldBinders.putAll(fieldBinders);
		}
		return this;
	}


	/**
	 * Specify the {@link Binder} to be used for the specified class. All fields
	 * with the specified class will be assigned that {@code Binder}.
	 * {@code Binder}s specified this way take precedence over binders retrieved
	 * from a {@link BinderRepository}.
	 * 
	 * @param forClass
	 *            The class to attach the binder to
	 * @param binder
	 *            The binder
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setBinder(Class<?> forClass, Binder binder)
	{
		if (invalid) {
			throw new IllegalAccessError(ERR_ALREADY_CREATED);
		}
		if (classBinders == null) {
			classBinders = new HashMap<Class<?>, Binder>();
		}
		classBinders.put(forClass, binder);
		return this;
	}


	/**
	 * Set the {@link Binder}s to be used for a set of classes. {@code Binder}s
	 * specified this way take precedence over binders retrieved from a
	 * {@link BinderRepository}.
	 * 
	 * @param classBinders
	 *            A map with class objects as keys and binder objects as values
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setBinders(Map<Class<?>, Binder> classBinders)
	{
		if (invalid) {
			throw new IllegalAccessError(ERR_ALREADY_CREATED);
		}
		if (this.classBinders == null) {
			this.classBinders = classBinders;
		}
		else {
			this.classBinders.putAll(classBinders);
		}
		return this;
	}


	DataExchangeUnit getDataExchangeUnit(Column column)
	{
		for (Field f : getAllFields(forClass)) {
			String columnName = getColumnName(f);
			if (columnName != null && columnName.equals(column)) {
				if (!f.isAccessible()) {
					f.setAccessible(true);
				}
				return new DataExchangeUnit(f, column, getBinder(f));
			}
		}
		return null;
	}


	private Entity createEntity()
	{
		if (entityName == null) {
			if (mappingAlgorithm != null) {
				entityName = mappingAlgorithm.mapClassToEntityName(forClass);
			}
			if (entityName == null) {
				throw new MetaDataAssemblyException("Cannot establish entity name");
			}
		}
		if (entitySchema == null) {
			try {
				entitySchema = connection.getCatalog();
				if (entitySchema == null) {
					throw new MetaDataAssemblyException("Cannot establish database schema");
				}
			}
			catch (SQLException e) {
				throw new DomainObjectSQLException(e);
			}
		}
		if (entityType == null || entityType == Entity.Type.TABLE) {
			return new TableEntity(entityName, entitySchema, connection);
		}
		return new ViewEntity(entityName, entitySchema, connection);
	}


	private DataExchangeUnit[] getDataExchangeUnits()
	{
		Column[] columns = entity.getColumns();
		HashMap<String, Column> columnIndex = new HashMap<String, Column>((int) (columns.length / .75) + 1);
		for (Column column : columns) {
			columnIndex.put(column.getName(), column);
		}

		ArrayList<DataExchangeUnit> list = new ArrayList<DataExchangeUnit>(columns.length);

		for (Field f : getAllFields(forClass)) {
			String columnName = getColumnName(f);
			if (columnName != null && columnIndex.containsKey(columnName)) {
				if (!f.isAccessible()) {
					f.setAccessible(true);
				}
				list.add(new DataExchangeUnit(f, columnIndex.get(columnName), getBinder(f)));
			}
		}
		DataExchangeUnit[] result = list.toArray(new DataExchangeUnit[list.size()]);
		Arrays.sort(result, new Comparator<DataExchangeUnit>() {

			public int compare(DataExchangeUnit d1, DataExchangeUnit d2)
			{
				return d1.getColumn().getOrdinalPosition() - d2.getColumn().getOrdinalPosition();
			}
		});
		return result;
	}


	private String getColumnName(Field forField)
	{
		String name = forField.getName();
		if (mappings != null && mappings.containsKey(name)) {
			return mappings.get(name);
		}
		if (mappingAlgorithm != null) {
			return mappingAlgorithm.mapFieldToColumnName(forField, forClass);
		}
		return null;
	}


	private Binder getBinder(Field field)
	{
		String name = field.getName();
		if (fieldBinders != null && fieldBinders.containsKey(name)) {
			return fieldBinders.get(name);
		}
		Class<?> fieldType = field.getType();
		if (classBinders != null && classBinders.containsKey(name)) {
			return classBinders.get(name);
		}
		if (binderRepository != null) {
			return binderRepository.getBinder(fieldType);
		}
		throw new MetaDataAssemblyException("Cannot assign a Binders without BinderRepository and custom Binders");
	}


	private static List<Field> getAllFields(Class<?> forClass)
	{
		List<Field> fields = new ArrayList<Field>();
		while (forClass != Object.class) {
			fields.addAll(Arrays.asList(forClass.getDeclaredFields()));
			forClass = forClass.getSuperclass();
		}
		return fields;
	}

}
