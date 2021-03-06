package org.domainobject.orm.core;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.domainobject.orm.bind.DefaultBinderRepository;
import org.domainobject.orm.bind.IBinder;
import org.domainobject.orm.bind.IBinderRepository;
import org.domainobject.orm.exception.MetaDataAssemblyException;
import org.domainobject.orm.exception.MissingBinderException;
import org.domainobject.orm.map.IMappingAlgorithm;
import org.domainobject.orm.map.PassThruMappingAlgorithm;

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
public class MetaDataConfigurator<T> {

	private static final String ERR_ALREADY_CREATED = "You cannot change a MetaDataConfigurator after you have called its createMetaData method";

	private final Context context;
	private final Class<T> forClass;

	private IBinderRepository binderRepository;
	private IMappingAlgorithm mappingAlgorithm;

	private String entitySchema;

	private Map<String, IBinder> fieldBinders;
	private Map<Class<?>, IBinder> classBinders;

	private boolean invalid;

	MetaDataConfigurator(Class<T> forClass, Context context)
	{
		this.context = context;
		this.forClass = forClass;
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
		if (mappingAlgorithm == null)
			mappingAlgorithm = new PassThruMappingAlgorithm();

		Connection conn = context.connection;
		InformationSchema is = context.objectFactory.createInformationSchema(conn);

		String entityName = mappingAlgorithm.mapClassToEntityName(forClass);
		if (entitySchema == null)
			entitySchema = is.getDatabaseSchema();

		int numEntities = is.countEntities(entityName, entitySchema);

		if (numEntities > 1) {
			throw new MetaDataAssemblyException("Ambiguous target entity");
		}
		else if (numEntities == 1) {
			Column[] columns = is.getColumns(entityName, entitySchema);
			DataExchangeUnit[] deus = getDataExchangeUnits(columns);
		}
		else {
			String sql = context.namedQueries.get(entityName);
			if (sql == null) {
				throw new MetaDataAssemblyException("Unmappable class");
			}
		}

		MetaData<T> metadata = null;
		context.metadataCache.put(forClass, metadata);
		return metadata;
	}

	String getEntitySchema()
	{
		return entitySchema;
	}

	/**
	 * Set the database schema that the table or view belongs to. By default
	 * this is inferred from {@link Connection#getCatalog()}.
	 * 
	 * @param schema
	 *            The database schema
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setEntitySchema(String schema)
	{
		this.entitySchema = schema;
		return this;
	}

	AbstractEntity createEntity()
	{
		Connection conn = context.connection;
		InformationSchema is = context.objectFactory.createInformationSchema(conn);
		String name = getMappingAlgorithm().mapClassToEntityName(forClass);
		if (entitySchema == null)
			entitySchema = is.getDatabaseSchema();
		int numEntities = is.countEntities(name, entitySchema);
		if (numEntities > 1) {
			String fmt = "Multiple database entities found with name \"%s\"";
			String msg;
			if (entitySchema == null) {
				msg = String.format(fmt, name);
			}
			else {
				fmt += " and schema \"%s\"";
				msg = String.format(fmt, name, entitySchema);
			}
			throw new MetaDataAssemblyException(msg);
		}
		else if (numEntities == 1) {
			Column[] columns = is.getColumns(name, entitySchema);
		}
		AbstractEntity entity = new AbstractEntity();
		entity.setName(name);
		entity.setSchema(entitySchema);

		return entity;
	}

	/**
	 * Set the mapping algorithm to use when mapping fields to columns.
	 * 
	 * @param mappingAlgorithm
	 *            The mapping algorithm
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setMappingAlgorithm(IMappingAlgorithm mappingAlgorithm)
	{
		this.mappingAlgorithm = mappingAlgorithm;
		return this;
	}

	public IMappingAlgorithm getMappingAlgorithm()
	{
		if (mappingAlgorithm == null)
			return new PassThruMappingAlgorithm();
		return mappingAlgorithm;
	}

	/**
	 * Set the binder repository when assigning {@code Binder}s to fields.
	 * 
	 * @param repository
	 *            The binder repository
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setBinderRepository(IBinderRepository repository)
	{
		this.binderRepository = repository;
		return this;
	}

	/**
	 * Sets the {@link IBinder} to be used for all fields of the the specified
	 * type. The binder specified here takes precedence over the binder
	 * specified by the {@link IBinderRepository}.
	 * 
	 * @param type
	 *            The class to attach the binder to
	 * @param binder
	 *            The binder
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setBinder(Class<?> type, IBinder binder)
	{
		if (classBinders == null) {
			classBinders = new HashMap<>();
		}
		classBinders.put(type, binder);
		return this;
	}

	/**
	 * Sets the {@link IBinder} to be used for the specified field. The binder
	 * specfied here takes precedence over binders specified using
	 * {@link #setBinder(Class, IBinder)}.
	 * 
	 * @param field
	 *            The field to which to assign the {@code Binder}.
	 * @param binder
	 *            The {@code Binder} to be used for the field
	 * 
	 * @return This metadata configurator instance
	 */
	public MetaDataConfigurator<T> setBinder(String field, IBinder binder)
	{
		if (fieldBinders == null)
			fieldBinders = new HashMap<>();
		fieldBinders.put(field, binder);
		return this;
	}

	DataExchangeUnit getDataExchangeUnit(Column column)
	{
		for (Field f : getAllFields(forClass)) {
			String colName = mappingAlgorithm.mapFieldToColumnName(f, forClass);
			if (colName != null && colName.equals(column)) {
				if (!f.isAccessible())
					f.setAccessible(true);
				return new DataExchangeUnit(f, column, getBinder(f));
			}
		}
		return null;
	}

	DataExchangeUnit[] getDataExchangeUnits(Column[] columns)
	{
		HashMap<String, Column> columnIndex = new HashMap<>((int) (columns.length / .75) + 1);
		for (Column column : columns) {
			columnIndex.put(column.getName(), column);
		}

		ArrayList<DataExchangeUnit> list = new ArrayList<>(columns.length);

		for (Field f : getAllFields(forClass)) {
			String columnName = mappingAlgorithm.mapFieldToColumnName(f, forClass);
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

	private IBinder getBinder(Field field)
	{
		String name = field.getName();
		if (fieldBinders != null && fieldBinders.containsKey(name)) {
			return fieldBinders.get(name);
		}
		Class<?> type = field.getType();
		if (classBinders != null && classBinders.containsKey(name)) {
			return classBinders.get(name);
		}
		if (binderRepository == null) {
			binderRepository = DefaultBinderRepository.getSharedInstance();
		}
		IBinder binder = binderRepository.getBinder(type);
		if (binder == null)
			throw new MissingBinderException(field);
		return binder;
	}

	private static List<Field> getAllFields(Class<?> forClass)
	{
		List<Field> fields = new ArrayList<>();
		while (forClass != Object.class) {
			fields.addAll(Arrays.asList(forClass.getDeclaredFields()));
			forClass = forClass.getSuperclass();
		}
		return fields;
	}

}
