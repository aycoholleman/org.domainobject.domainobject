package org.domainobject.orm.core;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.domainobject.orm.bind.IBinder;
import org.domainobject.orm.exception.DomainObjectException;
import org.domainobject.orm.exception.MetaDataAssemblyException;
import org.domainobject.orm.exception.UnmappedColumnException;
import org.domainobject.orm.exception.UnmappedFieldException;
import org.domainobject.orm.map.IMappingAlgorithm;
import org.domainobject.orm.persister.StandardPersister;
import org.domainobject.orm.util.Util;
import org.domainobject.util.ArrayUtil;
import org.domainobject.util.convert.Stringifier;

/**
 * <p>
 * A metadata object provides information on how an object can interact with the
 * database.
 * 
 * 
 * 
 * It is consulted by {@link StandardPersister}s when they carry out persistency
 * operations on behalf of a {@code Persistable} object, and by
 * {@link QueryGenerator}s when they generate the SQLString executed during the
 * persistency operations. Also, each metadata object maintains a cache of
 * {@link java.sql.PreparedStatement}s so that for each persistency operation
 * SQLString generation and {@code PreparedStatement} creation happen only once.
 * You cannot instantiate metadata objects yourself. You request a metadata
 * object from a {@link Context} or a {@link MetaDataConfigurator}. However, you
 * may never have to deal with metadata objects directly in the first place, or
 * only in a fleeting manner (as described in the javadoc page for the
 * {@link Persistable} interface).
 * </p>
 * <h3>Multiple metadata objects per Persistable class</h3>
 * <p>
 * A metadata object is created and valid for one and only one class of
 * {@code Persistable} objects. Also, all instances of that class <i>usually</i>
 * share one and the same metadata object. But in domainobject this is not
 * <i>necessarily</i> so. Multiple metadata objects may be created for, and used
 * by different instances of the same {@code Persistable} class, or by one and
 * the same instance at different times. The {@code Persistable} interface
 * itself does not allow for such a thing; it has a
 * {@link Persistable#getMetaData() getMetaData} method, but no
 * {@code setMetaData} method. But implementations may choose to provide such a
 * method, and {@code DomainObject} actually does. There are some compelling use
 * cases for this:
 * <ol>
 * <li>Suppose you have two identical tables, one in database A and the other in
 * database B, and you want to do some sort of copy operation from A to B. If
 * there can be only one metadata object for an entire class of
 * {@code Persistable} objects, then for each {@code Persistable} object
 * representing a record in the source table, you will have to transfer its
 * persistent state to another {@code Persistable} object that is able to
 * persist that state to the target table. If, on the other hand, you can change
 * a {@code Persistable} object's metadata on-the-fly, you can first initialize
 * its state with a record from the source table, then change over to a metadata
 * object that "points" to the target table, and then call the {@code save}
 * operation on the very same {@code Persistable} object. If the source table
 * contains two million records, that saves an awful lot of state transfer.</li>
 * <li>
 * Suppose you have a table, and you define a simple view on that just hides two
 * columns for security reasons. In domainobject these two entities can be
 * serviced by one {@code Persistable} class. Upon instantiation of that class
 * for the view, it is automatically detected that two of its fields will remain
 * unmapped; they become unused fields (at least from a persistency point of
 * view). In JPA, however, you would have no choice but to create two nearly
 * identical classes - with only the {@code @Entity} annotation differentiating
 * the two.</li>
 * <li>
 * You can even have valid reasons to use multiple metadata object for one and
 * the same table. If you want to populate the table from two sources, one of
 * which requires some pretty heavy-weight data transformations, you could
 * create a metadata object with purpose-built {@link IBinder}s for that source,
 * while using a regular metadata object for the other.</li>
 * </ol>
 * <p>
 * <h3>MetaData object IDs</h3>
 * <p>
 * Metadata object creation is a heavy-weight process. Therefore, metadata
 * objects are cached and shared by {@code Persistable} objects belonging to the
 * same class. Only the very first instance of that class causes a metadata
 * object to be created; subsequent instances retrieve that metadata object from
 * the cache. However since multiple metadata objects may be created for one and
 * the same class of {@code Persistable} objects, the class alone does not
 * suffice to identify a metadata object in the cache. Therefore, when creating
 * a metadata object, or when retrieving it from the cache, you must provide an
 * extra identifier. See {@link MetaDataConfigurator#createMetaData(String)} and
 * {@link Context#findMetaData(String, Class)}. The id can be anything you like,
 * but should probably be descriptive of the purpose of the metadata object. For
 * example, if you have a {@code Persistable} class called Employee, and you
 * want to use it both for the EMPLOYEE table and the EMPLOYEE_VIEW view, you
 * could name the respective metadata objects accordingly. Or you could leave
 * the metadata object for the EMPLOYEE table unnamed (i.e. specify null for its
 * id). This would make that metadata object the default metadata object for the
 * Employee class. See below.
 * </p>
 * <p>
 * The id you assign to a metadata object need not be unique in itself. It must
 * only be unique amongst the other metadata objects for the same
 * {@code Persistable} class.
 * </p>
 * <h3>The default metadata object</h3>
 * <p>
 * The default metadata object of a {@code Persistable} class is the one you get
 * when you don't specify an id. In other words, it is the metadata object you
 * get when you call {@link Context#findMetaData(Class)} in stead of
 * {@link Context#findMetaData(String, Class)}; and the metadata object that you
 * create-and-get when you call {@link MetaDataConfigurator#createMetaData()} in
 * stead of {@link MetaDataConfigurator#createMetaData(String)}. If multiple
 * metadata objects have been created for that {@code Persistable} class, it is
 * the one whose id equals null. If there is just one, then that is metadata
 * object you get, whatever its id.
 * </p>
 * <p>
 * If you don't need or don't want to make use of domainobject's facility to let
 * one {@code Persistable} class access multiple tables, or the same table in
 * different ways, then you won't need to create multiple metadata objects for
 * that class. In that case just stick to using unnamed metadata objects and
 * you'll never have to worry whether you got the right one.
 * </p>
 */
public final class MetaData<T> {

	/**
	 * A metadata object's adaptability determines how it will adapt to columns
	 * in {@link java.sql.ResultSet}s for which it has no mapping. A metadata
	 * object only maintains mappings between a persistent class and some
	 * pre-determined entity (e.g. a table or a view). However, persistent
	 * objects may be populated from queries that do not necessarily select
	 * exactly all or just columns from that entity. For example:
	 * 
	 * <pre>
	 * SELECT A.ID, A.NAME, A.BUSINESS_UNIT_ID, COUNT(*) AS NUM_EMPLOYEES
	 * FROM DEPARTMENT A,
	 * EMPLOYEE B
	 * WHERE A.ID = B.DEPARTMENT_ID
	 * GROUP BY A.ID, A.NAME, A.BUSINESS_UNIT_ID
	 * </pre>
	 * 
	 * This query could be used to populate a set of Department objects. But
	 * what to do with the NUM_EMPLOYEES column, which could not possibly have
	 * been mapped when the metadata object for the Department class was
	 * created? If the Department class contains no numEployees field, the
	 * answer is simple: the NUM_EMPLOYEES column in the {@code ResultSet} will
	 * be ignored. If the Department class <i>does</i> contain a numEployees
	 * field, the metadata object's adaptability comes into play. If set to
	 * IGNORE, the NUM_EPLOYEES column will still be ignored. This is the
	 * default adaptability setting. If set to EXTEND, the metadata object's
	 * mapping will temporarily (for the duration of the query) be extended with
	 * the new mapping between numEmployees and NUM_EMPLOYEES. If set to
	 * REBUILD, <i>all</i> mappings will be recalculated for the query, whether
	 * or not there are "alien" columns in the {@code Resultset}.
	 */
	public static enum Adaptability {
		IGNORE, EXTEND
	}

	private final Class<T> forClass;
	private final Entity entity;
	private DataExchangeUnit[] dataExchangeUnits;
	private final Context context;
	private final MetaDataConfigurator<T> config;

	private final HashMap<Field, DataExchangeUnit> byField;
	private final HashMap<String, DataExchangeUnit> byFieldName;
	private final HashMap<String, DataExchangeUnit> byColumnName;

	private Adaptability adaptability;

	private final HashMap<String, Object> props = new HashMap<>();

	MetaData(Class<T> forClass, Entity entity, DataExchangeUnit[] dataExchangeUnits,
			Context context, MetaDataConfigurator<T> configurator)
	{

		this.forClass = forClass;
		this.entity = entity;
		this.dataExchangeUnits = dataExchangeUnits;
		this.context = context;
		this.config = configurator;

		int capacity = (int) (dataExchangeUnits.length / .75) + 1;
		byField = new HashMap<>(capacity);
		byFieldName = new HashMap<>(capacity);
		byColumnName = new HashMap<>(capacity);

		createPermutations();

	}

	public void configure()
	{

		Connection conn = context.connection;
		InformationSchema is = context.objectFactory.createInformationSchema(conn);

		String entityName = config.getMappingAlgorithm().mapClassToEntityName(forClass);
		String entitySchema = is.getCatalog();

		int numEntities = is.countEntities(entityName, entitySchema);
		if (numEntities > 1) {
			throw new MetaDataAssemblyException("Ambiguous target entity");
		}
		else if (numEntities == 1) {
			Column[] columns = is.getColumns(entityName, entitySchema);
			dataExchangeUnits = config.getDataExchangeUnits(columns);
		}
		else {
			String sql = context.namedQueries.get(entityName);
			if (sql == null) {
				throw new MetaDataAssemblyException("Unmappable class");
			}
		}

	}

	/**
	 * Get the {@code Persistable} class for which this metadata object was
	 * created.
	 * 
	 * @return The {@code Persistable} class
	 */
	public Class<T> getForClass()
	{
		return forClass;
	}

	/**
	 * Get the entity (table, view or nested query) that the {@code Persistable}
	 * class maps to.
	 * 
	 * @return The {@code Entity}
	 */
	public Entity getEntity()
	{
		return entity;
	}

	/**
	 * Get the {@code DataExchangeUnit}s for the {@code Persistable} class.
	 * 
	 * @return An array of {@code DataExchangeUnit}s
	 */
	public DataExchangeUnit[] getDataExchangeUnits()
	{
		return dataExchangeUnits;
	}

	public Context getContext()
	{
		return context;
	}

	/**
	 * Get the {@code DataExchangeUnit} for the specified {@code Field}.
	 * 
	 * @param field
	 *            The {@code Field}
	 * 
	 * @return The {@code DataExchangeUnit}
	 */
	public DataExchangeUnit getDataExchangeUnit(Field field)
	{
		return byField.get(field);
	}

	/**
	 * Get the {@code DataExchangeUnit} for the field with the specified name.
	 * 
	 * @param fieldName
	 *            The name of the field.
	 * 
	 * @return The {@code DataExchangeUnit}
	 */
	public DataExchangeUnit getDataExchangeUnit(String fieldName)
	{
		return byFieldName.get(fieldName);
	}

	/**
	 * Get the {@code DataExchangeUnit} for the {@link Column} with the
	 * specified name.
	 * 
	 * @param columnName
	 *            The name of the {@code Column}.
	 * 
	 * @return The {@code DataExchangeUnit}
	 */
	public DataExchangeUnit getDataExchangeUnitForColumn(String columnName)
	{
		return byColumnName.get(columnName);
	}

	private Column[] mappedColumns;

	/**
	 * Get all {@code Column}s that are mapped to a {@code Field}. Note that not
	 * all {@code Field}s of a persistent class need to be mapped to a column
	 * and that some columns may remain unmapped (although all NOT NULL columns
	 * without a DEFAULT value must not remain unmapped). This information is
	 * cached, so you don't need to cache it yourself.
	 * 
	 * 
	 * @see MetaDataConfigurator#setMappingAlgorithm(IMappingAlgorithm)
	 * @see MetaDataConfigurator#setCustomMapping(String, String)
	 * 
	 * @return All {@code Column}s that are mapped to a {@code Field}.
	 */
	public Column[] getMappedColumns()
	{
		if (mappedColumns == null) {
			mappedColumns = new Column[dataExchangeUnits.length];
			for (int i = 0; i < dataExchangeUnits.length; ++i) {
				mappedColumns[i] = dataExchangeUnits[i].getColumn();
			}
		}
		return mappedColumns;
	}

	private String[] mappedColumnNames;

	/**
	 * Get the names of all {@code Column}s that are mapped to a {@code Field}.
	 * This information is cached, so you don't need to cache it yourself.
	 * 
	 * @see #getMappedColumns()
	 * 
	 * @return The names of all {@code Column}s that are mapped to a field.
	 */
	public String[] getMappedColumnNames()
	{
		if (mappedColumnNames == null) {
			mappedColumnNames = new String[dataExchangeUnits.length];
			for (int i = 0; i < dataExchangeUnits.length; ++i) {
				mappedColumnNames[i] = dataExchangeUnits[i].getColumn().getName();
			}
		}
		return mappedColumnNames;
	}

	private Field[] mappedFields;

	/**
	 * Get all fields that map to a column. This information is cached, so you
	 * don't need to cache it yourself.
	 * 
	 * @see #getMappedColumns()
	 * 
	 * @return All fields that map to a column
	 */
	public Field[] getMappedFields()
	{
		if (mappedFields == null) {
			mappedFields = new Field[dataExchangeUnits.length];
			for (int i = 0; i < dataExchangeUnits.length; ++i) {
				mappedFields[i] = dataExchangeUnits[i].getField();
			}
		}
		return mappedFields;
	}

	private String[] mappedFieldNames;

	/**
	 * Get the names of all fields that map to a column. This information is
	 * cached, so you don't need to cache it yourself.
	 * 
	 * @see #getMappedColumns()
	 * 
	 * @return The names of all fields that map to a column
	 */
	public String[] getMappedFieldNames()
	{
		if (mappedFieldNames == null) {
			mappedFieldNames = new String[dataExchangeUnits.length];
			for (int i = 0; i < dataExchangeUnits.length; ++i) {
				mappedFieldNames[i] = dataExchangeUnits[i].getField().getName();
			}
		}
		return mappedFieldNames;
	}

	private String[] primaryKey;

	/**
	 * Get the names of the {@code Field}s that constitute the primary key. This
	 * information is cached, so you don't need to cache it yourself.
	 * 
	 * @return The {@code Field}s constituting the primary key.
	 * 
	 */
	public String[] getPrimaryKeyFieldNames()
	{
		if (primaryKey == null) {
			Column[] columns = getPrimaryKeyColumns();
			primaryKey = new String[columns.length];
			for (int i = 0; i < columns.length; ++i) {
				DataExchangeUnit deu = getDataExchangeUnitForColumn(columns[i].getName());
				if (deu == null) {
					throw new UnmappedColumnException(forClass, columns[i].getName());
				}
				primaryKey[i] = deu.getField().getName();
			}
		}
		return primaryKey;
	}

	/**
	 * Set the names of the {@code Field}s that constitute the primary key. This
	 * may be necessary if the JDBC driver and/or the RDBMS cannot provide this
	 * information (through the RDBMS's data dictionary). Calling this method
	 * suppresses the dynamic, lazily executed lookup of the primary key.
	 * 
	 * @param fields
	 *            The names of the fields constituting the primary key
	 */
	public void setPrimaryKeyFieldNames(String... fields)
	{
		if (this.primaryKey != null) {
			throw new MetaDataAssemblyException("Primary key has already been set for class "
					+ getForClass());
		}
		Column[] columns = new Column[fields.length];
		for (int i = 0; i < fields.length; ++i) {
			String field = fields[i];
			DataExchangeUnit deu = getDataExchangeUnit(field);
			if (deu == null) {
				throw new UnmappedFieldException(getForClass(), field);
			}
			columns[i] = deu.getColumn();
		}
		this.entity.setPrimaryKeyColumns(columns);
		this.primaryKey = fields;
	}

	/**
	 * Get the columns in the primary key. This information is cached, so you
	 * don't need to cache it yourself.
	 * 
	 * @return The columns in the primary key.
	 */
	public Column[] getPrimaryKeyColumns()
	{
		return entity.getPrimaryKeyColumns();
	}

	private String[] primaryKeyColumnnames;

	/**
	 * Get the names of columns in the primary key. This information is cached,
	 * so you don't need to cache it yourself.
	 * 
	 * @return The names of columns in the primary key.
	 */
	public String[] getPrimaryKeyColumnNames()
	{
		if (primaryKeyColumnnames == null) {
			Column[] columns = entity.getPrimaryKeyColumns();
			primaryKeyColumnnames = new String[columns.length];
			for (int i = 0; i < columns.length; ++i) {
				primaryKeyColumnnames[i] = columns[i].getName();
			}
		}
		return primaryKeyColumnnames;
	}

	private Field[] primaryKeyFields;

	/**
	 * Get the {@code Field}s that constitute the primary key of the
	 * {@code Persistable} class. This information is cached, so you don't need
	 * to cache it yourself.
	 * 
	 * @return The {@code Field}s constituting the primary key.
	 * 
	 * @throws UnmappedColumnException
	 *             If one or more columns in the primary key appear to have
	 *             remained unmapped.
	 */
	public Field[] getPrimaryKeyFields()
	{
		if (primaryKeyFields == null) {
			String[] columns = getPrimaryKeyColumnNames();
			primaryKeyFields = new Field[columns.length];
			for (int i = 0; i < columns.length; ++i) {
				DataExchangeUnit deu = getDataExchangeUnitForColumn(columns[i]);
				if (deu == null) {
					throw new UnmappedColumnException(forClass, columns[i]);
				}
				primaryKeyFields[i] = deu.getField();
			}
		}
		return primaryKeyFields;
	}

	private String[] attributeColumns;

	/**
	 * Get all mapped columns that are not part of the primary key. This
	 * information is cached, so you don't need to cache it yourself.
	 * 
	 * @return All mapped columns that are not part of the primary key
	 */
	public String[] getAttributeColumns()
	{
		if (attributeColumns == null) {
			String[] mappedColumnNames = getMappedColumnNames();
			Collection<String> collection = new ArrayList<>(Arrays.asList(mappedColumnNames));
			collection.removeAll(Arrays.asList(getPrimaryKeyColumnNames()));
			attributeColumns = collection.toArray(new String[collection.size()]);
		}
		return attributeColumns;
	}

	private String[] attributeColumnNames;

	/**
	 * Get the names of all mapped columns that are not part of the primary key.
	 * This information is cached, so you don't need to cache it yourself.
	 * 
	 * @return The names of all mapped columns that are not part of the primary
	 *         key
	 */
	public String[] getAttributeColumnNames()
	{
		if (attributeColumnNames == null) {
			Collection<String> collection = Arrays.asList(getMappedColumnNames());
			collection.removeAll(Arrays.asList(getPrimaryKeyColumnNames()));
			attributeColumnNames = collection.toArray(new String[collection.size()]);
		}
		return attributeColumnNames;
	}

	private Field[] attributeFields = null;

	/**
	 * Get all persistent fields that are not part of the primary key. This
	 * information is cached, so you don't need to cache it yourself.
	 * 
	 * @return All persistent fields that are not part of the primary key
	 */
	public Field[] getAttributeFields()
	{
		if (attributeFields == null) {
			Collection<Field> collection = new ArrayList<>(Arrays.asList(getMappedFields()));
			collection.removeAll(Arrays.asList(getPrimaryKeyFields()));
			attributeFields = collection.toArray(new Field[collection.size()]);
		}
		return attributeFields;
	}

	private String[] attributeFieldNames = null;

	/**
	 * Get the names of all persistent fields that are not part of the primary
	 * key. This information is cached, so you don't need to cache it yourself.
	 * 
	 * @return The names of all persistent fields that are not part of the
	 *         primary key
	 */
	public String[] getAttributeFieldNames()
	{
		if (attributeFieldNames == null) {
			attributeFieldNames = ArrayUtil.stringify(getAttributeFields(), new Stringifier() {

				public String execute(Object object, Object... options)
				{
					return ((Field) object).getName();
				}
			});
		}
		return attributeFieldNames;
	}

	private HashMap<MetaData<?>, String[]> parents = new HashMap<>();

	/**
	 * Specify a relational parent and the field(s) constituting the foreign key
	 * to the parent. You may have to call this method if the JDBC driver and/or
	 * RDBMS cannot provide this information (through the RDBMS's data
	 * dictionary). Calling this method suppresses the dynamic, lazily executed
	 * retrieval of the foreign key.
	 * 
	 * @param parent
	 *            The metadata object representing the relational parent
	 * @param fields
	 *            The field(s) constituting the foreign key
	 */
	public void addParent(MetaData<?> parent, String... fields)
	{
		if (parents.containsKey(parent)) {
			throw new MetaDataAssemblyException("Foreign key to " + parent.getForClass()
					+ " has already been set for class " + getForClass());
		}
		Column[] columns = new Column[fields.length];
		for (int i = 0; i < fields.length; ++i) {
			String field = fields[i];
			DataExchangeUnit deu = getDataExchangeUnit(field);
			if (deu == null) {
				throw new UnmappedFieldException(forClass, field);
			}
			columns[i] = deu.getColumn();
		}
		entity.setForeignKeyColumns(parent.entity, columns);
		parents.put(parent, fields);
	}

	/**
	 * Same as {@code child.addParent(this, fields)}. See
	 * {@link #addParent(MetaData, String...)}.
	 * 
	 * @param child
	 *            A metadata object representing a relational child
	 * @param fields
	 *            The fields representing the foreign key
	 */
	public void addChild(MetaData<?> child, String... fields)
	{
		child.addParent(this, fields);
	}

	/**
	 * Get the field(s) constituting the foreign key to the persistent class
	 * represented by the specified metadata object.
	 * 
	 * @param parent
	 *            The metadata object representing the parent.
	 * 
	 * @return The field(s) constituting the foreign key to the parent.
	 */
	public String[] getForeignKeyFieldNames(MetaData<?> parent)
	{
		String[] fields = parents.get(parent);
		if (fields == null) {
			Column[] fkCols = getEntity().getForeignKeyColumns(parent.getEntity());
			if (fkCols.length == 0) {
				if (parent.getPrimaryKeyFields().length != 1) {
					// TODO throw something
				}
				StringBuilder sb = new StringBuilder(parent.getForClass().getSimpleName());
				sb.setCharAt(0, Character.toLowerCase(sb.charAt(0)));
				sb.append("Id");
				if (!Util.isPersistentField(this, sb.toString())) {
					// TODO throw something
				}
				fields = new String[] { sb.toString() };
			}
			else {
				fields = Util.getFieldNames(this, fkCols);
			}
			parents.put(parent, fields);
		}
		return fields;
	}

	/**
	 * Get the auto-increment column (if any).
	 * 
	 * @return The auto-increment column.
	 */
	public Column getAutoIncrementColumn()
	{
		if (entity instanceof NestedQueryEntity) {
			throw new DomainObjectException("QueryEntity has no auto-increment column");
		}
		return ((TableEntity) entity).getAutoIncrementColumn();
	}

	public Adaptability getAdaptability()
	{
		return adaptability;
	}

	public void setAdaptability(Adaptability adaptability)
	{
		this.adaptability = adaptability;
	}

	/**
	 * Get a value from the registry associated with this metadata object. See
	 * {@link #setValue(String, Object) setValue}.
	 * 
	 * @param key
	 *            The key under which the value was registered
	 * 
	 * @return The value
	 */
	public Object getValue(String key)
	{
		return props.get(key);
	}

	/**
	 * Register an arbitrary value with this metadata object. This can be used
	 * to share information among persistent objects with the same metadata
	 * (persistent objects representing rows from one and the same table). You
	 * shoud not use keys starting with "org.domainobject". These keys are
	 * reserved for use by domainobject itself.
	 * 
	 * @param key
	 *            The key under which to register the value
	 * @param value
	 *            The value to register
	 */
	public void setValue(String key, Object value)
	{
		props.put(key, value);
	}

	MetaDataConfigurator<T> getConfigurator()
	{
		return config;
	}

	private void createPermutations()
	{
		for (DataExchangeUnit deu : dataExchangeUnits) {
			byField.put(deu.getField(), deu);
			byFieldName.put(deu.getField().getName(), deu);
			byColumnName.put(deu.getColumn().getName(), deu);
		}
	}

}
