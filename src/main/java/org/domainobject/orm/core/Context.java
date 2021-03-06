package org.domainobject.orm.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.domainobject.orm.bind.DefaultBinderRepository;
import org.domainobject.orm.bind.IBinder;
import org.domainobject.orm.bind.IBinderRepository;
import org.domainobject.orm.exception.DomainObjectSQLException;
import org.domainobject.orm.exception.MetaDataAssemblyException;
import org.domainobject.orm.h2.H2ObjectFactory;
import org.domainobject.orm.map.IMappingAlgorithm;
import org.domainobject.orm.map.LowerCaseMappingAlgorithm;
import org.domainobject.orm.mysql.MySQLObjectFactory;
import org.domainobject.orm.oracle.OracleObjectFactory;

/**
 * <p>
 * A {@code Context} is responsible for creating {@link MetaData} objects. It is
 * typically the very first class within the domainobject library that you
 * instantiate. If you need fine-grained control over how a metadata object is
 * configured and assembled, you should not request it directly from the
 * {@code Context}. Instead, you should request a {@code MetaDataConfigurator}
 * from the factory, and then let the {@code MetaDataConfigurator} create the
 * metadata object for you.
 * </p>
 * <p>
 * In most cases you won't need more than one metadata factory. After all,
 * metadata configurators will let you override any or all factory settings
 * anyway later on. However, a factory will not let you create multiple metadata
 * objects for one class of objects. There are some use cases, though, where
 * your program might need two differently configured metadata object for one
 * and the same class of objects (see the {@link MetaData} documentation). In
 * that case you will need to instantiate two (or more) factories.
 * </p>
 * 
 * 
 * @see MetaData
 * @see MetaDataConfigurator
 */
public final class Context {

	private static Context defaultContext;

	/**
	 * Get the default metadata factory. This is the most recently instantiated
	 * metadata factory. Every new metadata factory that you create registers
	 * itself as the default metadata factory, overwriting the previous metadata
	 * factory. If no metadata factory has been created yet, this method will
	 * throw a {@link MetaDataAssemblyException}.
	 * 
	 * @return The most recently instantiated metadata factory.
	 * 
	 * @throws MetaDataAssemblyException
	 *             If no metadata factory has been created yet.
	 */
	public static Context getDefaultContext()
	{
		if (defaultContext == null) {
			throw new MetaDataAssemblyException("No Context available yet");
		}
		return defaultContext;
	}

	final Map<Class<?>, MetaData<?>> metadataCache = new HashMap<>();
	final Map<String, String> namedQueries = new HashMap<>();

	final Connection connection;
	final DatabaseInfo dbInfo;
	final IObjectFactory objectFactory;

	/**
	 * Create a metadata factory with the specified JDBC connection. A
	 * {@link LowerCaseMappingAlgorithm} is going to be used to map fields to
	 * columns, and binders for exchanging data between them are going to be
	 * sourced from a share instance of {@link DefaultBinderRepository}.
	 * 
	 * @see IMappingAlgorithm
	 * @see IBinder
	 * @see IBinderRepository
	 * @see DefaultBinderRepository#getSharedInstance()
	 * 
	 * @param conn
	 *            The JDBC connection
	 */
	public Context(Connection conn)
	{
		this.connection = conn;
		dbInfo = DatabaseInfo.create(conn);
		if (dbInfo.isMySQL())
			objectFactory = new MySQLObjectFactory();
		else if (dbInfo.isOracle())
			objectFactory = new OracleObjectFactory();
		else
			// TODO: more vendors
			objectFactory = new H2ObjectFactory();
		defaultContext = this;
	}

	/**
	 * Retrieves the metadata object for the specified class from the factory's
	 * metadata cache. Since metadata object creation is a heavy-weight process,
	 * you should always check the cache first (using this method) before
	 * requesting a metadata object from a {@link MetaDataConfigurator}. This is
	 * not necessary when requesting a metadata object directly from the
	 * factory.
	 * 
	 * @see #createMetaData(Class)
	 * @see MetaDataConfigurator#createMetaData()
	 * 
	 * @param forClass
	 *            The class for which to retrieve the metadata object
	 * @return The metadata object, or null if no metadata object has been
	 *         created yet for the specified class
	 */
	public <T> MetaData<T> findMetaData(Class<T> forClass)
	{
		@SuppressWarnings("unchecked")
		MetaData<T> metaData = (MetaData<T>) metadataCache.get(forClass);
		return metaData;
	}

	/**
	 * Returns a metadata object for the specified class. If a metadata object
	 * for this class already exists, that metadata object is returned.
	 * Otherwise a new metadata object is created and returned.
	 * 
	 * @param forClass
	 *            The class for which to create the metadata object
	 * 
	 * @return The metadata object
	 */
	public <T> MetaData<T> createMetaData(Class<T> forClass)
	{
		@SuppressWarnings("unchecked")
		MetaData<T> metadata = (MetaData<T>) metadataCache.get(forClass);
		if (metadata != null)
			return metadata;
		return new MetaDataConfigurator<>(forClass, this).createMetaData();
	}

	/**
	 * Creates and returns a new {@link MetaDataConfigurator} object. A
	 * {@code MetaDataConfigurator} allows you to override some or all factory
	 * settings before creating the metadata object.
	 * 
	 * @param forClass
	 *            The {@code Persistable} class for which you want to configure
	 *            a metadata object.
	 * 
	 * @return The {@code MetaDataConfigurator}
	 */
	public <T> MetaDataConfigurator<T> getConfigurator(Class<T> forClass)
	{
		return new MetaDataConfigurator<>(forClass, this);
	}

	/**
	 * Get the JDBC Connection with which this {@code Context} was instantiated.
	 * 
	 * @return The JDBC Connection with which this {@code Context} was
	 *         instantiated.
	 */
	public Connection getJDBCConnection()
	{
		return connection;
	}

	/**
	 * Close this factory as well as the JDBC connection with which it was
	 * instantiated (if any).
	 */
	public void close()
	{
		close(true);
	}

	/**
	 * Close this factory. This will clear the metadata object cache maintained
	 * by this factory. Also, all {@link Query} objects that were initilized
	 * with {@link MetaData} objects produced by this factory are closed and
	 * removed from the query cache. The database resources that the
	 * {@code Query} objects held on to will be relinquished. You should not use
	 * a factory after closing it. Doing so has undefined results.
	 * 
	 * @see Query#initialize(String, MetaData)
	 * @see Query#destroy()
	 * @see Query#clearCache()
	 * 
	 * @param closeConnection
	 *            Whether to also close the JDBC connection with which this
	 *            factory was instantiated (if any).
	 */
	public void close(boolean closeConnection)
	{
		metadataCache.clear();
		Query.clearCache(this);
		if (closeConnection && connection != null) {
			try {
				connection.close();
			}
			catch (SQLException e) {
				throw new DomainObjectSQLException(e);
			}
		}
	}

	public DatabaseInfo getDatabaseInfo()
	{
		return dbInfo;
	}

}