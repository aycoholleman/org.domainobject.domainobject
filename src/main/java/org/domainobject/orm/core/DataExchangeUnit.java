package org.domainobject.orm.core;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.domainobject.orm.bind.IBinder;
import org.domainobject.orm.exception.BindException;
import org.domainobject.orm.exception.DataExchangeException;
import org.domainobject.orm.exception.DomainObjectException;
import org.domainobject.orm.exception.ReflectionException;
import org.domainobject.orm.persister.StandardPersister;

/**
 * <p>
 * A {@code DataExchangeUnit} exchanges data between a field in a
 * {@link Persistable} object and a column in a {@link java.sql.ResultSet} c.q.
 * {@link java.sql.PreparedStatement}. A {@link MetaData} object maintains a
 * list of {@code DataExchangeUnit}s, one for each persistent field. As a
 * high-level user you typically do not need to be aware of the existence of
 * {@code DataExchangeUnit}s, just like you don't need to be aware of metadata
 * objects in the first place. Only when implementing your own
 * {@link StandardPersister} might you want to get hold of a metadata object's
 * {@code DataExchangeUnit}s. When sending data to the database (via a
 * {@code PreparedStatement}) or receiving data from the database (via a
 * {@code ResultSet}) you would typically iterate over the
 * {@code DataExchangeUnit}s and let them take care of exchanging the data.
 * </p>
 * <p>
 * A {@code DataExchangeUnit} is a feather-light object, basically just an
 * immutable triplet of a {@code Field}, a {@code Column} and a {@code Binder}.
 * </p>
 * 
 * @see MetaData
 * @see Column
 * @see IBinder
 */
public final class DataExchangeUnit {

	private final Field field;
	private final Column column;
	private final IBinder binder;

	public DataExchangeUnit(Field field, Column column, IBinder binder)
	{
		this.field = field;
		this.column = column;
		this.binder = binder;
	}

	/**
	 * Get the field (belonging to a {@code Persistable} object) whose value is
	 * going to be set or read by this {@code DataExchangeUnit}.
	 * 
	 * @return The Field
	 */
	public Field getField()
	{
		return field;
	}

	/**
	 * Get the column whose value is going to be set or read by this
	 * {@code DataExchangeUnit}.
	 */
	public Column getColumn()
	{
		return column;
	}

	/**
	 * Get the {@code Binder} that is going to transfer the data between the
	 * field and the {@code ResultSet} / {@code PreparedStatement}.
	 */
	public IBinder getBinder()
	{
		return binder;
	}

	public boolean isFieldSQLNull(Object instance)
	{
		try {
			return binder.isSQLNull(field.get(instance), column);
		}
		catch (IllegalArgumentException e) {
			throw new ReflectionException(e);
		}
		catch (IllegalAccessException e) {
			throw new ReflectionException(e);
		}
	}

	public boolean isValueSQLNull(Object value)
	{
		return binder.isSQLNull(value, column);
	}

	public void send(Object persistable, PreparedStatement ps, int paramIndex)
	{
		try {
			binder.send(field.get(persistable), column, ps, paramIndex);
		}
		catch (BindException e) {
			throw new DataExchangeException(this, true, e);
		}
		catch (Exception e) {
			throw DomainObjectException.rethrow(e);
		}
	}

	public void sendValue(Object value, PreparedStatement ps, int paramIndex)
	{
		try {
			binder.send(value, column, ps, paramIndex);
		}
		catch (BindException e) {
			throw new DataExchangeException(this, true, e);
		}
		catch (Exception e) {
			throw DomainObjectException.rethrow(e);
		}
	}

	public void receive(Object persistable, ResultSet rs, int columnIndex)
	{
		try {
			field.set(persistable, binder.receive(rs, columnIndex));
		}
		catch (BindException e) {
			throw new DataExchangeException(this, false, e);
		}
		catch (Exception e) {
			throw DomainObjectException.rethrow(e);
		}
	}

	public Object receiveValue(ResultSet rs, int columnIndex)
	{
		try {
			return binder.receive(rs, columnIndex);
		}
		catch (BindException e) {
			throw new DataExchangeException(this, false, e);
		}
		catch (Exception e) {
			throw DomainObjectException.rethrow(e);
		}
	}

}
