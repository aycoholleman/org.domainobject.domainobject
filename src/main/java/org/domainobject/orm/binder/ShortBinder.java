package org.domainobject.orm.binder;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.domainobject.orm.core.Column;
import org.domainobject.orm.core.MetaData;
import org.domainobject.orm.exception.BindException;

/**
 * The default binder for short and Short values. It generally uses
 * {@code ResultSet.getShort} to set fields and
 * {@code PreparedStatement.setShort} transfer data. If the field maps to a
 * date column date-to-short and short-to-date conversion is done using the
 * "yyyy" format.
 */
public final class ShortBinder extends NumberBinder {

	private static final SimpleDateFormat INTEGER_DATE_FORMAT = new SimpleDateFormat("yyyy");
	private static final DecimalFormat ZERO_PADDER = new DecimalFormat("0000");


	public static Short dateToShort(Date date)
	{
		return Short.valueOf(INTEGER_DATE_FORMAT.format(date));
	}


	public static Date shortToDate(short s) throws ParseException
	{
		String shortString = ZERO_PADDER.format(s);
		if (shortString.length() != 4) {
			throw new BindException("Too many digits in value. Maximum is 4. Shorts are converted to Dates using \"yyyy\" format.");
		}
		return INTEGER_DATE_FORMAT.parse(shortString);
	}


	public void receive(Object instance, MetaData metadata, Field field, ResultSet rs, int columnIndex) throws SQLException, IllegalAccessException
	{
		int sqlType = rs.getMetaData().getColumnType(columnIndex);
		switch (sqlType) {
			case Types.DATE:
				field.setShort(instance, dateToShort(rs.getDate(columnIndex)));
			default:
				field.setShort(instance, rs.getShort(columnIndex));
		}
	}


	public void send(Object instance, MetaData metadata, Field field, PreparedStatement ps, int parameterIndex) throws SQLException, IllegalAccessException, IllegalArgumentException, ParseException
	{
		int sqlType = metadata.getDataExchangeUnit(field).getColumn().getDataType();
		switch (sqlType) {
			case Types.DATE:
				Date date = shortToDate(field.getShort(instance));
				ps.setDate(parameterIndex, new java.sql.Date(date.getTime()));
				break;
			default:
				ps.setShort(parameterIndex, field.getShort(instance));
		}
	}


	public void send(Object value, Column column, PreparedStatement ps, int parameterIndex) throws Exception
	{
		int sqlType = column.getDataType();
		switch (sqlType) {
			case Types.DATE:
				Date date = shortToDate((Short) value);
				ps.setDate(parameterIndex, new java.sql.Date(date.getTime()));
				break;
			default:
				ps.setShort(parameterIndex, (Short) value);
		}
	}


	public Short receive(ResultSet rs, int columnIndex) throws Exception
	{
		int sqlType = rs.getMetaData().getColumnType(columnIndex);
		switch (sqlType) {
			case Types.DATE:
				return dateToShort(rs.getDate(columnIndex));
			default:
				return rs.getShort(columnIndex);
		}
	}

}
