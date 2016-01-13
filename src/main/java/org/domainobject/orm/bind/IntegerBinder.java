package org.domainobject.orm.bind;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.domainobject.orm.core.Column;
import org.domainobject.orm.exception.BindException;

/**
 * The default binder for int and Integer values. It generally uses
 * {@code ResultSet.getInt} to set fields and {@code PreparedStatement.setInt}
 * to transfer data. If the field maps to a date column date-to-int and
 * int-to-date conversion is done using the "yyyyMMdd" format.
 */
public final class IntegerBinder extends NumberBinder {

	private static final SimpleDateFormat INTEGER_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
	private static final DecimalFormat ZERO_PADDER = new DecimalFormat("00000000");


	public static Integer dateToInt(Date date)
	{
		return Integer.valueOf(INTEGER_DATE_FORMAT.format(date));
	}


	public static Date intToDate(Integer i) throws ParseException
	{
		String intString = ZERO_PADDER.format(i);
		if (intString.length() != 8) {
			throw new BindException("Too many digits in value. Maximum is 8. Integers are converted to Dates using \"yyyyMMdd\" format.");
		}
		return INTEGER_DATE_FORMAT.parse(intString);
	}


	public void send(Object value, Column column, PreparedStatement ps, int parameterIndex) throws Exception
	{
		Integer i = (Integer) value;
		int sqlType = column.getDataType();
		switch (sqlType) {
			case Types.DATE:
				Date date = intToDate(i);
				ps.setDate(parameterIndex, new java.sql.Date(date.getTime()));
				break;
			default:
				ps.setInt(parameterIndex, i);
		}
	}


	public Integer receive(ResultSet rs, int columnIndex) throws SQLException
	{
		int sqlType = rs.getMetaData().getColumnType(columnIndex);
		switch (sqlType) {
			case Types.DATE:
				return dateToInt(rs.getDate(columnIndex));
		}
		return rs.getInt(columnIndex);
	}

}
