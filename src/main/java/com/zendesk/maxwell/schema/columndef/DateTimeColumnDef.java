package com.zendesk.maxwell.schema.columndef;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.code.or.common.util.MySQLConstants;

public class DateTimeColumnDef extends ColumnDef {
	protected Long columnLength;

	private static ThreadLocal<StringBuilder> threadLocalBuilder = new ThreadLocal<StringBuilder>() {
		@Override
		protected StringBuilder initialValue() {
			return new StringBuilder();
		}

		@Override
		public StringBuilder get() {
			StringBuilder b = super.get();
			b.setLength(0);
			return b;
		}

	};

	public DateTimeColumnDef(String name, String type, int pos, Long columnLength) {
		super(name, type, pos);
		this.columnLength = columnLength;
	}

	private static SimpleDateFormat dateTimeFormatter;

	private static SimpleDateFormat getDateTimeFormatter() {
		if ( dateTimeFormatter == null ) {
			dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		}
		return dateTimeFormatter;
	}

	// Truncates the number of nano secs to the column length.
	// 123 456 789 nano secs for:
	// - col length 3 -> 123 ;
	// - col length 6 -> 123 456.
	// 123 456 nano secs (123 micro secs):
	// - col length 3 -> 0 (milli secs)
	// - col length 6 -> 123 (micro secs).
	public static int truncateNanosToColumnLength(int nanos, Long columnLength, boolean convertStringNanosToNanos) {
		// When we have in a datetime .1234 it's actually 123400000 nano secs,
		// not 1243
		if ( convertStringNanosToNanos ) {
			String strNanos = Integer.toString(nanos);
			StringBuilder realStrNanos = threadLocalBuilder.get();
			realStrNanos.append(strNanos);

			int i = 0;
			while (i < 9 - strNanos.length()) {
				realStrNanos.append('0');
				i++;
			}

			nanos = Integer.parseInt(realStrNanos.toString());
		}

		int micros = nanos / 1000;

		// 6 is the max precision of datetime2 in MysQL
		int divideBy = ((int) Math.pow(10, 6 - columnLength));

		return micros / divideBy;
	}

	public static String timestampToString(Timestamp t, Long columnLength, boolean convertStringNanosToNanos) {
		int nanos = t.getNanos();
		if ( nanos == 0 ) {
			return getDateTimeFormatter().format(t);
		}

		int fraction = truncateNanosToColumnLength(nanos, columnLength, convertStringNanosToNanos);
		String strFormat = "%0" + columnLength + "d";
		StringBuilder result = threadLocalBuilder.get();
		result.append(getDateTimeFormatter().format(t));
		result.append(".");
		result.append(String.format(strFormat, fraction));

		return result.toString();
	}


	@Override
	public boolean matchesMysqlType(int type) {
		if ( getType().equals("datetime") ) {
			return type == MySQLConstants.TYPE_DATETIME ||
				type == MySQLConstants.TYPE_DATETIME2;
		} else {
			return type == MySQLConstants.TYPE_TIMESTAMP ||
				type == MySQLConstants.TYPE_TIMESTAMP2;
		}
	}

	private String formatValue(Object value, boolean convertStringNanosToNanos) {
		/* protect against multithreaded access of static dateTimeFormatter */
		synchronized ( DateTimeColumnDef.class ) {
			if ( value instanceof Long && getType().equals("datetime") ) {
				return formatLong(( Long ) value);
			} else if ( value instanceof Timestamp ) {
				return timestampToString((Timestamp) value, this.columnLength, convertStringNanosToNanos);
			} else if ( value instanceof Date ) {
				return getDateTimeFormatter().format(( Date ) value);
			} else {
				return "";
			}
		}
	}

	private String formatLong(Long value) {
		final int second = (int)(value % 100); value /= 100;
		final int minute = (int)(value % 100); value /= 100;
		final int hour = (int)(value % 100); value /= 100;
		final int day = (int)(value % 100); value /= 100;
		final int month = (int)(value % 100);
		final int year = (int)(value / 100);

		return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
	}


	@Override
	public String toSQL(Object value) {
		return "'" + formatValue(value, false) + "'";
	}


	@Override
	public Object asJSON(Object value) {
		return formatValue(value, true);
	}

	public Long getColumnLength() { return columnLength ; }
}
