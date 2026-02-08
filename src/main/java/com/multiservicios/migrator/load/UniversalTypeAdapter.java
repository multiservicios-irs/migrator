package com.multiservicios.migrator.load;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.springframework.stereotype.Component;

import com.multiservicios.migrator.model.ColumnMeta;

@Component
public class UniversalTypeAdapter {
	public Object convert(ColumnMeta meta, Object rawValue) {
		if (rawValue == null) {
			return null;
		}
		if (meta == null) {
			return rawValue;
		}

		String typeName = meta.typeName() == null ? "" : meta.typeName().trim().toLowerCase();
		if ("uuid".equals(typeName)) {
			return toUuid(rawValue);
		}
		if ("json".equals(typeName) || "jsonb".equals(typeName)) {
			// best-effort: dejar String/Map tal cual y que el driver/DB haga coerción
			return rawValue;
		}

		return switch (meta.dataType()) {
			case Types.INTEGER, Types.SMALLINT, Types.TINYINT -> toInteger(rawValue);
			case Types.BIGINT -> toLong(rawValue);
			case Types.NUMERIC, Types.DECIMAL -> toBigDecimal(rawValue);
			case Types.REAL, Types.FLOAT, Types.DOUBLE -> toDouble(rawValue);
			case Types.BIT, Types.BOOLEAN -> toBoolean(rawValue);
			case Types.DATE -> toSqlDate(rawValue);
			case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> toTimestamp(rawValue);
			case Types.CHAR, Types.NCHAR, Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR ->
					toVarchar(rawValue, meta.columnSize());
			default -> rawValue;
		};
	}

	private static Object toVarchar(Object raw, Integer maxLen) {
		String s = raw instanceof String str ? str : String.valueOf(raw);
		if (s.isEmpty()) {
			return s;
		}
		if (maxLen != null && maxLen > 0 && s.length() > maxLen) {
			throw new IllegalArgumentException("Excede longitud máxima (" + maxLen + ")");
		}
		return s;
	}

	private static Object toUuid(Object raw) {
		if (raw instanceof java.util.UUID) {
			return raw;
		}
		String s = String.valueOf(raw).trim();
		if (s.isEmpty()) {
			return null;
		}
		try {
			return java.util.UUID.fromString(s);
		} catch (Exception ex) {
			throw new IllegalArgumentException("UUID inválido: '" + s + "'");
		}
	}

	private static Integer toInteger(Object raw) {
		if (raw instanceof Integer i) {
			return i;
		}
		Long l = toLong(raw);
		if (l == null) {
			return null;
		}
		if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Fuera de rango para INTEGER: " + l);
		}
		return l.intValue();
	}

	private static Long toLong(Object raw) {
		if (raw instanceof Long l) {
			return l;
		}
		if (raw instanceof Number n) {
			return n.longValue();
		}
		String s = normalizeNumberString(raw);
		if (s == null) {
			return null;
		}
		try {
			BigDecimal bd = new BigDecimal(s);
			return bd.longValue();
		} catch (Exception ex) {
			throw new IllegalArgumentException("Número entero inválido: '" + s + "'");
		}
	}

	private static BigDecimal toBigDecimal(Object raw) {
		if (raw instanceof BigDecimal bd) {
			return bd;
		}
		if (raw instanceof Number n) {
			// evitar problemas de punto flotante cuando venga Double
			return new BigDecimal(String.valueOf(n));
		}
		String s = normalizeNumberString(raw);
		if (s == null) {
			return null;
		}
		try {
			return new BigDecimal(s);
		} catch (Exception ex) {
			throw new IllegalArgumentException("Número decimal inválido: '" + s + "'");
		}
	}

	private static Double toDouble(Object raw) {
		if (raw instanceof Double d) {
			return d;
		}
		if (raw instanceof Number n) {
			return n.doubleValue();
		}
		String s = normalizeNumberString(raw);
		if (s == null) {
			return null;
		}
		try {
			return Double.parseDouble(s);
		} catch (Exception ex) {
			throw new IllegalArgumentException("Número inválido: '" + s + "'");
		}
	}

	private static Boolean toBoolean(Object raw) {
		if (raw instanceof Boolean b) {
			return b;
		}
		if (raw instanceof Number n) {
			return n.intValue() != 0;
		}
		String s = String.valueOf(raw).trim().toLowerCase();
		if (s.isEmpty()) {
			return null;
		}
		return switch (s) {
			case "1", "true", "t", "y", "yes", "s", "si", "sí" -> true;
			case "0", "false", "f", "n", "no" -> false;
			default -> throw new IllegalArgumentException("Boolean inválido: '" + s + "'");
		};
	}

	private static java.sql.Date toSqlDate(Object raw) {
		if (raw instanceof java.sql.Date d) {
			return d;
		}
		if (raw instanceof java.util.Date d) {
			return new java.sql.Date(d.getTime());
		}
		if (raw instanceof LocalDate d) {
			return java.sql.Date.valueOf(d);
		}
		if (raw instanceof LocalDateTime dt) {
			return java.sql.Date.valueOf(dt.toLocalDate());
		}
		if (raw instanceof Timestamp ts) {
			return new java.sql.Date(ts.getTime());
		}
		String s = String.valueOf(raw).trim();
		if (s.isEmpty()) {
			return null;
		}
		LocalDate parsed = parseLocalDate(s);
		return java.sql.Date.valueOf(parsed);
	}

	private static Timestamp toTimestamp(Object raw) {
		if (raw instanceof Timestamp ts) {
			return ts;
		}
		if (raw instanceof java.util.Date d) {
			return new Timestamp(d.getTime());
		}
		if (raw instanceof Instant i) {
			return Timestamp.from(i);
		}
		if (raw instanceof OffsetDateTime odt) {
			return Timestamp.from(odt.toInstant());
		}
		if (raw instanceof LocalDateTime ldt) {
			return Timestamp.valueOf(ldt);
		}
		if (raw instanceof LocalDate ld) {
			return Timestamp.valueOf(ld.atStartOfDay());
		}
		String s = String.valueOf(raw).trim();
		if (s.isEmpty()) {
			return null;
		}
		LocalDateTime parsed = parseLocalDateTime(s);
		return Timestamp.valueOf(parsed);
	}

	private static LocalDate parseLocalDate(String s) {
		try {
			return LocalDate.parse(s);
		} catch (Exception ignored) {
		}
		try {
			return LocalDate.parse(s, DateTimeFormatter.ofPattern("dd/MM/uuuu"));
		} catch (Exception ignored) {
		}
		try {
			return LocalDate.parse(s, DateTimeFormatter.ofPattern("uuuu-MM-dd"));
		} catch (Exception ignored) {
		}
		throw new IllegalArgumentException("Fecha inválida: '" + s + "'");
	}

	private static LocalDateTime parseLocalDateTime(String s) {
		try {
			// ISO_LOCAL_DATE_TIME
			return LocalDateTime.parse(s);
		} catch (Exception ignored) {
		}
		try {
			// timestamp con zona (ISO_OFFSET_DATE_TIME)
			OffsetDateTime odt = OffsetDateTime.parse(s);
			return odt.atZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime();
		} catch (Exception ignored) {
		}
		// formatos comunes
		try {
			return LocalDateTime.parse(s, DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss"));
		} catch (Exception ignored) {
		}
		try {
			return LocalDateTime.parse(s, DateTimeFormatter.ofPattern("dd/MM/uuuu HH:mm:ss"));
		} catch (Exception ignored) {
		}
		throw new IllegalArgumentException("Timestamp inválido: '" + s + "'");
	}

	private static String normalizeNumberString(Object raw) {
		String s = String.valueOf(raw);
		if (s == null) {
			return null;
		}
		s = s.trim();
		if (s.isEmpty()) {
			return null;
		}
		// normalizar separadores: "1.234,56" => "1234.56"
		if (s.contains(",") && s.contains(".")) {
			s = s.replace(".", "");
			s = s.replace(",", ".");
		} else if (s.contains(",") && !s.contains(".")) {
			s = s.replace(",", ".");
		}
		// quitar espacios internos
		s = s.replace(" ", "");
		return s;
	}
}
