package com.multiservicios.migrator.transform;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

import com.multiservicios.migrator.config.MigratorProperties;
import com.multiservicios.migrator.model.FieldMapping;
import com.multiservicios.migrator.model.MappingType;
import com.multiservicios.migrator.model.RowData;

@Component
public class RuleProcessor {
	private final MigratorProperties properties;
	private final ExpressionParser spel = new SpelExpressionParser();

	public RuleProcessor(MigratorProperties properties) {
		this.properties = properties;
	}

	public Object resolveValue(RowData row, FieldMapping mapping) {
		Object base;
		if (mapping.getType() == null) {
			base = directValue(row, mapping.getSource());
		} else {
			base = switch (mapping.getType()) {
				case DIRECTO -> directValue(row, mapping.getSource());
				case CONSTANTE -> mapping.getValue();
				case EXPRESION -> evaluateExpression(row.values(), mapping.getValue());
				case DEFAULT -> resolveDefault(mapping);
			};
		}
		return applyRule(base, mapping.getRule());
	}

	private Object applyRule(Object value, String ruleRaw) {
		if (ruleRaw == null || ruleRaw.isBlank()) {
			return value;
		}
		String rule = ruleRaw.trim();
		if (rule.isEmpty()) {
			return value;
		}

		String name = rule;
		String arg = null;
		int p = rule.indexOf('(');
		if (p > 0 && rule.endsWith(")")) {
			name = rule.substring(0, p).trim();
			arg = rule.substring(p + 1, rule.length() - 1).trim();
			if (arg != null && ((arg.startsWith("\"") && arg.endsWith("\"")) || (arg.startsWith("'") && arg.endsWith("'")))) {
				arg = arg.substring(1, arg.length() - 1);
			}
		}

		String key = name.toLowerCase();
		return switch (key) {
			case "now", "currenttimestamp", "current_timestamp" -> LocalDateTime.now();
			case "today", "currentdate", "current_date" -> LocalDate.now();
			case "trim" -> value == null ? null : String.valueOf(value).trim();
			case "upper" -> value == null ? null : String.valueOf(value).toUpperCase();
			case "lower" -> value == null ? null : String.valueOf(value).toLowerCase();
			case "emptytonull" -> {
				if (value == null) yield null;
				String s = String.valueOf(value).trim();
				yield s.isEmpty() ? null : s;
			}
			case "coalesce" -> {
				if (value != null) {
					if (!(value instanceof String)) yield value;
					String s = ((String) value).trim();
					if (!s.isEmpty()) yield value;
				}
				yield parseLiteral(arg);
			}
			case "toint" -> {
				if (value == null) yield null;
				if (value instanceof Number n) yield n.intValue();
				String s = normalizeNumberString(value);
				if (s == null) yield null;
				try {
					yield new BigDecimal(s).intValueExact();
				} catch (Exception ex) {
					throw new IllegalArgumentException("toInt: número inválido");
				}
			}
			case "tolong" -> {
				if (value == null) yield null;
				if (value instanceof Number n) yield n.longValue();
				String s = normalizeNumberString(value);
				if (s == null) yield null;
				try {
					yield new BigDecimal(s).longValueExact();
				} catch (Exception ex) {
					throw new IllegalArgumentException("toLong: número inválido");
				}
			}
			case "todecimal" -> {
				if (value == null) yield null;
				if (value instanceof BigDecimal bd) yield bd;
				if (value instanceof Number n) yield new BigDecimal(String.valueOf(n));
				String s = normalizeNumberString(value);
				if (s == null) yield null;
				try {
					yield new BigDecimal(s);
				} catch (Exception ex) {
					throw new IllegalArgumentException("toDecimal: número inválido");
				}
			}
			case "tobool", "toboolean" -> {
				if (value == null) yield null;
				if (value instanceof Boolean b) yield b;
				String s = String.valueOf(value).trim().toLowerCase();
				if (s.isEmpty()) yield null;
				yield switch (s) {
					case "1", "true", "t", "y", "yes", "s", "si", "sí" -> true;
					case "0", "false", "f", "n", "no" -> false;
					default -> throw new IllegalArgumentException("toBool: valor inválido");
				};
			}
			case "todate" -> {
				if (value == null) yield null;
				if (value instanceof LocalDate ld) yield ld;
				String s = String.valueOf(value).trim();
				if (s.isEmpty()) yield null;
				try {
					if (arg == null || arg.isBlank()) yield LocalDate.parse(s);
					yield LocalDate.parse(s, DateTimeFormatter.ofPattern(arg));
				} catch (Exception ex) {
					throw new IllegalArgumentException("toDate: fecha inválida");
				}
			}
			case "totimestamp", "todatetime" -> {
				if (value == null) yield null;
				if (value instanceof LocalDateTime ldt) yield ldt;
				String s = String.valueOf(value).trim();
				if (s.isEmpty()) yield null;
				try {
					if (arg == null || arg.isBlank()) yield LocalDateTime.parse(s);
					yield LocalDateTime.parse(s, DateTimeFormatter.ofPattern(arg));
				} catch (Exception ex) {
					throw new IllegalArgumentException("toTimestamp: valor inválido");
				}
			}
			default -> value;
		};
	}

	private static Object parseLiteral(String arg) {
		if (arg == null) {
			return null;
		}
		String s = arg.trim();
		if (s.isEmpty()) {
			return null;
		}
		String lower = s.toLowerCase();
		if ("null".equals(lower)) {
			return null;
		}
		if ("true".equals(lower)) {
			return true;
		}
		if ("false".equals(lower)) {
			return false;
		}
		try {
			String n = normalizeNumberString(s);
			if (n != null) {
				return new BigDecimal(n);
			}
		} catch (Exception ignored) {
		}
		return s;
	}

	private static String normalizeNumberString(Object raw) {
		if (raw == null) {
			return null;
		}
		String s = String.valueOf(raw).trim();
		if (s.isEmpty()) {
			return null;
		}
		if (s.contains(",") && s.contains(".")) {
			s = s.replace(".", "");
			s = s.replace(",", ".");
		} else if (s.contains(",") && !s.contains(".")) {
			s = s.replace(",", ".");
		}
		s = s.replace(" ", "");
		return s;
	}

	private Object directValue(RowData row, String column) {
		if (column == null) {
			return null;
		}
		String col = column.trim();
		if (col.isEmpty()) {
			return null;
		}
		Object v = row.get(col);
		if (v != null) {
			return v;
		}

		// Si la columna existe (aunque sea NULL), no la tratamos como literal.
		if (hasColumnIgnoreCase(row, col)) {
			return null;
		}

		// UX: permitir literales aunque el tipo sea DIRECTO.
		// Ej: source="true" -> Boolean.TRUE, source="0" -> BigDecimal(0), source="'abc'" -> "abc".
		return parseLiteral(col);
	}

	private static boolean hasColumnIgnoreCase(RowData row, String column) {
		if (row == null || column == null) {
			return false;
		}
		String col = column.trim();
		if (col.isEmpty()) {
			return false;
		}
		Map<String, Object> values = row.values();
		if (values.containsKey(col)) {
			return true;
		}
		for (String k : values.keySet()) {
			if (k != null && k.equalsIgnoreCase(col)) {
				return true;
			}
		}
		return false;
	}

	private Object evaluateExpression(Map<String, Object> row, String expression) {
		if (expression == null || expression.isBlank()) {
			return null;
		}
		StandardEvaluationContext ctx = new StandardEvaluationContext();
		ctx.setVariable("row", row);
		return spel.parseExpression(expression).getValue(ctx);
	}

	private Object resolveDefault(FieldMapping mapping) {
		String key = mapping.getValue();
		if (key != null && !key.isBlank()) {
			return switch (key) {
				case "depositoCentralId" -> properties.getDefaults().getDepositoCentralId();
				case "marcaGenericaId" -> properties.getDefaults().getMarcaGenericaId();
				case "categoriaGenericaId" -> properties.getDefaults().getCategoriaGenericaId();
				case "unidadMedidaGenericaId" -> properties.getDefaults().getUnidadMedidaGenericaId();
				default -> null;
			};
		}

		String target = mapping.getTarget();
		if (target == null) {
			return null;
		}
		if (target.equalsIgnoreCase("depositoId")) {
			return properties.getDefaults().getDepositoCentralId();
		}
		if (target.equalsIgnoreCase("producto.marcaId")) {
			return properties.getDefaults().getMarcaGenericaId();
		}
		if (target.equalsIgnoreCase("producto.categoriaId")) {
			return properties.getDefaults().getCategoriaGenericaId();
		}
		if (target.equalsIgnoreCase("producto.unidadMedidaId")) {
			return properties.getDefaults().getUnidadMedidaGenericaId();
		}
		return null;
	}
}
