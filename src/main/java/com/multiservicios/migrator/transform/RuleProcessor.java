package com.multiservicios.migrator.transform;

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
		if (mapping.getType() == null) {
			return directValue(row, mapping.getSource());
		}

		return switch (mapping.getType()) {
			case DIRECTO -> directValue(row, mapping.getSource());
			case CONSTANTE -> mapping.getValue();
			case EXPRESION -> evaluateExpression(row.values(), mapping.getValue());
			case DEFAULT -> resolveDefault(mapping);
		};
	}

	private Object directValue(RowData row, String column) {
		if (column == null) {
			return null;
		}
		String col = column.trim();
		if (col.isEmpty()) {
			return null;
		}
		return row.get(col);
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
