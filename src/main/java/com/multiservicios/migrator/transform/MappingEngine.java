package com.multiservicios.migrator.transform;

import java.math.BigDecimal;
import java.util.List;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.stereotype.Component;

import com.multiservicios.migrator.dto.ProductoMigrationDto;
import com.multiservicios.migrator.model.FieldMapping;
import com.multiservicios.migrator.model.RowData;

@Component
public class MappingEngine {
	private final RuleProcessor ruleProcessor;

	public MappingEngine(RuleProcessor ruleProcessor) {
		this.ruleProcessor = ruleProcessor;
	}

	public ProductoMigrationDto mapToProducto(RowData row, List<FieldMapping> mappings) {
		ProductoMigrationDto dto = new ProductoMigrationDto();
		ensureNesting(dto);
		BeanWrapper wrapper = new BeanWrapperImpl(dto);

		for (FieldMapping mapping : mappings) {
			if (mapping == null) {
				continue;
			}
			String target = mapping.getTarget();
			if (target == null) {
				continue;
			}
			target = target.trim();
			if (target.isEmpty()) {
				continue;
			}
			Object resolved = ruleProcessor.resolveValue(row, mapping);
			setSafely(wrapper, target, resolved);
		}
		return dto;
	}

	private void ensureNesting(ProductoMigrationDto dto) {
		if (dto.getProducto() == null) {
			dto.setProducto(new ProductoMigrationDto.ProductoDto());
		}
		if (dto.getStockPorDeposito() == null) {
			dto.setStockPorDeposito(new java.util.ArrayList<>());
		}
		if (dto.getStockPorDeposito().isEmpty()) {
			dto.getStockPorDeposito().add(new ProductoMigrationDto.StockPorDepositoDto());
		}
	}

	private void setSafely(BeanWrapper wrapper, String propertyPath, Object value) {
		try {
			Object coerced = coerceValueForTarget(wrapper, propertyPath, value);
			wrapper.setPropertyValue(propertyPath, coerced);
		} catch (Exception ignored) {
			// la UI puede enviar targets inválidos; se reporta en logs por registro
		}
	}

	private static Object coerceValueForTarget(BeanWrapper wrapper, String propertyPath, Object value) {
		if (value == null) {
			return null;
		}
		Class<?> targetType;
		try {
			targetType = wrapper.getPropertyType(propertyPath);
		} catch (Exception ex) {
			return value;
		}
		if (targetType == null) {
			return value;
		}

		if (BigDecimal.class.equals(targetType)) {
			return asBigDecimalOrNull(value);
		}
		if (Long.class.equals(targetType)) {
			return asLongOrNull(value);
		}
		if (Boolean.class.equals(targetType)) {
			return asBooleanOrNull(value);
		}
		if (targetType.isEnum() && value instanceof String s) {
			String t = s.trim();
			if (t.isEmpty()) {
				return null;
			}
			@SuppressWarnings({"rawtypes", "unchecked"})
			Class<? extends Enum> enumType = (Class<? extends Enum>) targetType;
			try {
				return Enum.valueOf(enumType, t.toUpperCase());
			} catch (Exception ex) {
				return value;
			}
		}
		return value;
	}

	private static BigDecimal asBigDecimalOrNull(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof BigDecimal bd) {
			return bd;
		}
		if (value instanceof Number n) {
			return new BigDecimal(String.valueOf(n));
		}
		String s = String.valueOf(value).trim();
		if (s.isEmpty()) {
			return null;
		}
		return new BigDecimal(s);
	}

	private static Long asLongOrNull(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Long l) {
			return l;
		}
		if (value instanceof Integer i) {
			return i.longValue();
		}
		if (value instanceof Number n) {
			return n.longValue();
		}
		String s = String.valueOf(value).trim();
		if (s.isEmpty()) {
			return null;
		}
		return Long.parseLong(s);
	}

	private static Boolean asBooleanOrNull(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Boolean b) {
			return b;
		}
		if (value instanceof Number n) {
			return n.intValue() != 0;
		}
		String s = String.valueOf(value).trim();
		if (s.isEmpty()) {
			return null;
		}
		return Boolean.parseBoolean(s);
	}
}
