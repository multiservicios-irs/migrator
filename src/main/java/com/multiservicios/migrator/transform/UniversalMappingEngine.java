package com.multiservicios.migrator.transform;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.multiservicios.migrator.model.FieldMapping;
import com.multiservicios.migrator.model.MappingType;
import com.multiservicios.migrator.model.RowData;

@Component
public class UniversalMappingEngine {
	private final RuleProcessor ruleProcessor;

	public UniversalMappingEngine(RuleProcessor ruleProcessor) {
		this.ruleProcessor = ruleProcessor;
	}

	public Map<String, Object> mapToColumns(RowData row, List<FieldMapping> mappings) {
		Map<String, Object> values = new LinkedHashMap<>();
		if (mappings == null || mappings.isEmpty()) {
			return values;
		}
		for (FieldMapping mapping : mappings) {
			if (mapping == null) {
				continue;
			}
			String target = mapping.getTarget();
			if (target == null || target.isBlank()) {
				continue;
			}
			Object resolved = ruleProcessor.resolveValue(row, mapping);
			values.put(target.trim(), resolved);
		}
		return values;
	}

	public static boolean isDirect(FieldMapping mapping) {
		if (mapping == null) {
			return false;
		}
		MappingType t = mapping.getType();
		return t != null && "DIRECTO".equalsIgnoreCase(t.name());
	}
}
