package com.multiservicios.migrator.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class RowData {
	private final Map<String, Object> values;

	public RowData(Map<String, Object> values) {
		this.values = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(values, "values")));
	}

	public Map<String, Object> values() {
		return values;
	}

	public Object get(String columnName) {
		if (columnName == null) {
			return null;
		}
		Object direct = values.get(columnName);
		if (direct != null || values.containsKey(columnName)) {
			return direct;
		}
		for (var entry : values.entrySet()) {
			String key = entry.getKey();
			if (key != null && key.equalsIgnoreCase(columnName)) {
				return entry.getValue();
			}
		}
		return null;
	}
}
