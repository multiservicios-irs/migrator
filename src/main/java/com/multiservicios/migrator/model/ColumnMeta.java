package com.multiservicios.migrator.model;

public record ColumnMeta(
		String name,
		int dataType,
		String typeName,
		Integer columnSize,
		Integer decimalDigits,
		boolean nullable,
		String columnDefault,
		boolean autoIncrement,
		boolean generated
) {
	public String nameLower() {
		return name == null ? "" : name.toLowerCase();
	}

	public boolean hasDefault() {
		return columnDefault != null && !columnDefault.isBlank();
	}

	/**
	 * True si la app debe proveer un valor (NOT NULL, sin default y no autogenerada).
	 */
	public boolean isRequiredInput() {
		return !nullable && !hasDefault() && !autoIncrement && !generated;
	}
}
