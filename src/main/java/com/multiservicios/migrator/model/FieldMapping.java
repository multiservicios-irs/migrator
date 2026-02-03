package com.multiservicios.migrator.model;

import jakarta.validation.constraints.NotBlank;

public class FieldMapping {
	private String source;

	@NotBlank
	private String target;

	private MappingType type = MappingType.DIRECTO;

	private String value;

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public MappingType getType() {
		return type;
	}

	public void setType(MappingType type) {
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
