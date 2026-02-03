package com.multiservicios.migrator.session.requests;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public class LoadSqlRequest {
	@NotBlank
	private String origenProfile;

	@NotBlank
	private String sql;

	@Positive
	private Integer maxRows;

	public String getOrigenProfile() {
		return origenProfile;
	}

	public void setOrigenProfile(String origenProfile) {
		this.origenProfile = origenProfile;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public Integer getMaxRows() {
		return maxRows;
	}

	public void setMaxRows(Integer maxRows) {
		this.maxRows = maxRows;
	}
}
