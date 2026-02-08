package com.multiservicios.migrator.model;

import java.util.ArrayList;
import java.util.List;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public class MigrationJob {
	@NotBlank
	private String profileName;

	@NotBlank
	private String sql;

	@NotNull
	private List<FieldMapping> mappings = new ArrayList<>();

	private boolean dryRun = true;
	private int previewLimit = 5;

	public String getProfileName() {
		return profileName;
	}

	public void setProfileName(String profileName) {
		this.profileName = profileName;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public List<FieldMapping> getMappings() {
		return mappings;
	}

	public void setMappings(List<FieldMapping> mappings) {
		this.mappings = mappings;
	}

	public boolean isDryRun() {
		return dryRun;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}

	public int getPreviewLimit() {
		return previewLimit;
	}

	public void setPreviewLimit(int previewLimit) {
		this.previewLimit = previewLimit;
	}
}
