package com.multiservicios.migrator.config;

import jakarta.validation.constraints.NotBlank;

public class ConnectionProfile {
	@NotBlank
	private String name;

	@NotBlank
	private String jdbcUrl;

	private String username;
	private String password;
	private String driverClassName;
	private int validationTimeoutSeconds = 5;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getDriverClassName() {
		return driverClassName;
	}

	public void setDriverClassName(String driverClassName) {
		this.driverClassName = driverClassName;
	}

	public int getValidationTimeoutSeconds() {
		return validationTimeoutSeconds;
	}

	public void setValidationTimeoutSeconds(int validationTimeoutSeconds) {
		this.validationTimeoutSeconds = validationTimeoutSeconds;
	}
}
