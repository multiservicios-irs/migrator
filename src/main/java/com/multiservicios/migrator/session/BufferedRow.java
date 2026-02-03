package com.multiservicios.migrator.session;

import java.util.Map;

public class BufferedRow {
	private final Map<String, Object> row;
	private int attempts;
	private String lastError;

	public BufferedRow(Map<String, Object> row) {
		this.row = row;
	}

	public Map<String, Object> getRow() {
		return row;
	}

	public int getAttempts() {
		return attempts;
	}

	public void incrementAttempts() {
		this.attempts++;
	}

	public String getLastError() {
		return lastError;
	}

	public void setLastError(String lastError) {
		this.lastError = lastError;
	}
}
