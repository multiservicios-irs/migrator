package com.multiservicios.migrator.session;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class MigrationSessionStatus {
	private UUID id;
	private Instant createdAt;
	private String origenProfile;
	private String sql;
	private int pending;
	private int attempted;
	private int ok;
	private int error;
	private int failed;
	private List<MigrationLogEntry> lastLogs;

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Instant createdAt) {
		this.createdAt = createdAt;
	}

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

	public int getPending() {
		return pending;
	}

	public void setPending(int pending) {
		this.pending = pending;
	}

	public int getAttempted() {
		return attempted;
	}

	public void setAttempted(int attempted) {
		this.attempted = attempted;
	}

	public int getOk() {
		return ok;
	}

	public void setOk(int ok) {
		this.ok = ok;
	}

	public int getError() {
		return error;
	}

	public void setError(int error) {
		this.error = error;
	}

	public int getFailed() {
		return failed;
	}

	public void setFailed(int failed) {
		this.failed = failed;
	}

	public List<MigrationLogEntry> getLastLogs() {
		return lastLogs;
	}

	public void setLastLogs(List<MigrationLogEntry> lastLogs) {
		this.lastLogs = lastLogs;
	}
}
