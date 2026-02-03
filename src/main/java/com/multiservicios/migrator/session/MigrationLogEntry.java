package com.multiservicios.migrator.session;

import java.time.Instant;

public class MigrationLogEntry {
	private final Instant at;
	private final Level level;
	private final String message;
	private final String details;

	public MigrationLogEntry(Level level, String message, String details) {
		this.at = Instant.now();
		this.level = level;
		this.message = message;
		this.details = details;
	}

	public Instant getAt() {
		return at;
	}

	public Level getLevel() {
		return level;
	}

	public String getMessage() {
		return message;
	}

	public String getDetails() {
		return details;
	}

	public enum Level {
		INFO,
		ERROR
	}
}
