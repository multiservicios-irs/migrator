package com.multiservicios.migrator.logs;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Service;

@Service
public class MigrationLogService {
	private final List<MigrationLogEntry> entries = Collections.synchronizedList(new ArrayList<>());

	public void clear() {
		entries.clear();
	}

	public void info(int rowNumber, String message) {
		entries.add(new MigrationLogEntry(Instant.now(), rowNumber, "INFO", message));
	}

	public void error(int rowNumber, String message) {
		entries.add(new MigrationLogEntry(Instant.now(), rowNumber, "ERROR", message));
	}

	public List<MigrationLogEntry> snapshot() {
		synchronized (entries) {
			return List.copyOf(entries);
		}
	}

	public record MigrationLogEntry(Instant at, int rowNumber, String level, String message) {
	}
}
