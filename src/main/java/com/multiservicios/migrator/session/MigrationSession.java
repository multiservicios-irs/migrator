package com.multiservicios.migrator.session;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MigrationSession {
	private final UUID id;
	private final Instant createdAt;
	private final String origenProfile;
	private final String sql;

	private final Deque<BufferedRow> pendientes;
	private final List<FailedRow> failed = new ArrayList<>();
	private final List<MigrationLogEntry> logs = new ArrayList<>();

	private int attempted;
	private int ok;
	private int error;

	public MigrationSession(UUID id, String origenProfile, String sql, List<Map<String, Object>> rows) {
		this.id = id;
		this.createdAt = Instant.now();
		this.origenProfile = origenProfile;
		this.sql = sql;
		this.pendientes = new ArrayDeque<>(rows == null ? List.of() : rows.stream().map(BufferedRow::new).toList());
	}

	public UUID getId() {
		return id;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}

	public String getOrigenProfile() {
		return origenProfile;
	}

	public String getSql() {
		return sql;
	}

	public synchronized int getPendingCount() {
		return pendientes.size();
	}

	public synchronized int getFailedCount() {
		return failed.size();
	}

	public synchronized List<FailedRow> getFailedSnapshot(int max) {
		int from = Math.max(0, failed.size() - Math.max(1, max));
		return new ArrayList<>(failed.subList(from, failed.size()));
	}

	public synchronized int getAttempted() {
		return attempted;
	}

	public synchronized int getOk() {
		return ok;
	}

	public synchronized int getError() {
		return error;
	}

	public synchronized List<MigrationLogEntry> getLogsSnapshot(int max) {
		int from = Math.max(0, logs.size() - Math.max(1, max));
		return new ArrayList<>(logs.subList(from, logs.size()));
	}

	public synchronized BufferedRow pollNext() {
		return pendientes.pollFirst();
	}

	public synchronized void requeue(BufferedRow bufferedRow) {
		if (bufferedRow != null) {
			pendientes.addLast(bufferedRow);
		}
	}

	public synchronized void markFailed(BufferedRow bufferedRow) {
		if (bufferedRow == null) {
			return;
		}
		failed.add(new FailedRow(bufferedRow.getRow(), bufferedRow.getLastError(), bufferedRow.getAttempts()));
		int maxFailed = 5000;
		if (failed.size() > maxFailed) {
			failed.subList(0, failed.size() - maxFailed).clear();
		}
	}

	public synchronized void markOk(String message, String details) {
		attempted++;
		ok++;
		appendLog(MigrationLogEntry.Level.INFO, message, details);
	}

	public synchronized void markError(String message, String details) {
		attempted++;
		error++;
		appendLog(MigrationLogEntry.Level.ERROR, message, details);
	}

	public synchronized void registerFailure(BufferedRow bufferedRow, String message, String details, int maxAttempts) {
		if (bufferedRow == null) {
			markError(message, details);
			return;
		}
		bufferedRow.incrementAttempts();
		bufferedRow.setLastError(message);
		markError(message, details == null ? ("attempt=" + bufferedRow.getAttempts() + "/" + maxAttempts) : (details + " | attempt=" + bufferedRow.getAttempts() + "/" + maxAttempts));
		if (bufferedRow.getAttempts() >= Math.max(1, maxAttempts)) {
			markFailed(bufferedRow);
			appendLog(MigrationLogEntry.Level.ERROR, "Fila marcada como FAILED definitivo", bufferedRow.getLastError());
			return;
		}
		requeue(bufferedRow);
	}

	private void appendLog(MigrationLogEntry.Level level, String message, String details) {
		logs.add(new MigrationLogEntry(level, message, details));
		int maxLogs = 1000;
		if (logs.size() > maxLogs) {
			logs.subList(0, logs.size() - maxLogs).clear();
		}
	}
}
