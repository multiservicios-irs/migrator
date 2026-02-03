package com.multiservicios.migrator.session;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.multiservicios.migrator.config.MigratorProperties;
import com.multiservicios.migrator.config.DataSourceRegistry;

@Service
public class SqlBufferService {
	private final DataSourceRegistry dataSourceRegistry;
	private final MigratorProperties properties;
	private final ConcurrentHashMap<UUID, MigrationSession> sessions = new ConcurrentHashMap<>();

	public SqlBufferService(DataSourceRegistry dataSourceRegistry, MigratorProperties properties) {
		this.dataSourceRegistry = dataSourceRegistry;
		this.properties = properties;
	}

	public MigrationSession createSession(String origenProfile, String sql, Integer maxRows) {
		SqlSafetyValidator.validateSelectOnly(sql);
		int effectiveMaxRows = resolveMaxRows(maxRows);
		DataSource origen = dataSourceRegistry.getRequiredDataSource(origenProfile);
		JdbcTemplate jdbcTemplate = new JdbcTemplate(origen);
		String limitedSql = wrapSqlWithLimit(sql, effectiveMaxRows);
		List<Map<String, Object>> rows = jdbcTemplate.queryForList(limitedSql);

		UUID id = UUID.randomUUID();
		MigrationSession session = new MigrationSession(id, origenProfile, limitedSql, rows);
		sessions.put(id, session);
		return session;
	}

	public boolean delete(UUID id) {
		return sessions.remove(id) != null;
	}

	public int deleteAll() {
		int n = sessions.size();
		sessions.clear();
		return n;
	}

	public MigrationSession getRequired(UUID id) {
		MigrationSession session = sessions.get(id);
		if (session == null) {
			throw new IllegalArgumentException("Sesión no encontrada: " + id);
		}
		return session;
	}

	public List<MigrationSessionStatus> listStatuses(int logs) {
		return sessions.values().stream()
				.map(s -> toStatus(s, logs))
				.toList();
	}

	public MigrationSessionStatus status(UUID id, int logs) {
		return toStatus(getRequired(id), logs);
	}

	private MigrationSessionStatus toStatus(MigrationSession s, int logs) {
		MigrationSessionStatus st = new MigrationSessionStatus();
		st.setId(s.getId());
		st.setCreatedAt(s.getCreatedAt());
		st.setOrigenProfile(s.getOrigenProfile());
		st.setSql(s.getSql());
		st.setPending(s.getPendingCount());
		st.setAttempted(s.getAttempted());
		st.setOk(s.getOk());
		st.setError(s.getError());
		st.setFailed(s.getFailedCount());
		st.setLastLogs(s.getLogsSnapshot(logs));
		return st;
	}

	public List<FailedRow> failedRows(UUID sessionId, int max) {
		return getRequired(sessionId).getFailedSnapshot(max);
	}

	@Scheduled(fixedDelayString = "${migrator.session.cleanupIntervalSeconds:300}000")
	public void cleanupExpiredSessions() {
		int ttlMin = Math.max(1, properties.getSession().getTtlMinutes());
		var cutoff = java.time.Instant.now().minus(java.time.Duration.ofMinutes(ttlMin));
		sessions.entrySet().removeIf(e -> e.getValue().getCreatedAt().isBefore(cutoff));
	}

	private int resolveMaxRows(Integer requested) {
		int dflt = Math.max(1, properties.getSession().getMaxRowsDefault());
		int hard = Math.max(1, properties.getSession().getMaxRowsHardLimit());
		int v = requested == null ? dflt : requested;
		v = Math.max(1, v);
		return Math.min(v, hard);
	}

	private String wrapSqlWithLimit(String sql, int limit) {
		String s = sql == null ? "" : sql.trim();
		if (s.isEmpty()) {
			throw new IllegalArgumentException("SQL vacío");
		}
		return "select * from (" + s + ") t limit " + limit;
	}
}
