package com.multiservicios.migrator.controller;

import java.util.Collections;
import java.util.Map;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.multiservicios.migrator.config.ConnectionProfile;
import com.multiservicios.migrator.config.DataSourceRegistry;
import com.multiservicios.migrator.engine.UniversalMigrationRunner;
import com.multiservicios.migrator.load.UniversalTableLoader;
import com.multiservicios.migrator.logs.MigrationLogService;
import com.multiservicios.migrator.model.FieldMapping;
import com.zaxxer.hikari.HikariDataSource;

@Controller
public class UniversalMigrationController {
	private final DataSourceRegistry dataSourceRegistry;
	private final UniversalMigrationRunner runner;
	private final UniversalTableLoader tableLoader;
	private final MigrationLogService logService;
	private final ObjectMapper objectMapper;

	public UniversalMigrationController(
			DataSourceRegistry dataSourceRegistry,
			UniversalMigrationRunner runner,
			UniversalTableLoader tableLoader,
			MigrationLogService logService,
			ObjectMapper objectMapper) {
		this.dataSourceRegistry = dataSourceRegistry;
		this.runner = runner;
		this.tableLoader = tableLoader;
		this.logService = logService;
		this.objectMapper = objectMapper;
	}

	@GetMapping("/universal")
	public String index(Model model,
			@RequestParam(name = "dbOrigenName", required = false) String dbOrigenName,
			@RequestParam(name = "dbOrigenUrl", required = false) String dbOrigenUrl,
			@RequestParam(name = "dbOrigenUsername", required = false) String dbOrigenUsername,
			@RequestParam(name = "dbOrigenPassword", required = false) String dbOrigenPassword,
			@RequestParam(name = "dbOrigenDriver", required = false) String dbOrigenDriver,
			@RequestParam(name = "dbDestinoName", required = false) String dbDestinoName,
			@RequestParam(name = "dbDestinoUrl", required = false) String dbDestinoUrl,
			@RequestParam(name = "dbDestinoUsername", required = false) String dbDestinoUsername,
			@RequestParam(name = "dbDestinoPassword", required = false) String dbDestinoPassword,
			@RequestParam(name = "dbDestinoDriver", required = false) String dbDestinoDriver,
			@RequestParam(name = "sql", required = false) String sql,
			@RequestParam(name = "destSchema", required = false) String destSchema,
			@RequestParam(name = "destTable", required = false) String destTable,
			@RequestParam(name = "mappings", required = false) String mappings) {

		// Defaults simples (mantener similar a la UI actual)
		model.addAttribute("dbOrigenName", dbOrigenName == null ? "deliStore" : dbOrigenName);
		model.addAttribute("dbOrigenUrl", dbOrigenUrl == null ? "jdbc:postgresql://localhost:5432/deliStore" : dbOrigenUrl);
		model.addAttribute("dbOrigenUsername", dbOrigenUsername == null ? "postgres" : dbOrigenUsername);
		model.addAttribute("dbOrigenPassword", dbOrigenPassword == null ? "" : dbOrigenPassword);
		model.addAttribute("dbOrigenDriver", dbOrigenDriver == null ? "org.postgresql.Driver" : dbOrigenDriver);

		model.addAttribute("dbDestinoName", dbDestinoName == null ? "prueba_db" : dbDestinoName);
		model.addAttribute("dbDestinoUrl", dbDestinoUrl == null ? "jdbc:postgresql://localhost:5432/prueba_db" : dbDestinoUrl);
		model.addAttribute("dbDestinoUsername", dbDestinoUsername == null ? "postgres" : dbDestinoUsername);
		model.addAttribute("dbDestinoPassword", dbDestinoPassword == null ? "" : dbDestinoPassword);
		model.addAttribute("dbDestinoDriver", dbDestinoDriver == null ? "org.postgresql.Driver" : dbDestinoDriver);

		model.addAttribute("sql", sql == null ? "" : sql);
		model.addAttribute("destSchema", (destSchema == null || destSchema.isBlank()) ? "public" : destSchema);
		model.addAttribute("destTable", destTable == null ? "" : destTable);
		model.addAttribute("mappings", mappings == null ? "[]" : mappings);
		if (!model.asMap().containsKey("previewColumns")) {
			model.addAttribute("previewColumns", List.of());
		}
		return "universal";
	}

	@PostMapping("/universal/test-connection")
	public String testConnection(
			@RequestParam("dbOrigenName") String dbOrigenName,
			@RequestParam("dbOrigenUrl") String dbOrigenUrl,
			@RequestParam("dbOrigenUsername") String dbOrigenUsername,
			@RequestParam("dbOrigenPassword") String dbOrigenPassword,
			@RequestParam("dbOrigenDriver") String dbOrigenDriver,
			@RequestParam("dbDestinoName") String dbDestinoName,
			@RequestParam("dbDestinoUrl") String dbDestinoUrl,
			@RequestParam("dbDestinoUsername") String dbDestinoUsername,
			@RequestParam("dbDestinoPassword") String dbDestinoPassword,
			@RequestParam("dbDestinoDriver") String dbDestinoDriver,
			@RequestParam(name = "sql", required = false) String sql,
			@RequestParam(name = "destSchema", required = false) String destSchema,
			@RequestParam(name = "destTable", required = false) String destTable,
			@RequestParam(name = "mappings", required = false) String mappings,
			Model model) {
		String message;
		String origenDb = null;
		String destinoDb = null;
		try {
			ConnectionProfile profile = new ConnectionProfile();
			profile.setName(dbOrigenName);
			profile.setJdbcUrl(dbOrigenUrl);
			profile.setUsername(dbOrigenUsername);
			profile.setPassword(dbOrigenPassword);
			profile.setDriverClassName(dbOrigenDriver);
			profile.setValidationTimeoutSeconds(5);
			origenDb = testAndGetConnectedDb(profile);

			ConnectionProfile destino = new ConnectionProfile();
			destino.setName(dbDestinoName);
			destino.setJdbcUrl(dbDestinoUrl);
			destino.setUsername(dbDestinoUsername);
			destino.setPassword(dbDestinoPassword);
			destino.setDriverClassName(dbDestinoDriver);
			destino.setValidationTimeoutSeconds(5);
			destinoDb = testAndGetConnectedDb(destino);

			message = "✓ Conexión exitosa — Origen: " + origenDb + " | Destino: " + destinoDb;
		} catch (Exception ex) {
			String errorMsg = ex.getMessage();
			if (errorMsg != null && errorMsg.contains("does not exist")) {
				String dbName = extractDbNameFromError(errorMsg);
				message = "✗ Error: La base de datos '" + dbName + "' no existe";
			} else if (errorMsg != null && errorMsg.contains("password authentication failed")) {
				message = "✗ Error: Usuario o contraseña incorrectos";
			} else if (errorMsg != null && errorMsg.contains("Connection refused")) {
				message = "✗ Error: No se pudo conectar al servidor. ¿Está PostgreSQL corriendo?";
			} else {
				message = "✗ Error de conexión: " + errorMsg;
			}
		}
		model.addAttribute("flash", message);
		return index(model, dbOrigenName, dbOrigenUrl, dbOrigenUsername, dbOrigenPassword, dbOrigenDriver,
				dbDestinoName, dbDestinoUrl, dbDestinoUsername, dbDestinoPassword, dbDestinoDriver,
				sql, destSchema, destTable, mappings);
	}

	@PostMapping("/universal/preview")
	public String preview(
			@RequestParam("dbOrigenName") String dbOrigenName,
			@RequestParam("dbOrigenUrl") String dbOrigenUrl,
			@RequestParam("dbOrigenUsername") String dbOrigenUsername,
			@RequestParam("dbOrigenPassword") String dbOrigenPassword,
			@RequestParam("dbOrigenDriver") String dbOrigenDriver,
			@RequestParam("dbDestinoName") String dbDestinoName,
			@RequestParam("dbDestinoUrl") String dbDestinoUrl,
			@RequestParam("dbDestinoUsername") String dbDestinoUsername,
			@RequestParam("dbDestinoPassword") String dbDestinoPassword,
			@RequestParam("dbDestinoDriver") String dbDestinoDriver,
			@RequestParam("sql") String sql,
			@RequestParam(name = "limit", defaultValue = "20") int limit,
			@RequestParam(name = "destSchema", required = false) String destSchema,
			@RequestParam(name = "destTable", required = false) String destTable,
			@RequestParam("mappings") String mappingsJson,
			Model model) {
		HikariDataSource ds = null;
		try {
			String sanitizedSql = sanitizeSql(sql);
			ConnectionProfile profile = new ConnectionProfile();
			profile.setName(dbOrigenName);
			profile.setJdbcUrl(dbOrigenUrl);
			profile.setUsername(dbOrigenUsername);
			profile.setPassword(dbOrigenPassword);
			profile.setDriverClassName(dbOrigenDriver);
			profile.setValidationTimeoutSeconds(5);
			ds = dataSourceRegistry.createDataSource(profile);
			int safeLimit = Math.max(1, limit);
			String limitedSql = wrapSqlWithLimit(sanitizedSql, safeLimit);
			JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
			var rows = jdbcTemplate.queryForList(limitedSql);
			model.addAttribute("previewRows", rows);
			model.addAttribute("previewColumns", columnsFromRows(rows));
		} catch (Exception ex) {
			model.addAttribute("flash", "Error al previsualizar (universal): " + ex.getMessage());
		} finally {
			dataSourceRegistry.closeQuietly(ds);
		}
		return index(model, dbOrigenName, dbOrigenUrl, dbOrigenUsername, dbOrigenPassword, dbOrigenDriver,
				dbDestinoName, dbDestinoUrl, dbDestinoUsername, dbDestinoPassword, dbDestinoDriver,
				sanitizeSqlQuietly(sql), destSchema, destTable, mappingsJson);
	}

	@PostMapping("/universal/clear-logs")
	public String clearLogs(Model model,
			@RequestParam(name = "dbOrigenName", required = false) String dbOrigenName,
			@RequestParam(name = "dbOrigenUrl", required = false) String dbOrigenUrl,
			@RequestParam(name = "dbOrigenUsername", required = false) String dbOrigenUsername,
			@RequestParam(name = "dbOrigenPassword", required = false) String dbOrigenPassword,
			@RequestParam(name = "dbOrigenDriver", required = false) String dbOrigenDriver,
			@RequestParam(name = "dbDestinoName", required = false) String dbDestinoName,
			@RequestParam(name = "dbDestinoUrl", required = false) String dbDestinoUrl,
			@RequestParam(name = "dbDestinoUsername", required = false) String dbDestinoUsername,
			@RequestParam(name = "dbDestinoPassword", required = false) String dbDestinoPassword,
			@RequestParam(name = "dbDestinoDriver", required = false) String dbDestinoDriver,
			@RequestParam(name = "sql", required = false) String sql,
			@RequestParam(name = "destSchema", required = false) String destSchema,
			@RequestParam(name = "destTable", required = false) String destTable,
			@RequestParam(name = "mappings", required = false) String mappings) {
		logService.clear();
		model.addAttribute("flash", "✓ Logs limpiados");
		return index(model, dbOrigenName, dbOrigenUrl, dbOrigenUsername, dbOrigenPassword, dbOrigenDriver,
				dbDestinoName, dbDestinoUrl, dbDestinoUsername, dbDestinoPassword, dbDestinoDriver,
				sql, destSchema, destTable, mappings);
	}

	@PostMapping(value = "/universal/list-tables", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public List<String> listTables(
			@RequestParam("dbDestinoName") String dbDestinoName,
			@RequestParam("dbDestinoUrl") String dbDestinoUrl,
			@RequestParam("dbDestinoUsername") String dbDestinoUsername,
			@RequestParam("dbDestinoPassword") String dbDestinoPassword,
			@RequestParam("dbDestinoDriver") String dbDestinoDriver,
			@RequestParam(name = "schema", defaultValue = "public") String schema) throws Exception {
		HikariDataSource ds = null;
		try {
			ConnectionProfile p = new ConnectionProfile();
			p.setName(dbDestinoName);
			p.setJdbcUrl(dbDestinoUrl);
			p.setUsername(dbDestinoUsername);
			p.setPassword(dbDestinoPassword);
			p.setDriverClassName(dbDestinoDriver);
			p.setValidationTimeoutSeconds(5);
			ds = dataSourceRegistry.createDataSource(p);
			return tableLoader.listTables(ds, schema);
		} finally {
			dataSourceRegistry.closeQuietly(ds);
		}
	}

	@PostMapping(value = "/universal/describe-table", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public List<String> describeTable(
			@RequestParam("dbDestinoName") String dbDestinoName,
			@RequestParam("dbDestinoUrl") String dbDestinoUrl,
			@RequestParam("dbDestinoUsername") String dbDestinoUsername,
			@RequestParam("dbDestinoPassword") String dbDestinoPassword,
			@RequestParam("dbDestinoDriver") String dbDestinoDriver,
			@RequestParam(name = "schema", defaultValue = "public") String schema,
			@RequestParam("table") String table) throws Exception {
		HikariDataSource ds = null;
		try {
			ConnectionProfile p = new ConnectionProfile();
			p.setName(dbDestinoName);
			p.setJdbcUrl(dbDestinoUrl);
			p.setUsername(dbDestinoUsername);
			p.setPassword(dbDestinoPassword);
			p.setDriverClassName(dbDestinoDriver);
			p.setValidationTimeoutSeconds(5);
			ds = dataSourceRegistry.createDataSource(p);
			return tableLoader.listColumns(ds, schema, table);
		} finally {
			dataSourceRegistry.closeQuietly(ds);
		}
	}

	@PostMapping("/universal/run")
	public String run(
			@RequestParam("dbOrigenName") String dbOrigenName,
			@RequestParam("dbOrigenUrl") String dbOrigenUrl,
			@RequestParam("dbOrigenUsername") String dbOrigenUsername,
			@RequestParam("dbOrigenPassword") String dbOrigenPassword,
			@RequestParam("dbOrigenDriver") String dbOrigenDriver,
			@RequestParam("dbDestinoName") String dbDestinoName,
			@RequestParam("dbDestinoUrl") String dbDestinoUrl,
			@RequestParam("dbDestinoUsername") String dbDestinoUsername,
			@RequestParam("dbDestinoPassword") String dbDestinoPassword,
			@RequestParam("dbDestinoDriver") String dbDestinoDriver,
			@RequestParam("sql") String sql,
			@RequestParam("destSchema") String destSchema,
			@RequestParam("destTable") String destTable,
			@RequestParam("mappings") String mappingsJson,
			@RequestParam(name = "dryRun", defaultValue = "true") boolean dryRun,
			Model model) {
		HikariDataSource origen = null;
		HikariDataSource destino = null;
		try {
			List<FieldMapping> mappings = parseMappings(mappingsJson);

			ConnectionProfile pOrigen = new ConnectionProfile();
			pOrigen.setName(dbOrigenName);
			pOrigen.setJdbcUrl(dbOrigenUrl);
			pOrigen.setUsername(dbOrigenUsername);
			pOrigen.setPassword(dbOrigenPassword);
			pOrigen.setDriverClassName(dbOrigenDriver);
			pOrigen.setValidationTimeoutSeconds(5);
			origen = dataSourceRegistry.createDataSource(pOrigen);

			ConnectionProfile pDestino = new ConnectionProfile();
			pDestino.setName(dbDestinoName);
			pDestino.setJdbcUrl(dbDestinoUrl);
			pDestino.setUsername(dbDestinoUsername);
			pDestino.setPassword(dbDestinoPassword);
			pDestino.setDriverClassName(dbDestinoDriver);
			pDestino.setValidationTimeoutSeconds(5);
			destino = dataSourceRegistry.createDataSource(pDestino);

			var result = runner.run(origen, destino, sql, mappings, destSchema, destTable, dryRun);
			model.addAttribute("runResult", result);
			model.addAttribute("runLogs", logService.snapshot());
		} catch (Exception ex) {
			model.addAttribute("flash", "Error al migrar (universal): " + ex.getMessage());
		} finally {
			dataSourceRegistry.closeQuietly(origen);
			dataSourceRegistry.closeQuietly(destino);
		}

		return index(model, dbOrigenName, dbOrigenUrl, dbOrigenUsername, dbOrigenPassword, dbOrigenDriver,
			dbDestinoName, dbDestinoUrl, dbDestinoUsername, dbDestinoPassword, dbDestinoDriver,
			sql, destSchema, destTable, mappingsJson);
	}

	private List<FieldMapping> parseMappings(String mappingsJson) throws Exception {
		if (mappingsJson == null || mappingsJson.isBlank()) {
			return List.of();
		}
		return objectMapper.readValue(mappingsJson, new TypeReference<List<FieldMapping>>() {
		});
	}

	private String extractDbNameFromError(String errorMsg) {
		int start = errorMsg.indexOf('"');
		int end = errorMsg.indexOf('"', start + 1);
		if (start >= 0 && end > start) {
			return errorMsg.substring(start + 1, end);
		}
		return "desconocida";
	}

	private String testAndGetConnectedDb(ConnectionProfile profile) throws Exception {
		HikariDataSource ds = null;
		try {
			ds = dataSourceRegistry.createDataSource(profile);
			try (var connection = ds.getConnection()) {
				int timeout = Math.max(1, profile.getValidationTimeoutSeconds());
				boolean ok = connection.isValid(timeout);
				if (!ok) {
					throw new IllegalStateException("La conexión respondió inválida");
				}
				String connectedDb = databaseFromJdbcUrl(profile.getJdbcUrl());
				if (profile.getJdbcUrl() != null && profile.getJdbcUrl().startsWith("jdbc:postgresql:")) {
					try (var ps = connection.prepareStatement("select current_database()")) {
						try (var rs = ps.executeQuery()) {
							if (rs.next()) {
								connectedDb = rs.getString(1);
							}
						}
					} catch (Exception ignored) {
					}
				}
				return connectedDb != null ? connectedDb : "OK";
			}
		} finally {
			dataSourceRegistry.closeQuietly(ds);
		}
	}

	private static String databaseFromJdbcUrl(String jdbcUrl) {
		if (jdbcUrl == null) {
			return null;
		}
		int slash = jdbcUrl.lastIndexOf('/');
		if (slash < 0 || slash == jdbcUrl.length() - 1) {
			return null;
		}
		String tail = jdbcUrl.substring(slash + 1);
		int q = tail.indexOf('?');
		return q >= 0 ? tail.substring(0, q) : tail;
	}

	private List<String> columnsFromRows(List<Map<String, Object>> rows) {
		if (rows == null || rows.isEmpty()) {
			return List.of();
		}
		return rows.get(0).keySet().stream().toList();
	}

	private String wrapSqlWithLimit(String sql, int limit) {
		String s = sanitizeSql(sql);
		if (s.isEmpty()) {
			throw new IllegalArgumentException("SQL vacío");
		}
		return "select * from (" + s + ") t limit " + limit;
	}

	private static String sanitizeSqlQuietly(String sql) {
		try {
			return sanitizeSql(sql);
		} catch (Exception ex) {
			return sql == null ? "" : sql;
		}
	}

	private static String sanitizeSql(String sql) {
		String s = sql == null ? "" : sql.trim();
		if (s.isEmpty()) {
			return s;
		}
		while (s.endsWith(";")) {
			s = s.substring(0, s.length() - 1).trim();
		}
		if (s.contains(";")) {
			throw new IllegalArgumentException("El SQL no debe contener ';' (solo una sentencia)");
		}
		return s;
	}
}
