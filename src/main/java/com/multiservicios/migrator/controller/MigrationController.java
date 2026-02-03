package com.multiservicios.migrator.controller;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.multiservicios.migrator.config.ConnectionProfile;
import com.multiservicios.migrator.config.DataSourceRegistry;
import com.multiservicios.migrator.config.MigratorProperties;
import com.multiservicios.migrator.engine.MigrationRunner;
import com.multiservicios.migrator.extract.SqlExtractor;
import com.multiservicios.migrator.logs.MigrationLogService;
import com.multiservicios.migrator.model.FieldMapping;
import com.zaxxer.hikari.HikariDataSource;

@Controller
public class MigrationController {
	private final MigratorProperties properties;
	private final DataSourceRegistry dataSourceRegistry;
	private final SqlExtractor extractor;
	private final MigrationRunner runner;
	private final MigrationLogService logService;
	private final ObjectMapper objectMapper;

	public MigrationController(
			MigratorProperties properties,
			DataSourceRegistry dataSourceRegistry,
			SqlExtractor extractor,
			MigrationRunner runner,
			MigrationLogService logService,
			ObjectMapper objectMapper) {
		this.properties = properties;
		this.dataSourceRegistry = dataSourceRegistry;
		this.extractor = extractor;
		this.runner = runner;
		this.logService = logService;
		this.objectMapper = objectMapper;
	}

	@GetMapping("/")
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
			@RequestParam(name = "mappings", required = false) String mappings) {
		ConnectionProfile fallbackOrigen = properties.getProfiles().isEmpty() ? null : properties.getProfiles().get(0);
		ConnectionProfile fallbackDestino = properties.getProfiles().size() > 1 ? properties.getProfiles().get(1) : null;

		model.addAttribute("dbOrigenName",
				coalesce(dbOrigenName, fallbackOrigen != null ? fallbackOrigen.getName() : "deliStore"));
		model.addAttribute("dbOrigenUrl",
				coalesce(dbOrigenUrl, fallbackOrigen != null ? fallbackOrigen.getJdbcUrl() : "jdbc:postgresql://localhost:5432/deliStore"));
		model.addAttribute("dbOrigenUsername",
				coalesce(dbOrigenUsername, fallbackOrigen != null ? fallbackOrigen.getUsername() : "postgres"));
		model.addAttribute("dbOrigenPassword",
				coalesce(dbOrigenPassword, fallbackOrigen != null ? fallbackOrigen.getPassword() : ""));
		model.addAttribute("dbOrigenDriver",
				coalesce(dbOrigenDriver, fallbackOrigen != null ? fallbackOrigen.getDriverClassName() : "org.postgresql.Driver"));

		model.addAttribute("dbDestinoName",
				coalesce(dbDestinoName, fallbackDestino != null ? fallbackDestino.getName() : "prueba_db"));
		model.addAttribute("dbDestinoUrl",
				coalesce(dbDestinoUrl, fallbackDestino != null ? fallbackDestino.getJdbcUrl() : "jdbc:postgresql://localhost:5432/prueba_db"));
		model.addAttribute("dbDestinoUsername",
				coalesce(dbDestinoUsername, fallbackDestino != null ? fallbackDestino.getUsername() : "postgres"));
		model.addAttribute("dbDestinoPassword",
				coalesce(dbDestinoPassword, fallbackDestino != null ? fallbackDestino.getPassword() : ""));
		model.addAttribute("dbDestinoDriver",
				coalesce(dbDestinoDriver, fallbackDestino != null ? fallbackDestino.getDriverClassName() : "org.postgresql.Driver"));
		model.addAttribute("sql", sql == null ? "" : sql);
		model.addAttribute("mappings", mappings == null ? defaultMappingsJson() : mappings);
		model.addAttribute("defaults", properties.getDefaults());
		model.addAttribute("targetFields", targetFields());
		if (!model.asMap().containsKey("previewColumns")) {
			model.addAttribute("previewColumns", List.of());
		}
		return "index";
	}

	private static String coalesce(String value, String fallback) {
		if (value == null) {
			return fallback;
		}
		String trimmed = value.trim();
		return trimmed.isEmpty() ? fallback : value;
	}

	@PostMapping("/test-connection")
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
			// Simplificar mensajes de error comunes
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
				sql, mappings);
	}

	private String extractDbNameFromError(String errorMsg) {
		// Extraer nombre de DB del mensaje "database \"xxx\" does not exist"
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

				// Obtener nombre real de la DB conectada
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

	@PostMapping("/preview")
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
			model.addAttribute("flash", "Error al previsualizar: " + ex.getMessage());
		} finally {
			dataSourceRegistry.closeQuietly(ds);
		}
		return index(model, dbOrigenName, dbOrigenUrl, dbOrigenUsername, dbOrigenPassword, dbOrigenDriver,
				dbDestinoName, dbDestinoUrl, dbDestinoUsername, dbDestinoPassword, dbDestinoDriver,
				sanitizeSqlQuietly(sql), mappingsJson);
	}

	@PostMapping("/run")
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
			@RequestParam("mappings") String mappingsJson,
			@RequestParam(name = "dryRun", defaultValue = "true") boolean dryRun,
			Model model) {
		HikariDataSource origen = null;
		HikariDataSource destino = null;
		try {
			String sanitizedSql = sanitizeSql(sql);
			List<FieldMapping> mappings = parseMappings(mappingsJson);
			ConnectionProfile pOrigen = new ConnectionProfile();
			pOrigen.setName(dbOrigenName);
			pOrigen.setJdbcUrl(dbOrigenUrl);
			pOrigen.setUsername(dbOrigenUsername);
			pOrigen.setPassword(dbOrigenPassword);
			pOrigen.setDriverClassName(dbOrigenDriver);
			pOrigen.setValidationTimeoutSeconds(5);
			origen = dataSourceRegistry.createDataSource(pOrigen);

			// Preflight: obtener columnas reales del SQL (1 fila) para que la UI pueda sugerir
			// y para validar que los mapeos DIRECTO apunten a columnas existentes.
			try {
				String limitedSql = wrapSqlWithLimit(sanitizedSql, 1);
				JdbcTemplate jdbcTemplate = new JdbcTemplate(origen);
				var previewRows = jdbcTemplate.queryForList(limitedSql);
				model.addAttribute("previewRows", previewRows);
				model.addAttribute("previewColumns", columnsFromRows(previewRows));
			} catch (Exception ignored) {
				// Si falla el preview no bloqueamos acá: el runner dará el error real.
			}

			// Validación fuerte: producto.nombre debe venir de una columna existente.
			validateMappingsAgainstPreviewOrThrow(mappings, model);

			ConnectionProfile pDestino = new ConnectionProfile();
			pDestino.setName(dbDestinoName);
			pDestino.setJdbcUrl(dbDestinoUrl);
			pDestino.setUsername(dbDestinoUsername);
			pDestino.setPassword(dbDestinoPassword);
			pDestino.setDriverClassName(dbDestinoDriver);
			pDestino.setValidationTimeoutSeconds(5);
			destino = dataSourceRegistry.createDataSource(pDestino);

			var result = runner.run(origen, destino, sanitizedSql, mappings, dryRun);
			model.addAttribute("runResult", result);
			model.addAttribute("runLogs", logService.snapshot());
		} catch (Exception ex) {
			model.addAttribute("flash", "Error al migrar: " + ex.getMessage());
		} finally {
			dataSourceRegistry.closeQuietly(origen);
			dataSourceRegistry.closeQuietly(destino);
		}
		return index(model, dbOrigenName, dbOrigenUrl, dbOrigenUsername, dbOrigenPassword, dbOrigenDriver,
				dbDestinoName, dbDestinoUrl, dbDestinoUsername, dbDestinoPassword, dbDestinoDriver,
				sanitizeSqlQuietly(sql), mappingsJson);
	}

	private void validateMappingsAgainstPreviewOrThrow(List<FieldMapping> mappings, Model model) {
		List<String> cols = columnsFromRows(model.asMap().get("previewRows"));
		if (cols == null || cols.isEmpty()) {
			// Sin columnas no podemos validar. El runner validará por datos.
			return;
		}
		var colsLower = cols.stream().map(s -> s == null ? "" : s.toLowerCase()).collect(java.util.stream.Collectors.toSet());

		FieldMapping nombreMapping = null;
		for (FieldMapping m : mappings) {
			if (m == null) {
				continue;
			}
			String target = m.getTarget();
			String type = m.getType() == null ? "" : m.getType().name();
			if (target != null && target.equalsIgnoreCase("producto.nombre") && "DIRECTO".equalsIgnoreCase(type)) {
				nombreMapping = m;
				break;
			}
		}

		if (nombreMapping == null) {
			throw new IllegalArgumentException("Falta el mapeo requerido: producto.nombre");
		}
		String nombreSource = nombreMapping.getSource() == null ? "" : nombreMapping.getSource().trim();
		if (nombreSource.isEmpty()) {
			throw new IllegalArgumentException("El mapeo producto.nombre (Directo) no tiene Campo Origen. Elegí una columna del Preview (ej: 'descripcion').");
		}
		if (!colsLower.contains(nombreSource.toLowerCase())) {
			throw new IllegalArgumentException("El Campo Origen '" + nombreSource + "' no existe en el Preview. Corregí el mapeo de producto.nombre.");
		}

		// Validar el resto de DIRECTO con source no vacío
		for (FieldMapping m : mappings) {
			if (m == null) {
				continue;
			}
			if (m.getType() == null || m.getType().name() == null) {
				continue;
			}
			if (!"DIRECTO".equalsIgnoreCase(m.getType().name())) {
				continue;
			}
			String src = m.getSource() == null ? "" : m.getSource().trim();
			if (src.isEmpty()) {
				continue;
			}
			if (!colsLower.contains(src.toLowerCase())) {
				String target = m.getTarget() == null ? "" : m.getTarget().trim();
				throw new IllegalArgumentException("El Campo Origen '" + src + "' no existe en el Preview (target='" + target + "').");
			}
		}
	}

	private List<FieldMapping> parseMappings(String mappingsJson) throws Exception {
		if (mappingsJson == null || mappingsJson.isBlank()) {
			return List.of();
		}
		return objectMapper.readValue(mappingsJson, new TypeReference<List<FieldMapping>>() {
		});
	}

	private String defaultMappingsJson() {
		// No asumir nombres de columnas del origen (ej: NOMBRE/CANTIDAD). La UI puede auto-sugerir
		// cuando existe preview, o el usuario elige la columna real.
		return "[\n" +
				"  { \"target\": \"producto.nombre\", \"type\": \"DIRECTO\" },\n" +
				"  { \"target\": \"stockPorDeposito[0].cantidad\", \"type\": \"DIRECTO\" },\n" +
				"  { \"target\": \"stockPorDeposito[0].depositoId\", \"type\": \"DEFAULT\", \"value\": \"depositoCentralId\" }\n" +
				"]";
	}

	private List<String> targetFields() {
		boolean includeAudit = properties.getProductos() != null && properties.getProductos().isIncludeAuditColumns();
		var fields = new java.util.ArrayList<String>();
		Collections.addAll(fields,
				// ProductoDto (campos realmente usados por el loader)
				"producto.tipo",
				"producto.codigoBarra",
				"producto.nombre",
				"producto.descripcion",
				"producto.marcaId",
				"producto.unidadMedidaId",
				"producto.categoriaId",
				"producto.precioDeCompra",
				"producto.precioMinorista",
				"producto.precioMayorista",
				"producto.precioCredito",
				"producto.ivaPercent",
				"producto.stockMin",
				"producto.serializable",
				"producto.imagenUrl",

				// Stock (inventario inicial)
				"stock",
				"depositoId",
				"stockPorDeposito[0].depositoId",
				"stockPorDeposito[0].cantidad",
				"stockPorDeposito[0].reservado"
		);
		if (includeAudit) {
			Collections.addAll(fields,
					"producto.activo",
					"producto.createdBy",
					"producto.updatedBy"
			);
		}
		return fields;
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
		// En Postgres y muchos motores funciona: SELECT * FROM (<sql>) t LIMIT n
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
		// Quita ';' finales (muy común al copiar/pegar) porque rompe al envolver en subquery.
		while (s.endsWith(";")) {
			s = s.substring(0, s.length() - 1).trim();
		}
		// Evita múltiples sentencias.
		if (s.contains(";")) {
			throw new IllegalArgumentException("El SQL no debe contener ';' (solo una sentencia)");
		}
		return s;
	}

	@SuppressWarnings("unchecked")
	private List<String> columnsFromRows(Object rowsObj) {
		if (rowsObj instanceof List<?> list && !list.isEmpty() && list.get(0) instanceof Map<?, ?> first) {
			return first.keySet().stream().map(Object::toString).toList();
		}
		return Collections.emptyList();
	}
}
