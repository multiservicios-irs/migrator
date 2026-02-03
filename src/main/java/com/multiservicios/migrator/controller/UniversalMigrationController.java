package com.multiservicios.migrator.controller;

import java.util.List;

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
		return "universal";
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
}
