package com.multiservicios.migrator.engine;

import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.springframework.stereotype.Component;

import com.multiservicios.migrator.extract.SqlExtractor;
import com.multiservicios.migrator.load.UniversalTableLoader;
import com.multiservicios.migrator.logs.MigrationLogService;
import com.multiservicios.migrator.model.FieldMapping;
import com.multiservicios.migrator.model.RowData;
import com.multiservicios.migrator.transform.UniversalMappingEngine;

@Component
public class UniversalMigrationRunner {
	private final SqlExtractor extractor;
	private final UniversalMappingEngine mappingEngine;
	private final UniversalTableLoader loader;
	private final MigrationLogService logService;

	public UniversalMigrationRunner(SqlExtractor extractor, UniversalMappingEngine mappingEngine, UniversalTableLoader loader,
			MigrationLogService logService) {
		this.extractor = extractor;
		this.mappingEngine = mappingEngine;
		this.loader = loader;
		this.logService = logService;
	}

	public UniversalRunResult run(DataSource origen, DataSource destino, String sql, List<FieldMapping> mappings,
			String schema, String table, boolean dryRun) {
		logService.clear();
		List<RowData> rows = extractor.extract(origen, sql);

		Set<String> allowed;
		try {
			allowed = loader.loadAllowedColumnsLower(destino, schema, table);
		} catch (Exception ex) {
			logService.error(0, "No se pudo leer columnas del destino: " + ex.getMessage());
			return new UniversalRunResult(rows.size(), 0, rows.size(), dryRun);
		}

		int ok = 0;
		int fail = 0;
		for (int i = 0; i < rows.size(); i++) {
			int rowNumber = i + 1;
			try {
				var values = mappingEngine.mapToColumns(rows.get(i), mappings);
				var result = loader.insertRow(destino, schema, table, values, allowed, dryRun);
				if (result.success()) {
					ok++;
					logService.info(rowNumber, result.message());
				} else {
					fail++;
					logService.error(rowNumber, result.message());
				}
			} catch (Exception ex) {
				fail++;
				logService.error(rowNumber, ex.getClass().getSimpleName() + ": " + ex.getMessage());
			}
		}

		return new UniversalRunResult(rows.size(), ok, fail, dryRun);
	}

	public record UniversalRunResult(int total, int ok, int fail, boolean dryRun) {
	}
}
