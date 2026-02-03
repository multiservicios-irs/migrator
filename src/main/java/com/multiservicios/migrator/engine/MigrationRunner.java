package com.multiservicios.migrator.engine;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.stereotype.Component;

import com.multiservicios.migrator.config.DataSourceRegistry;
import com.multiservicios.migrator.dto.ProductoMigrationDto;
import com.multiservicios.migrator.extract.SqlExtractor;
import com.multiservicios.migrator.load.ProductoLoader;
import com.multiservicios.migrator.logs.MigrationLogService;
import com.multiservicios.migrator.model.FieldMapping;
import com.multiservicios.migrator.model.RowData;
import com.multiservicios.migrator.transform.MappingEngine;

@Component
public class MigrationRunner {
	private final DataSourceRegistry dataSourceRegistry;
	private final SqlExtractor extractor;
	private final MappingEngine mappingEngine;
	private final ProductoLoader productoLoader;
	private final MigrationLogService logService;

	public MigrationRunner(
			DataSourceRegistry dataSourceRegistry,
			SqlExtractor extractor,
			MappingEngine mappingEngine,
			ProductoLoader productoLoader,
			MigrationLogService logService) {
		this.dataSourceRegistry = dataSourceRegistry;
		this.extractor = extractor;
		this.mappingEngine = mappingEngine;
		this.productoLoader = productoLoader;
		this.logService = logService;
	}

	public MigrationRunResult run(DataSource origen, DataSource destino, String sql, List<FieldMapping> mappings, boolean dryRun) {
		logService.clear();
		List<RowData> rows = extractor.extract(origen, sql);

		int ok = 0;
		int fail = 0;
		for (int i = 0; i < rows.size(); i++) {
			int rowNumber = i + 1;
			try {
				ProductoMigrationDto dto = mappingEngine.mapToProducto(rows.get(i), mappings);
				var pr = productoLoader.load(destino, dto, dryRun);
				if (pr.isSuccess()) {
					ok++;
					logService.info(rowNumber, pr.getMessage());
				} else {
					fail++;
					logService.error(rowNumber, pr.getMessage());
				}
			} catch (Exception ex) {
				fail++;
				logService.error(rowNumber, ex.getClass().getSimpleName() + ": " + ex.getMessage());
			}
		}
		return new MigrationRunResult(rows.size(), ok, fail, dryRun);
	}

	public record MigrationRunResult(int total, int ok, int fail, boolean dryRun) {
	}
}
