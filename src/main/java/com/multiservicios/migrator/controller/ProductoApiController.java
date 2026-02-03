package com.multiservicios.migrator.controller;

import javax.sql.DataSource;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.multiservicios.migrator.config.DataSourceRegistry;
import com.multiservicios.migrator.config.MigratorProperties;
import com.multiservicios.migrator.dto.CreateProductoResponse;
import com.multiservicios.migrator.dto.ProductoMigrationDto;
import com.multiservicios.migrator.load.ProductoLoader;

@RestController
@RequestMapping("/api/productos")
public class ProductoApiController {
	private final MigratorProperties properties;
	private final DataSourceRegistry dataSourceRegistry;
	private final ProductoLoader productoLoader;

	public ProductoApiController(MigratorProperties properties, DataSourceRegistry dataSourceRegistry, ProductoLoader productoLoader) {
		this.properties = properties;
		this.dataSourceRegistry = dataSourceRegistry;
		this.productoLoader = productoLoader;
	}

	@PostMapping
	public ResponseEntity<CreateProductoResponse> createProducto(
			@RequestBody ProductoMigrationDto request,
			@RequestParam(name = "destinoProfile", required = false) String destinoProfile,
			@RequestParam(name = "dryRun", defaultValue = "false") boolean dryRun) {
		String profileName = resolveDestinoProfileName(destinoProfile);
		DataSource destino = dataSourceRegistry.getRequiredDataSource(profileName);

		var result = productoLoader.createWithInitialStockBestEffort(destino, request, dryRun);
		if (!result.isSuccess()) {
			return ResponseEntity.badRequest().body(CreateProductoResponse.fail(result.getMessage()));
		}
		return ResponseEntity.ok(CreateProductoResponse.ok(result.getMessage(), result.getCreatedId()));
	}

	private String resolveDestinoProfileName(String destinoProfile) {
		if (destinoProfile != null && !destinoProfile.isBlank()) {
			return destinoProfile.trim();
		}
		if (properties.getProfiles().size() > 1 && properties.getProfiles().get(1).getName() != null) {
			return properties.getProfiles().get(1).getName();
		}
		if (!properties.getProfiles().isEmpty() && properties.getProfiles().get(0).getName() != null) {
			return properties.getProfiles().get(0).getName();
		}
		throw new IllegalStateException("No hay perfiles migrator.profiles configurados");
	}
}
