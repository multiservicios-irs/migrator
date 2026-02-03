package com.multiservicios.migrator.session;

import java.util.UUID;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.multiservicios.migrator.config.MigratorProperties;
import com.multiservicios.migrator.client.MultiServiciosProductoClient;
import com.multiservicios.migrator.dto.CreateProductoResponse;
import com.multiservicios.migrator.dto.ProductoCreateRequest;
import com.multiservicios.migrator.transform.ProductoTransformService;

@Service
public class MigrationProcessorService {
	private final SqlBufferService bufferService;
	private final ProductoTransformService transformService;
	private final MultiServiciosProductoClient productoClient;
	private final MigratorProperties properties;

	public MigrationProcessorService(
			SqlBufferService bufferService,
			ProductoTransformService transformService,
			MultiServiciosProductoClient productoClient,
			MigratorProperties properties) {
		this.bufferService = bufferService;
		this.transformService = transformService;
		this.productoClient = productoClient;
		this.properties = properties;
	}

	public MigrationSessionStatus runNext(UUID sessionId, boolean dryRun) {
		MigrationSession session = bufferService.getRequired(sessionId);
		BufferedRow bufferedRow = session.pollNext();
		if (bufferedRow == null) {
			return bufferService.status(sessionId, 50);
		}

		Map<String, Object> row = bufferedRow.getRow();
		int maxAttempts = Math.max(1, properties.getSession().getMaxAttempts());

		try {
			Object payload = switch (properties.getDestino().getContract()) {
				case NESTED -> transformService.toProductoMigrationDto(row);
				case FLAT -> transformService.toCreateProductoRequest(row);
			};
			CreateProductoResponse resp = productoClient.createProducto(payload, dryRun);
			if (resp == null) {
				session.registerFailure(bufferedRow, "Respuesta null del destino", null, maxAttempts);
				return bufferService.status(sessionId, 50);
			}
			if (resp.success()) {
				session.markOk(resp.message(), resp.id() == null ? null : ("id=" + resp.id()));
				return bufferService.status(sessionId, 50);
			}
			session.registerFailure(bufferedRow, resp.message(), null, maxAttempts);
			return bufferService.status(sessionId, 50);
		} catch (Exception ex) {
			session.registerFailure(bufferedRow, ex.getClass().getSimpleName() + ": " + ex.getMessage(), null, maxAttempts);
			return bufferService.status(sessionId, 50);
		}
	}

	public MigrationSessionStatus runAll(UUID sessionId, boolean dryRun, Integer maxItems) {
		MigrationSession session = bufferService.getRequired(sessionId);
		int pendingAtStart = session.getPendingCount();
		int limit = maxItems == null ? pendingAtStart : Math.max(0, maxItems);
		int toProcess = Math.min(pendingAtStart, limit);

		for (int i = 0; i < toProcess; i++) {
			runNext(sessionId, dryRun);
		}
		return bufferService.status(sessionId, 50);
	}
}
