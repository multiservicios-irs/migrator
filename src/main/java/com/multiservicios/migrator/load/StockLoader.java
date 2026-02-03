package com.multiservicios.migrator.load;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class StockLoader {
	private static final Logger log = LoggerFactory.getLogger(StockLoader.class);

	public LoadResult loadSimple(BigDecimal cantidad, Long depositoId, boolean dryRun) {
		if (dryRun) {
			return LoadResult.ok("DRY-RUN stock: cantidad=" + cantidad + " depositoId=" + depositoId);
		}
		log.info("Crear stock en destino: cantidad={} depositoId={}", cantidad, depositoId);
		return LoadResult.ok("Stock creado (stub): cantidad=" + cantidad);
	}
}
