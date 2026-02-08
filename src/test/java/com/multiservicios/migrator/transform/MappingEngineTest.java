package com.multiservicios.migrator.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.multiservicios.migrator.config.MigratorProperties;
import com.multiservicios.migrator.model.FieldMapping;
import com.multiservicios.migrator.model.MappingType;
import com.multiservicios.migrator.model.RowData;

class MappingEngineTest {
	@Test
	void mapToProducto_coercesDoubleToBigDecimal_andTrimsTarget() {
		MigratorProperties props = new MigratorProperties();
		RuleProcessor ruleProcessor = new RuleProcessor(props);
		MappingEngine engine = new MappingEngine(ruleProcessor);

		RowData row = new RowData(Map.of(
				"venta1", 180000.00d,
				"venta2", 150000.00d,
				"venta3", 130000.00d
		));

		FieldMapping minorista = new FieldMapping();
		minorista.setType(MappingType.DIRECTO);
		minorista.setSource("venta1");
		minorista.setTarget(" producto.precioMinorista ");

		FieldMapping mayorista = new FieldMapping();
		mayorista.setType(MappingType.DIRECTO);
		mayorista.setSource("venta2");
		mayorista.setTarget("producto.precioMayorista");

		FieldMapping credito = new FieldMapping();
		credito.setType(MappingType.DIRECTO);
		credito.setSource("venta3");
		credito.setTarget("producto.precioCredito");

		var dto = engine.mapToProducto(row, List.of(minorista, mayorista, credito));
		assertNotNull(dto);
		assertNotNull(dto.getProducto());
		assertEquals("180000.0", dto.getProducto().getPrecioMinorista().toPlainString());
		assertEquals("150000.0", dto.getProducto().getPrecioMayorista().toPlainString());
		assertEquals("130000.0", dto.getProducto().getPrecioCredito().toPlainString());
	}

	@Test
	void universal_likeMapping_allowsBooleanLiteralInDirectType_whenColumnDoesNotExist() {
		MigratorProperties props = new MigratorProperties();
		RuleProcessor ruleProcessor = new RuleProcessor(props);
		UniversalMappingEngine engine = new UniversalMappingEngine(ruleProcessor);

		RowData row = new RowData(Map.of(
				"ruc", "80036027-3",
				"nombre", "INK-NDM SRL"
		));

		FieldMapping activo = new FieldMapping();
		activo.setType(MappingType.DIRECTO);
		activo.setSource("true");
		activo.setTarget("activo");

		var values = engine.mapToColumns(row, List.of(activo));
		assertTrue(values.containsKey("activo"));
		assertEquals(Boolean.TRUE, values.get("activo"));
	}

	@Test
	void universal_likeMapping_supportsNowRule_forTimestamps() {
		MigratorProperties props = new MigratorProperties();
		RuleProcessor ruleProcessor = new RuleProcessor(props);
		UniversalMappingEngine engine = new UniversalMappingEngine(ruleProcessor);

		RowData row = new RowData(Map.of("ruc", "80036027-3"));

		FieldMapping createdAt = new FieldMapping();
		createdAt.setType(MappingType.DIRECTO);
		createdAt.setSource("");
		createdAt.setTarget("created_at");
		createdAt.setRule("now");

		var values = engine.mapToColumns(row, List.of(createdAt));
		assertTrue(values.containsKey("created_at"));
		assertNotNull(values.get("created_at"));
	}
}
