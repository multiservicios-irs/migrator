package com.multiservicios.migrator.transform;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.multiservicios.migrator.config.MigratorProperties;
import com.multiservicios.migrator.dto.ProductoCreateRequest;
import com.multiservicios.migrator.dto.ProductoCreateRequest.StockPorDepositoItem;
import com.multiservicios.migrator.dto.ProductoMigrationDto;
import com.multiservicios.migrator.dto.TipoItem;

@Service
public class ProductoTransformService {
	private final MigratorProperties properties;
	private final ObjectMapper objectMapper;

	public ProductoTransformService(MigratorProperties properties, ObjectMapper objectMapper) {
		this.properties = properties;
		this.objectMapper = objectMapper;
	}

	public ProductoCreateRequest toCreateProductoRequest(Map<String, Object> row) {
		if (row == null) {
			throw new IllegalArgumentException("Fila null");
		}

		ProductoCreateRequest req = new ProductoCreateRequest();

		String tipo = normalizeTipo(asString(getIgnoreCase(row, "tipo")));
		if (tipo == null) {
			tipo = "PRODUCTO";
		}

		validateExtractContractV1(row, tipo);

		req.setTipo(tipo);
		req.setNombre(normalizeNombre(asString(getIgnoreCase(row, "nombre"))));
		req.setPrecioMinorista(asBigDecimal(getIgnoreCase(row, "precioMinorista")));
		req.setIvaPercent(asBigDecimal(getIgnoreCase(row, "ivaPercent")));

		// Regla: si tipo != PRODUCTO -> NO mandar stock
		if (!"PRODUCTO".equalsIgnoreCase(tipo)) {
			req.setDepositoId(null);
			req.setStock(null);
			req.setStockPorDeposito(null);
			validate(req);
			return req;
		}

		// Prioridad: si stockPorDeposito viene informado, NO mandar stock+depositoId
		List<StockPorDepositoItem> stockPorDeposito = parseStockPorDeposito(getIgnoreCase(row, "stockPorDeposito"));
		if (stockPorDeposito != null && !stockPorDeposito.isEmpty()) {
			req.setStockPorDeposito(stockPorDeposito);
			req.setDepositoId(null);
			req.setStock(null);
			validate(req);
			return req;
		}

		// Forma A (simple): deposito central + stock
		BigDecimal stock = asBigDecimal(getIgnoreCase(row, "stock"));
		if (stock == null) {
			stock = BigDecimal.ZERO;
		}
		Long depositoId = asLong(getIgnoreCase(row, "depositoId"));
		if (depositoId == null) {
			depositoId = requiredDefault(properties.getDefaults().getDepositoCentralId(), "depositoCentralId");
		}
		req.setDepositoId(depositoId);
		req.setStock(stock);
		req.setStockPorDeposito(null);

		validate(req);
		return req;
	}

	public ProductoMigrationDto toProductoMigrationDto(Map<String, Object> row) {
		if (row == null) {
			throw new IllegalArgumentException("Fila null");
		}

		String tipoStr = normalizeTipo(asString(getIgnoreCase(row, "tipo")));
		TipoItem tipo = TipoItem.PRODUCTO;
		if (tipoStr != null) {
			try {
				tipo = TipoItem.valueOf(tipoStr);
			} catch (Exception ignored) {
				// fallback PRODUCTO
			}
		}

		validateExtractContractV1(row, tipo.name());

		ProductoMigrationDto dto = new ProductoMigrationDto();
		ProductoMigrationDto.ProductoDto producto = new ProductoMigrationDto.ProductoDto();
		producto.setTipo(tipo);

		// Defaults del destino (cuando aplica) - se puede sobrescribir desde el SQL (aliases: marcaId/categoriaId/unidadMedidaId)
		Long marcaId = asLong(getIgnoreCase(row, "marcaId"));
		if (marcaId == null) {
			marcaId = requiredDefault(properties.getDefaults().getMarcaGenericaId(), "marcaGenericaId");
		}
		Long categoriaId = asLong(getIgnoreCase(row, "categoriaId"));
		if (categoriaId == null) {
			categoriaId = requiredDefault(properties.getDefaults().getCategoriaGenericaId(), "categoriaGenericaId");
		}
		Long unidadMedidaId = asLong(getIgnoreCase(row, "unidadMedidaId"));
		if (unidadMedidaId == null) {
			unidadMedidaId = requiredDefault(properties.getDefaults().getUnidadMedidaGenericaId(), "unidadMedidaGenericaId");
		}
		producto.setMarcaId(marcaId);
		producto.setCategoriaId(categoriaId);
		producto.setUnidadMedidaId(unidadMedidaId);

		producto.setNombre(normalizeNombre(asString(getIgnoreCase(row, "nombre"))));
		producto.setCodigoBarra(normalizeCodigoBarra(asString(getIgnoreCase(row, "codigoBarra"))));
		producto.setPrecioMinorista(asBigDecimal(getIgnoreCase(row, "precioMinorista")));
		producto.setPrecioDeCompra(asBigDecimal(getIgnoreCase(row, "precioDeCompra")));
		producto.setIvaPercent(asBigDecimal(getIgnoreCase(row, "ivaPercent")));

		dto.setProducto(producto);

		// Regla: si tipo != PRODUCTO -> NO mandar stock
		if (tipo != TipoItem.PRODUCTO) {
			dto.setDepositoId(null);
			dto.setStock(null);
			dto.setStockPorDeposito(new java.util.ArrayList<>());
			validateNested(dto);
			return dto;
		}

		// Prioridad: stockPorDeposito > stock
		List<ProductoMigrationDto.StockPorDepositoDto> stockPorDeposito = parseStockPorDepositoNested(getIgnoreCase(row, "stockPorDeposito"));
		if (stockPorDeposito != null && !stockPorDeposito.isEmpty()) {
			dto.setStockPorDeposito(stockPorDeposito);
			dto.setDepositoId(null);
			dto.setStock(null);
			validateNested(dto);
			return dto;
		}

		BigDecimal stock = asBigDecimal(getIgnoreCase(row, "stock"));
		if (stock == null) {
			stock = BigDecimal.ZERO;
		}
		Long depositoId = asLong(getIgnoreCase(row, "depositoId"));
		if (depositoId == null) {
			depositoId = requiredDefault(properties.getDefaults().getDepositoCentralId(), "depositoCentralId");
		}
		dto.setDepositoId(depositoId);
		dto.setStock(stock);
		validateNested(dto);
		return dto;
	}

	private static Long asLong(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Long l) {
			return l;
		}
		if (value instanceof Integer i) {
			return i.longValue();
		}
		if (value instanceof Number n) {
			return n.longValue();
		}
		String s = String.valueOf(value).trim();
		if (s.isEmpty()) {
			return null;
		}
		try {
			return Long.parseLong(s);
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException("No se pudo convertir a long: '" + s + "'");
		}
	}

	private void validate(ProductoCreateRequest req) {
		String nombre = trimToNull(req.getNombre());
		if (nombre == null) {
			throw new IllegalArgumentException("nombre es requerido (alias: nombre)");
		}
		if (req.getPrecioMinorista() == null) {
			throw new IllegalArgumentException("precioMinorista es requerido (alias: precioMinorista)");
		}
		if ("PRODUCTO".equalsIgnoreCase(req.getTipo())) {
			if (req.getStockPorDeposito() != null && !req.getStockPorDeposito().isEmpty()) {
				return;
			}
			if (req.getDepositoId() == null) {
				throw new IllegalArgumentException("depositoId requerido para tipo PRODUCTO (default: depositoCentralId)");
			}
			if (req.getStock() == null) {
				throw new IllegalArgumentException("stock requerido para tipo PRODUCTO (alias: stock)");
			}
		}
	}

	private void validateNested(ProductoMigrationDto dto) {
		if (dto.getProducto() == null) {
			throw new IllegalArgumentException("producto requerido");
		}
		String nombre = trimToNull(dto.getProducto().getNombre());
		if (nombre == null) {
			throw new IllegalArgumentException("producto.nombre es requerido (alias: nombre)");
		}
		if (dto.getProducto().getPrecioMinorista() == null) {
			throw new IllegalArgumentException("producto.precioMinorista es requerido (alias: precioMinorista)");
		}
		if (dto.getProducto().getTipo() == TipoItem.PRODUCTO) {
			boolean hasStockPorDeposito = dto.getStockPorDeposito() != null && !dto.getStockPorDeposito().isEmpty();
			if (hasStockPorDeposito) {
				return;
			}
			if (dto.getDepositoId() == null) {
				throw new IllegalArgumentException("depositoId requerido para tipo PRODUCTO (default: depositoCentralId)");
			}
			if (dto.getStock() == null) {
				throw new IllegalArgumentException("stock requerido para tipo PRODUCTO (alias: stock)");
			}
		}
	}

	private static void validateExtractContractV1(Map<String, Object> row, String tipo) {
		List<String> missing = new ArrayList<>();
		// Requeridos para el destino
		if (!containsKeyIgnoreCase(row, "nombre")) {
			missing.add("nombre");
		}
		if (!containsKeyIgnoreCase(row, "precioMinorista")) {
			missing.add("precioMinorista");
		}

		// Para PRODUCTO, requerimos stock (Forma A) o stockPorDeposito (Forma B)
		if ("PRODUCTO".equalsIgnoreCase(tipo)) {
			boolean hasStock = containsKeyIgnoreCase(row, "stock");
			boolean hasStockPorDeposito = containsKeyIgnoreCase(row, "stockPorDeposito");
			if (!hasStock && !hasStockPorDeposito) {
				missing.add("stock (o stockPorDeposito)");
			}
		}
		// Opcionales: ivaPercent, tipo

		if (!missing.isEmpty()) {
			throw new IllegalArgumentException(
					"El SQL no expone los alias requeridos (Extract Contract v1): " + missing
							+ "; aliases presentes: " + row.keySet());
		}
	}

	private List<StockPorDepositoItem> parseStockPorDeposito(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof List<?> list) {
			try {
				JavaType type = objectMapper.getTypeFactory().constructCollectionType(List.class, StockPorDepositoItem.class);
				return objectMapper.convertValue(list, type);
			} catch (IllegalArgumentException ex) {
				throw new IllegalArgumentException("No se pudo interpretar stockPorDeposito como lista de items: " + ex.getMessage());
			}
		}
		String s = asString(value);
		s = trimToNull(s);
		if (s == null) {
			return null;
		}
		try {
			JavaType type = objectMapper.getTypeFactory().constructCollectionType(List.class, StockPorDepositoItem.class);
			return objectMapper.readValue(s, type);
		} catch (Exception ex) {
			throw new IllegalArgumentException("No se pudo parsear stockPorDeposito (esperado JSON array): " + ex.getMessage());
		}
	}

	private List<ProductoMigrationDto.StockPorDepositoDto> parseStockPorDepositoNested(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof List<?> list) {
			try {
				JavaType type = objectMapper.getTypeFactory().constructCollectionType(List.class, ProductoMigrationDto.StockPorDepositoDto.class);
				return objectMapper.convertValue(list, type);
			} catch (IllegalArgumentException ex) {
				throw new IllegalArgumentException("No se pudo interpretar stockPorDeposito como lista de items: " + ex.getMessage());
			}
		}
		String s = asString(value);
		s = trimToNull(s);
		if (s == null) {
			return null;
		}
		try {
			JavaType type = objectMapper.getTypeFactory().constructCollectionType(List.class, ProductoMigrationDto.StockPorDepositoDto.class);
			return objectMapper.readValue(s, type);
		} catch (Exception ex) {
			throw new IllegalArgumentException("No se pudo parsear stockPorDeposito (esperado JSON array): " + ex.getMessage());
		}
	}

	private static boolean containsKeyIgnoreCase(Map<String, Object> row, String key) {
		if (row.containsKey(key)) {
			return true;
		}
		for (String k : row.keySet()) {
			if (k != null && k.equalsIgnoreCase(key)) {
				return true;
			}
		}
		return false;
	}

	private static Object getIgnoreCase(Map<String, Object> row, String key) {
		if (row.containsKey(key)) {
			return row.get(key);
		}
		for (var entry : row.entrySet()) {
			if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(key)) {
				return entry.getValue();
			}
		}
		return null;
	}

	private static String asString(Object value) {
		return value == null ? null : String.valueOf(value);
	}

	private static BigDecimal asBigDecimal(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof BigDecimal bd) {
			return bd;
		}
		if (value instanceof Number n) {
			return new BigDecimal(String.valueOf(n));
		}
		String s = String.valueOf(value).trim();
		if (s.isEmpty()) {
			return null;
		}
		try {
			return new BigDecimal(s);
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException("No se pudo convertir a número: '" + s + "'");
		}
	}

	private static Long requiredDefault(Long value, String name) {
		if (value == null) {
			throw new IllegalStateException("Default requerido no configurado: " + name);
		}
		return value;
	}

	private static String normalizeNombre(String s) {
		s = trimToNull(s);
		if (s == null) {
			return null;
		}
		return s.trim().replaceAll("\\s+", " ").toUpperCase();
	}

	private static String normalizeTipo(String s) {
		s = trimToNull(s);
		return s == null ? null : s.trim().toUpperCase();
	}

	private static String normalizeCodigoBarra(String s) {
		s = trimToNull(s);
		if (s == null) {
			return null;
		}
		String compact = s.replaceAll("\\s+", "");
		return compact.isEmpty() ? null : compact;
	}

	private static String trimToNull(String s) {
		if (s == null) {
			return null;
		}
		String t = s.trim();
		return t.isEmpty() ? null : t;
	}
}
