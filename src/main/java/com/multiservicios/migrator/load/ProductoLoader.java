package com.multiservicios.migrator.load;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.multiservicios.migrator.config.MigratorProperties;
import com.multiservicios.migrator.dto.ProductoMigrationDto;
import com.multiservicios.migrator.dto.TipoItem;

@Component
public class ProductoLoader {
	private static final Logger log = LoggerFactory.getLogger(ProductoLoader.class);
	private final MigratorProperties properties;

	public ProductoLoader(MigratorProperties properties) {
		this.properties = properties;
	}

	public LoadResult load(DataSource destino, ProductoMigrationDto dto, boolean dryRun) {
		if (dto == null || dto.getProducto() == null) {
			return LoadResult.fail("DTO producto vacío");
		}
		final boolean includeAuditColumns = properties.getProductos() != null && properties.getProductos().isIncludeAuditColumns();
		ProductoMigrationDto.ProductoDto producto = dto.getProducto();
		TipoItem tipo = producto.getTipo() == null ? TipoItem.PRODUCTO : producto.getTipo();
		boolean esPaquete = tipo == TipoItem.PAQUETE;
		boolean esServicio = tipo == TipoItem.SERVICIO;

		String nombre = trimToNull(producto.getNombre());
		if (nombre == null) {
			return LoadResult.fail("producto.nombre es requerido");
		}

		BigDecimal stockMin = (esPaquete || esServicio) ? BigDecimal.ZERO : defaultZero(producto.getStockMin());
		BigDecimal stockProducto = BigDecimal.ZERO; // regla: el inventario inicial va en tabla stocks
		Boolean activo = includeAuditColumns ? (producto.getActivo() == null ? Boolean.TRUE : producto.getActivo()) : null;
		Boolean serializable = producto.getSerializable() == null ? Boolean.FALSE : producto.getSerializable();
		String codigoBarra = normalizeCodigoBarra(producto.getCodigoBarra());

		Long marcaId = defaultFk(producto.getMarcaId(), properties.getDefaults().getMarcaGenericaId());
		Long unidadMedidaId = defaultFk(producto.getUnidadMedidaId(), properties.getDefaults().getUnidadMedidaGenericaId());
		Long categoriaId = defaultFk(producto.getCategoriaId(), properties.getDefaults().getCategoriaGenericaId());
		BigDecimal ivaPercent = defaultZero(producto.getIvaPercent());
		String createdBy = includeAuditColumns ? defaultUser(producto.getCreatedBy()) : null;
		String updatedBy = includeAuditColumns ? defaultUser(producto.getUpdatedBy()) : null;

		if (dryRun) {
			String msg = "DRY-RUN insert producto: tipo=" + tipo.name() + " nombre=" + nombre
					+ (esPaquete || esServicio ? " (stock/stock_min=0; sin inventario)" : " (stock en tabla stocks)");
			return LoadResult.ok(msg);
		}

		try (Connection connection = destino.getConnection()) {
			boolean oldAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);
			try {
				ensureCatalogAndDepositosExist(connection, marcaId, unidadMedidaId, categoriaId, dto);

				ProductoUpsertResult productoResult = insertProducto(connection,
						codigoBarra,
						nombre,
						producto.getDescripcion(),
						marcaId,
						unidadMedidaId,
						categoriaId,
						producto.getPrecioDeCompra(),
						producto.getPrecioMinorista(),
						producto.getPrecioMayorista(),
						producto.getPrecioCredito(),
						ivaPercent,
						stockMin,
						stockProducto,
						serializable,
						tipo.name(),
						producto.getImagenUrl(),
						activo,
						createdBy,
						updatedBy,
						includeAuditColumns);
				long productoId = productoResult.id();

				BigDecimal totalStock = null;
				if (!esPaquete && !esServicio) {
					var stockOps = upsertStocksAndMovimientos(connection, productoId, dto, productoResult.created());
					totalStock = stockOps.totalStock();
					updateProductoStock(connection, productoId, totalStock, includeAuditColumns);
				} else {
					// seguridad extra: si por algún motivo existieran stocks, los limpiamos para SERVICIO
					if (esServicio) {
						deleteStocks(connection, productoId);
					}
				}

				if (dto.getComponentes() != null && !dto.getComponentes().isEmpty()) {
					// falta definición de tabla/relación real; se implementa cuando esté el esquema
					log.warn("Componentes recibidos ({}). Reemplazo de componentes no implementado (falta esquema destino).", dto.getComponentes().size());
				}

				connection.commit();
				String stockInfo;
				if (esPaquete || esServicio) {
					stockInfo = "sin stock (tipo=" + tipo.name() + ")";
				} else if (dto.getStockPorDeposito() != null && !dto.getStockPorDeposito().isEmpty()) {
					stockInfo = "stockPorDeposito items=" + dto.getStockPorDeposito().size() + " totalStock=" + totalStock;
				} else {
					stockInfo = "depositoId=" + dto.getDepositoId() + " stock=" + dto.getStock() + " totalStock=" + totalStock;
				}
				String msg = (productoResult.created() ? "Producto creado en destino" : "Producto ya existía en destino") + ": id=" + productoId
						+ " nombre=" + nombre
						+ " tipo=" + tipo.name()
						+ " precioMinorista=" + producto.getPrecioMinorista()
						+ " precioMayorista=" + producto.getPrecioMayorista()
						+ " precioCredito=" + producto.getPrecioCredito()
						+ " ivaPercent=" + ivaPercent
						+ " " + stockInfo;
				return LoadResult.ok(msg, productoId);
			} catch (Exception ex) {
				try {
					connection.rollback();
				} catch (Exception ignored) {
				}
				throw ex;
			} finally {
				try {
					connection.setAutoCommit(oldAutoCommit);
				} catch (Exception ignored) {
				}
			}
		} catch (Exception ex) {
			return LoadResult.fail("Error guardando producto en destino: " + ex.getMessage());
		}
	}

	/**
	 * Modo "POST" (best-effort): crea el producto y luego intenta cargar el stock inicial.
	 * Si la carga de stock falla, deja el producto creado y devuelve OK con warning en el mensaje.
	 */
	public LoadResult createWithInitialStockBestEffort(DataSource destino, ProductoMigrationDto dto, boolean dryRun) {
		if (dto == null || dto.getProducto() == null) {
			return LoadResult.fail("DTO producto vacío");
		}
		ProductoMigrationDto.ProductoDto producto = dto.getProducto();
		TipoItem tipo = producto.getTipo() == null ? TipoItem.PRODUCTO : producto.getTipo();
		boolean esPaquete = tipo == TipoItem.PAQUETE;
		boolean esServicio = tipo == TipoItem.SERVICIO;

		String nombre = trimToNull(producto.getNombre());
		if (nombre == null) {
			return LoadResult.fail("producto.nombre es requerido");
		}

		BigDecimal stockMin = (esPaquete || esServicio) ? BigDecimal.ZERO : defaultZero(producto.getStockMin());
		BigDecimal stockProducto = BigDecimal.ZERO;
		Boolean activo = producto.getActivo() == null ? Boolean.TRUE : producto.getActivo();
		Boolean serializable = producto.getSerializable() == null ? Boolean.FALSE : producto.getSerializable();
		String codigoBarra = normalizeCodigoBarra(producto.getCodigoBarra());

		Long marcaId = defaultFk(producto.getMarcaId(), properties.getDefaults().getMarcaGenericaId());
		Long unidadMedidaId = defaultFk(producto.getUnidadMedidaId(), properties.getDefaults().getUnidadMedidaGenericaId());
		Long categoriaId = defaultFk(producto.getCategoriaId(), properties.getDefaults().getCategoriaGenericaId());
		BigDecimal ivaPercent = defaultZero(producto.getIvaPercent());
		String createdBy = defaultUser(producto.getCreatedBy());
		String updatedBy = defaultUser(producto.getUpdatedBy());
		boolean includeAuditColumns = properties.getProductos().isIncludeAuditColumns();

		if (dryRun) {
			String msg = "DRY-RUN insert producto+stock: tipo=" + tipo.name() + " nombre=" + nombre;
			return LoadResult.ok(msg);
		}

		Long productoId;
		boolean created;
		try (Connection connection = destino.getConnection()) {
			boolean oldAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);
			try {
				ensureCatalogAndDepositosExist(connection, marcaId, unidadMedidaId, categoriaId, dto);

				ProductoUpsertResult productoResult = insertProducto(connection,
						codigoBarra,
						nombre,
						producto.getDescripcion(),
						marcaId,
						unidadMedidaId,
						categoriaId,
						producto.getPrecioDeCompra(),
						producto.getPrecioMinorista(),
						producto.getPrecioMayorista(),
						producto.getPrecioCredito(),
						ivaPercent,
						stockMin,
						stockProducto,
						serializable,
						tipo.name(),
						producto.getImagenUrl(),
						activo,
						createdBy,
						updatedBy,
						includeAuditColumns);
				productoId = productoResult.id();
				created = productoResult.created();
				connection.commit();
			} catch (Exception ex) {
				try {
					connection.rollback();
				} catch (Exception ignored) {
				}
				throw ex;
			} finally {
				try {
					connection.setAutoCommit(oldAutoCommit);
				} catch (Exception ignored) {
				}
			}
		} catch (Exception ex) {
			return LoadResult.fail("Error guardando producto en destino: " + ex.getMessage());
		}

		// 2da fase: stock best-effort (si corresponde)
		if (!esPaquete && !esServicio) {
			try (Connection connection = destino.getConnection()) {
				boolean oldAutoCommit = connection.getAutoCommit();
				connection.setAutoCommit(false);
				try {
					var stockOps = upsertStocksAndMovimientos(connection, productoId, dto, created);
					updateProductoStock(connection, productoId, stockOps.totalStock(), includeAuditColumns);
					connection.commit();
				} catch (Exception ex) {
					try {
						connection.rollback();
					} catch (Exception ignored) {
					}
					log.warn("Producto creado (id={}) pero falló carga de stock: {}", productoId, ex.getMessage());
					return LoadResult.ok("Producto creado en destino: id=" + productoId + " nombre=" + nombre + " (WARN: stock no se pudo cargar)", productoId);
				} finally {
					try {
						connection.setAutoCommit(oldAutoCommit);
					} catch (Exception ignored) {
					}
				}
			} catch (Exception ex) {
				log.warn("Producto creado (id={}) pero falló carga de stock (2da fase): {}", productoId, ex.getMessage());
				return LoadResult.ok("Producto creado en destino: id=" + productoId + " nombre=" + nombre + " (WARN: stock no se pudo cargar)", productoId);
			}
		} else if (esServicio) {
			try (Connection connection = destino.getConnection()) {
				deleteStocks(connection, productoId);
			} catch (Exception ex) {
				// best-effort
				log.warn("Producto SERVICIO creado (id={}) pero falló limpieza de stocks: {}", productoId, ex.getMessage());
			}
		}

		return LoadResult.ok((created ? "Producto creado en destino" : "Producto ya existía en destino") + ": id=" + productoId + " nombre=" + nombre, productoId);
	}

	private record StockOps(BigDecimal totalStock) {
	}

	private record ProductoUpsertResult(long id, boolean created) {
	}

	private ProductoUpsertResult insertProducto(
			Connection connection,
			String codigoBarra,
			String nombre,
			String descripcion,
			Long marcaId,
			Long unidadMedidaId,
			Long categoriaId,
			BigDecimal precioDeCompra,
			BigDecimal precioMinorista,
			BigDecimal precioMayorista,
			BigDecimal precioCredito,
			BigDecimal ivaPercent,
			BigDecimal stockMin,
			BigDecimal stock,
			Boolean serializable,
			String tipo,
			String imagenUrl,
			Boolean activo,
			String createdBy,
			String updatedBy,
			boolean includeAuditColumns) throws SQLException {
		final String sql;
		if (includeAuditColumns) {
			sql = "insert into productos (" +
					"codigo_barra, nombre, descripcion, marca_id, unidad_medida_id, categoria_id, " +
					"precio_de_compra, precio_minorista, precio_mayorista, precio_credito, " +
					"iva_percent, stock_min, stock, serializable, tipo, imagen_url, activo, " +
					"created_at, updated_at, created_by, updated_by" +
					") values (" +
					"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp, ?, ?" +
					") returning id";
		} else {
			sql = "insert into productos (" +
					"codigo_barra, nombre, descripcion, marca_id, unidad_medida_id, categoria_id, " +
					"precio_de_compra, precio_minorista, precio_mayorista, precio_credito, " +
					"iva_percent, stock_min, stock, serializable, tipo, imagen_url" +
					") values (" +
					"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" +
					") returning id";
		}

		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			int i = 1;
			setStringOrNull(ps, i++, codigoBarra);
			setStringOrNull(ps, i++, nombre);
			setStringOrNull(ps, i++, descripcion);
			setLongOrNull(ps, i++, marcaId);
			setLongOrNull(ps, i++, unidadMedidaId);
			setLongOrNull(ps, i++, categoriaId);
			setBigDecimalOrNull(ps, i++, precioDeCompra);
			setBigDecimalOrNull(ps, i++, precioMinorista);
			setBigDecimalOrNull(ps, i++, precioMayorista);
			setBigDecimalOrNull(ps, i++, precioCredito);
			setBigDecimalOrNull(ps, i++, ivaPercent);
			setBigDecimalOrNull(ps, i++, stockMin);
			setBigDecimalOrNull(ps, i++, stock);
			ps.setBoolean(i++, serializable != null && serializable);
			setStringOrNull(ps, i++, tipo);
			setStringOrNull(ps, i++, imagenUrl);
			if (includeAuditColumns) {
				ps.setBoolean(i++, activo == null || activo);
				setStringOrNull(ps, i++, createdBy);
				setStringOrNull(ps, i++, updatedBy);
			}

			// En PostgreSQL, si un statement falla dentro de una transacción (autoCommit=false),
			// la transacción queda "abortada" hasta ejecutar rollback. Usamos SAVEPOINT para
			// poder recuperarnos de errores esperados (ej. unique violation) sin tumbar el resto.
			Savepoint insertSp = null;
			try {
				if (connection != null && !connection.getAutoCommit()) {
					insertSp = connection.setSavepoint();
				}
			} catch (SQLException ignored) {
				insertSp = null;
			}

			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return new ProductoUpsertResult(rs.getLong(1), true);
				}
				throw new IllegalStateException("No se pudo obtener id del producto insertado");
			} catch (SQLException ex) {
				// PostgreSQL: 23505 = unique_violation
				if (isUniqueViolation(ex)) {
					if (insertSp != null) {
						try {
							connection.rollback(insertSp);
						} catch (SQLException ignored) {
							// Si falla, el rollback de la transacción completa lo hará el caller.
						}
					}
					Long existingId = findExistingProductoId(connection, codigoBarra, nombre);
					if (existingId != null) {
						log.warn("Producto duplicado (nombre/codigo_barra). Se reutiliza id={} nombre={}", existingId, nombre);
						// Best-effort: completar campos faltantes (NULL) en destino.
						try {
							patchProductoIfMissing(connection,
								existingId,
								codigoBarra,
								nombre,
								descripcion,
								marcaId,
								unidadMedidaId,
								categoriaId,
								precioDeCompra,
								precioMinorista,
								precioMayorista,
								precioCredito,
								ivaPercent,
								stockMin,
								serializable,
								tipo,
								imagenUrl,
								includeAuditColumns ? activo : null,
								includeAuditColumns ? createdBy : null,
								includeAuditColumns ? updatedBy : null,
								includeAuditColumns);
						} catch (SQLException patchEx) {
							log.warn("No se pudo completar campos faltantes del producto id={}: {}", existingId, patchEx.getMessage());
						}
						return new ProductoUpsertResult(existingId, false);
					}
				}
				throw ex;
			}
		}
	}

	private void patchProductoIfMissing(
			Connection connection,
			long id,
			String codigoBarra,
			String nombre,
			String descripcion,
			Long marcaId,
			Long unidadMedidaId,
			Long categoriaId,
			BigDecimal precioDeCompra,
			BigDecimal precioMinorista,
			BigDecimal precioMayorista,
			BigDecimal precioCredito,
			BigDecimal ivaPercent,
			BigDecimal stockMin,
			Boolean serializable,
			String tipo,
			String imagenUrl,
			Boolean activo,
			String createdBy,
			String updatedBy,
			boolean includeAuditColumns) throws SQLException {
		final String sql;
		if (includeAuditColumns) {
			sql = "update productos set " +
					"codigo_barra = coalesce(codigo_barra, ?), " +
					"nombre = coalesce(nombre, ?), " +
					"descripcion = coalesce(descripcion, ?), " +
					"marca_id = coalesce(marca_id, ?), " +
					"unidad_medida_id = coalesce(unidad_medida_id, ?), " +
					"categoria_id = coalesce(categoria_id, ?), " +
					"precio_de_compra = coalesce(precio_de_compra, ?), " +
					"precio_minorista = coalesce(precio_minorista, ?), " +
					"precio_mayorista = coalesce(precio_mayorista, ?), " +
					"precio_credito = coalesce(precio_credito, ?), " +
					"iva_percent = coalesce(iva_percent, ?), " +
					"stock_min = coalesce(stock_min, ?), " +
					"serializable = coalesce(serializable, ?), " +
					"tipo = coalesce(tipo, ?), " +
					"imagen_url = coalesce(imagen_url, ?), " +
					"activo = coalesce(activo, ?), " +
					"created_by = coalesce(created_by, ?), " +
					"updated_by = coalesce(updated_by, ?), " +
					"updated_at = current_timestamp " +
					"where id = ?";
		} else {
			sql = "update productos set " +
					"codigo_barra = coalesce(codigo_barra, ?), " +
					"nombre = coalesce(nombre, ?), " +
					"descripcion = coalesce(descripcion, ?), " +
					"marca_id = coalesce(marca_id, ?), " +
					"unidad_medida_id = coalesce(unidad_medida_id, ?), " +
					"categoria_id = coalesce(categoria_id, ?), " +
					"precio_de_compra = coalesce(precio_de_compra, ?), " +
					"precio_minorista = coalesce(precio_minorista, ?), " +
					"precio_mayorista = coalesce(precio_mayorista, ?), " +
					"precio_credito = coalesce(precio_credito, ?), " +
					"iva_percent = coalesce(iva_percent, ?), " +
					"stock_min = coalesce(stock_min, ?), " +
					"serializable = coalesce(serializable, ?), " +
					"tipo = coalesce(tipo, ?), " +
					"imagen_url = coalesce(imagen_url, ?), " +
					"updated_at = current_timestamp " +
					"where id = ?";
		}

		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			int i = 1;
			setStringOrNull(ps, i++, codigoBarra);
			setStringOrNull(ps, i++, nombre);
			setStringOrNull(ps, i++, descripcion);
			setLongOrNull(ps, i++, marcaId);
			setLongOrNull(ps, i++, unidadMedidaId);
			setLongOrNull(ps, i++, categoriaId);
			setBigDecimalOrNull(ps, i++, precioDeCompra);
			setBigDecimalOrNull(ps, i++, precioMinorista);
			setBigDecimalOrNull(ps, i++, precioMayorista);
			setBigDecimalOrNull(ps, i++, precioCredito);
			setBigDecimalOrNull(ps, i++, ivaPercent);
			setBigDecimalOrNull(ps, i++, stockMin);
			setBooleanOrNull(ps, i++, serializable);
			setStringOrNull(ps, i++, tipo);
			setStringOrNull(ps, i++, imagenUrl);
			if (includeAuditColumns) {
				setBooleanOrNull(ps, i++, activo);
				setStringOrNull(ps, i++, createdBy);
				setStringOrNull(ps, i++, updatedBy);
			}
			ps.setLong(i++, id);
			ps.executeUpdate();
		}
	}

	private static void setBooleanOrNull(PreparedStatement ps, int idx, Boolean value) throws SQLException {
		if (value == null) {
			ps.setNull(idx, java.sql.Types.BOOLEAN);
		} else {
			ps.setBoolean(idx, value);
		}
	}

	private StockOps upsertStocksAndMovimientos(Connection connection, long productoId, ProductoMigrationDto dto, boolean insertMovimientos) throws SQLException {
		BigDecimal total = BigDecimal.ZERO;
		if (dto.getStockPorDeposito() != null && !dto.getStockPorDeposito().isEmpty()) {
			for (ProductoMigrationDto.StockPorDepositoDto s : dto.getStockPorDeposito()) {
				if (s == null) {
					continue;
				}
				Long depositoId = s.getDepositoId() != null ? s.getDepositoId() : properties.getDefaults().getDepositoCentralId();
				if (depositoId == null) {
					continue;
				}
				BigDecimal cantidad = defaultZero(s.getCantidad());
				BigDecimal reservado = defaultZero(s.getReservado());
				upsertStockRow(connection, productoId, depositoId, cantidad, reservado);
				if (insertMovimientos) {
					insertStockMovimiento(connection, productoId, depositoId, cantidad);
				}
				total = total.add(cantidad);
			}
			return new StockOps(total);
		}
		if (dto.getStock() != null) {
			Long depositoId = dto.getDepositoId() != null ? dto.getDepositoId() : properties.getDefaults().getDepositoCentralId();
			if (depositoId != null) {
				BigDecimal cantidad = defaultZero(dto.getStock());
				upsertStockRow(connection, productoId, depositoId, cantidad, BigDecimal.ZERO);
				if (insertMovimientos) {
					insertStockMovimiento(connection, productoId, depositoId, cantidad);
				}
				total = total.add(cantidad);
			}
		}
		return new StockOps(total);
	}

	private static boolean isUniqueViolation(SQLException ex) {
		return ex != null && "23505".equals(ex.getSQLState());
	}

	private static Long findExistingProductoId(Connection connection, String codigoBarra, String nombre) throws SQLException {
		if (connection == null) {
			return null;
		}

		String cb = codigoBarra != null ? codigoBarra.trim() : null;
		if (cb != null && !cb.isBlank()) {
			try (PreparedStatement ps = connection.prepareStatement("select id from productos where codigo_barra = ? limit 1")) {
				ps.setString(1, cb);
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						return rs.getLong(1);
					}
				}
			}
		}

		String n = nombre != null ? nombre.trim() : null;
		if (n != null && !n.isBlank()) {
			try (PreparedStatement ps = connection.prepareStatement("select id from productos where nombre = ? limit 1")) {
				ps.setString(1, n);
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						return rs.getLong(1);
					}
				}
			}
		}
		return null;
	}

	private void updateProductoStock(Connection connection, long productoId, BigDecimal totalStock, boolean includeAuditColumns) throws SQLException {
		final String sql = includeAuditColumns
				? "update productos set stock = ?, updated_at = current_timestamp where id = ?"
				: "update productos set stock = ? where id = ?";
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			ps.setBigDecimal(1, defaultZero(totalStock));
			ps.setLong(2, productoId);
			ps.executeUpdate();
		}
	}

	private void insertStockMovimiento(Connection connection, long productoId, long depositoId, BigDecimal cantidad) throws SQLException {
		String sql = "insert into stock_movimientos (" +
				"created_at, activo, producto_id, deposito_id, cantidad, tipo, referencia, usuario, fecha, observaciones, venta_id, venta_detalle_id" +
				") values (" +
				"current_timestamp, true, ?, ?, ?, 'ENTRADA', 'ALTA_PRODUCTO', 'migrator', current_timestamp, 'Stock inicial', null, null" +
				")";
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			ps.setLong(1, productoId);
			ps.setLong(2, depositoId);
			ps.setBigDecimal(3, defaultZero(cantidad));
			ps.executeUpdate();
		}
	}

	private void upsertStockRow(Connection connection, long productoId, long depositoId, BigDecimal cantidad, BigDecimal reservado) throws SQLException {
		String sql = "insert into stocks (created_at, activo, producto_id, deposito_id, cantidad, reservado) values (current_timestamp, true, ?, ?, ?, ?) " +
				"on conflict (producto_id, deposito_id) do update set cantidad = excluded.cantidad, reservado = excluded.reservado";
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			ps.setLong(1, productoId);
			ps.setLong(2, depositoId);
			ps.setBigDecimal(3, defaultZero(cantidad));
			ps.setBigDecimal(4, defaultZero(reservado));
			ps.executeUpdate();
		}
	}

	private void deleteStocks(Connection connection, long productoId) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement("delete from stocks where producto_id = ?")) {
			ps.setLong(1, productoId);
			ps.executeUpdate();
		}
	}

	private void ensureCatalogAndDepositosExist(
			Connection connection,
			Long marcaId,
			Long unidadMedidaId,
			Long categoriaId,
			ProductoMigrationDto dto) throws SQLException {
		if (!properties.getDefaults().isAutoCreateCatalog()) {
			return;
		}

		// Catálogos: marcas/categorias/unidades_medida
		ensureCatalogRowExists(connection, "marcas", marcaId,
				marcaId != null && marcaId.equals(properties.getDefaults().getMarcaGenericaId())
						? properties.getDefaults().getMarcaGenericaNombre()
						: "AUTO_" + marcaId);
		ensureCatalogRowExists(connection, "categorias", categoriaId,
				categoriaId != null && categoriaId.equals(properties.getDefaults().getCategoriaGenericaId())
						? properties.getDefaults().getCategoriaGenericaNombre()
						: "AUTO_" + categoriaId);
		ensureCatalogRowExists(connection, "unidades_medida", unidadMedidaId,
				unidadMedidaId != null && unidadMedidaId.equals(properties.getDefaults().getUnidadMedidaGenericaId())
						? properties.getDefaults().getUnidadMedidaGenericaNombre()
						: "AUTO_" + unidadMedidaId);

		// Depósitos: para stock inicial
		if (dto != null) {
			if (dto.getStockPorDeposito() != null && !dto.getStockPorDeposito().isEmpty()) {
				for (ProductoMigrationDto.StockPorDepositoDto s : dto.getStockPorDeposito()) {
					if (s == null) {
						continue;
					}
					Long depositoId = s.getDepositoId() != null ? s.getDepositoId() : properties.getDefaults().getDepositoCentralId();
					ensureCatalogRowExists(connection, "depositos", depositoId,
							depositoId != null && depositoId.equals(properties.getDefaults().getDepositoCentralId())
									? properties.getDefaults().getDepositoCentralNombre()
									: "AUTO_" + depositoId);
				}
			} else {
				Long depositoId = dto.getDepositoId() != null ? dto.getDepositoId() : properties.getDefaults().getDepositoCentralId();
				ensureCatalogRowExists(connection, "depositos", depositoId,
						depositoId != null && depositoId.equals(properties.getDefaults().getDepositoCentralId())
								? properties.getDefaults().getDepositoCentralNombre()
								: "AUTO_" + depositoId);
			}
		}
	}

	private void ensureCatalogRowExists(Connection connection, String table, Long id, String nombre) throws SQLException {
		if (id == null) {
			return;
		}
		if (existsById(connection, table, id)) {
			return;
		}

		String resolvedNombre = trimToNull(nombre);
		if (resolvedNombre == null) {
			resolvedNombre = "AUTO_" + id;
		}

		// Intentos best-effort (esquemas pueden variar entre instalaciones)
		String[] candidates = new String[] {
				"insert into " + table + " (id, nombre, activo, created_at, updated_at) values (?, ?, true, current_timestamp, current_timestamp)",
				"insert into " + table + " (id, nombre, activo, created_at) values (?, ?, true, current_timestamp)",
				"insert into " + table + " (id, nombre, activo) values (?, ?, true)",
				"insert into " + table + " (id, nombre) values (?, ?)"
		};

		SQLException last = null;
		for (String sql : candidates) {
			try (PreparedStatement ps = connection.prepareStatement(sql)) {
				ps.setLong(1, id);
				ps.setString(2, resolvedNombre);
				ps.executeUpdate();
				return;
			} catch (SQLException ex) {
				last = ex;
				// Puede ser que otro proceso lo haya creado entre el exists y el insert
				if (existsById(connection, table, id)) {
					return;
				}
			}
		}
		throw new SQLException("No se pudo auto-crear registro en tabla '" + table + "' para id=" + id + ": " + (last == null ? "error desconocido" : last.getMessage()), last);
	}

	private boolean existsById(Connection connection, String table, long id) throws SQLException {
		String sql = "select 1 from " + table + " where id = ?";
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			ps.setLong(1, id);
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next();
			}
		}
	}

	private BigDecimal defaultZero(BigDecimal v) {
		return v == null ? BigDecimal.ZERO : v;
	}

	private String normalizeCodigoBarra(String s) {
		String trimmed = trimToNull(s);
		return trimmed == null ? null : trimmed;
	}

	private String trimToNull(String s) {
		if (s == null) {
			return null;
		}
		String t = s.trim();
		return t.isEmpty() ? null : t;
	}

	private Long defaultFk(Long provided, Long fallback) {
		return provided != null ? provided : fallback;
	}

	private String defaultUser(String provided) {
		String t = trimToNull(provided);
		return t != null ? t : "migrator";
	}

	private void setStringOrNull(PreparedStatement ps, int index, String value) throws SQLException {
		if (value == null) {
			ps.setNull(index, java.sql.Types.VARCHAR);
		} else {
			ps.setString(index, value);
		}
	}

	private void setLongOrNull(PreparedStatement ps, int index, Long value) throws SQLException {
		if (value == null) {
			ps.setNull(index, java.sql.Types.BIGINT);
		} else {
			ps.setLong(index, value);
		}
	}

	private void setBigDecimalOrNull(PreparedStatement ps, int index, BigDecimal value) throws SQLException {
		if (value == null) {
			ps.setNull(index, java.sql.Types.NUMERIC);
		} else {
			ps.setBigDecimal(index, value);
		}
	}
}
