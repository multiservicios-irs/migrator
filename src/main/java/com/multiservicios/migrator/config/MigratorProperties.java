package com.multiservicios.migrator.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

@Validated
@ConfigurationProperties(prefix = "migrator")
public class MigratorProperties {
	@Valid
	@NotNull
	private List<ConnectionProfile> profiles = new ArrayList<>();

	@Valid
	@NotNull
	private Defaults defaults = new Defaults();

	@NotNull
	private Policies policies = new Policies();

	@NotNull
	private Destino destino = new Destino();

	@NotNull
	private Productos productos = new Productos();

	@NotNull
	private Session session = new Session();

	public List<ConnectionProfile> getProfiles() {
		return profiles;
	}

	public void setProfiles(List<ConnectionProfile> profiles) {
		this.profiles = profiles;
	}

	public Defaults getDefaults() {
		return defaults;
	}

	public void setDefaults(Defaults defaults) {
		this.defaults = defaults;
	}

	public Policies getPolicies() {
		return policies;
	}

	public void setPolicies(Policies policies) {
		this.policies = policies;
	}

	public Destino getDestino() {
		return destino;
	}

	public void setDestino(Destino destino) {
		this.destino = destino;
	}

	public Productos getProductos() {
		return productos;
	}

	public void setProductos(Productos productos) {
		this.productos = productos;
	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public static class Session {
		/**
		 * Límite máximo de filas a cargar en memoria por /migration/load-sql.
		 */
		private int maxRowsDefault = 1000;
		private int maxRowsHardLimit = 20000;

		/**
		 * Máximo de intentos por fila antes de marcarla como FAILED definitivo.
		 */
		private int maxAttempts = 3;

		/**
		 * TTL de sesiones en minutos. Sesiones más viejas se eliminan automáticamente.
		 */
		private int ttlMinutes = 24 * 60;

		/**
		 * Intervalo de cleanup (segundos).
		 */
		private int cleanupIntervalSeconds = 300;

		public int getMaxRowsDefault() {
			return maxRowsDefault;
		}

		public void setMaxRowsDefault(int maxRowsDefault) {
			this.maxRowsDefault = maxRowsDefault;
		}

		public int getMaxRowsHardLimit() {
			return maxRowsHardLimit;
		}

		public void setMaxRowsHardLimit(int maxRowsHardLimit) {
			this.maxRowsHardLimit = maxRowsHardLimit;
		}

		public int getMaxAttempts() {
			return maxAttempts;
		}

		public void setMaxAttempts(int maxAttempts) {
			this.maxAttempts = maxAttempts;
		}

		public int getTtlMinutes() {
			return ttlMinutes;
		}

		public void setTtlMinutes(int ttlMinutes) {
			this.ttlMinutes = ttlMinutes;
		}

		public int getCleanupIntervalSeconds() {
			return cleanupIntervalSeconds;
		}

		public void setCleanupIntervalSeconds(int cleanupIntervalSeconds) {
			this.cleanupIntervalSeconds = cleanupIntervalSeconds;
		}
	}

	public static class Destino {
		/**
		 * Base URL del backend MultiServicios IRS (donde vive POST /api/productos).
		 * Ej: http://localhost:8080
		 */
		private String baseUrl = "http://localhost:8080";

		/**
		 * Contrato del endpoint destino /api/productos.
		 * - FLAT: JSON plano (nombre/tipo/precioMinorista/ivaPercent + stock/depositoId o stockPorDeposito)
		 * - NESTED: DTO anidado (ProductoMigrationDto con { producto: {...}, stock, depositoId, stockPorDeposito })
		 */
		private DestinoContract contract = DestinoContract.FLAT;

		public String getBaseUrl() {
			return baseUrl;
		}

		public void setBaseUrl(String baseUrl) {
			this.baseUrl = baseUrl;
		}

		public DestinoContract getContract() {
			return contract;
		}

		public void setContract(DestinoContract contract) {
			this.contract = contract;
		}
	}

	public static class Productos {
		/**
		 * Si true, el migrador incluirá columnas/campos de auditoría al insertar/actualizar
		 * (ej: activo, created_at, updated_at, created_by, updated_by).
		 *
		 * Por defecto es false para no “forzar” auditoría (se asume que el destino la gestiona).
		 */
		private boolean includeAuditColumns = false;

		public boolean isIncludeAuditColumns() {
			return includeAuditColumns;
		}

		public void setIncludeAuditColumns(boolean includeAuditColumns) {
			this.includeAuditColumns = includeAuditColumns;
		}
	}

	public enum DestinoContract {
		FLAT,
		NESTED
	}

	public static class Defaults {
		private Long depositoCentralId = 1L;
		private String depositoCentralNombre = "CENTRAL";
		private Long marcaGenericaId = 1L;
		private String marcaGenericaNombre = "GENERICA";
		private Long categoriaGenericaId = 1L;
		private String categoriaGenericaNombre = "GENERICA";
		private Long unidadMedidaGenericaId = 1L;
		private String unidadMedidaGenericaNombre = "UNIDAD";

		/**
		 * Si true y el destino es DB (ProductoLoader), intenta crear los registros
		 * faltantes (marca/categoría/unidad/deposito) para evitar errores de FK.
		 */
		private boolean autoCreateCatalog = false;

		public Long getDepositoCentralId() {
			return depositoCentralId;
		}

		public void setDepositoCentralId(Long depositoCentralId) {
			this.depositoCentralId = depositoCentralId;
		}

		public String getDepositoCentralNombre() {
			return depositoCentralNombre;
		}

		public void setDepositoCentralNombre(String depositoCentralNombre) {
			this.depositoCentralNombre = depositoCentralNombre;
		}

		public Long getMarcaGenericaId() {
			return marcaGenericaId;
		}

		public void setMarcaGenericaId(Long marcaGenericaId) {
			this.marcaGenericaId = marcaGenericaId;
		}

		public String getMarcaGenericaNombre() {
			return marcaGenericaNombre;
		}

		public void setMarcaGenericaNombre(String marcaGenericaNombre) {
			this.marcaGenericaNombre = marcaGenericaNombre;
		}

		public Long getCategoriaGenericaId() {
			return categoriaGenericaId;
		}

		public void setCategoriaGenericaId(Long categoriaGenericaId) {
			this.categoriaGenericaId = categoriaGenericaId;
		}

		public String getCategoriaGenericaNombre() {
			return categoriaGenericaNombre;
		}

		public void setCategoriaGenericaNombre(String categoriaGenericaNombre) {
			this.categoriaGenericaNombre = categoriaGenericaNombre;
		}

		public Long getUnidadMedidaGenericaId() {
			return unidadMedidaGenericaId;
		}

		public void setUnidadMedidaGenericaId(Long unidadMedidaGenericaId) {
			this.unidadMedidaGenericaId = unidadMedidaGenericaId;
		}

		public String getUnidadMedidaGenericaNombre() {
			return unidadMedidaGenericaNombre;
		}

		public void setUnidadMedidaGenericaNombre(String unidadMedidaGenericaNombre) {
			this.unidadMedidaGenericaNombre = unidadMedidaGenericaNombre;
		}

		public boolean isAutoCreateCatalog() {
			return autoCreateCatalog;
		}

		public void setAutoCreateCatalog(boolean autoCreateCatalog) {
			this.autoCreateCatalog = autoCreateCatalog;
		}
	}

	public static class Policies {
		private NullPolicy nullPolicy = NullPolicy.SKIP_ROW;
		private DuplicatePolicy duplicatePolicy = DuplicatePolicy.SKIP;

		public NullPolicy getNullPolicy() {
			return nullPolicy;
		}

		public void setNullPolicy(NullPolicy nullPolicy) {
			this.nullPolicy = nullPolicy;
		}

		public DuplicatePolicy getDuplicatePolicy() {
			return duplicatePolicy;
		}

		public void setDuplicatePolicy(DuplicatePolicy duplicatePolicy) {
			this.duplicatePolicy = duplicatePolicy;
		}
	}

	public enum NullPolicy {
		SKIP_ROW,
		SET_NULL,
		ERROR
	}

	public enum DuplicatePolicy {
		SKIP,
		UPDATE,
		ERROR
	}
}
