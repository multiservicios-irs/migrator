package com.multiservicios.migrator.dto;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class ProductoMigrationDto {
	private ProductoDto producto;
	private BigDecimal stock;
	private Long depositoId;
	private List<StockPorDepositoDto> stockPorDeposito = new ArrayList<>();
	private List<Long> componentes = new ArrayList<>();

	public ProductoDto getProducto() {
		return producto;
	}

	public void setProducto(ProductoDto producto) {
		this.producto = producto;
	}

	public BigDecimal getStock() {
		return stock;
	}

	public void setStock(BigDecimal stock) {
		this.stock = stock;
	}

	public Long getDepositoId() {
		return depositoId;
	}

	public void setDepositoId(Long depositoId) {
		this.depositoId = depositoId;
	}

	public List<StockPorDepositoDto> getStockPorDeposito() {
		return stockPorDeposito;
	}

	public void setStockPorDeposito(List<StockPorDepositoDto> stockPorDeposito) {
		this.stockPorDeposito = stockPorDeposito;
	}

	public List<Long> getComponentes() {
		return componentes;
	}

	public void setComponentes(List<Long> componentes) {
		this.componentes = componentes;
	}

	public static class ProductoDto {
		private Long id;
		private TipoItem tipo = TipoItem.PRODUCTO;
		private String codigoBarra;
		private String nombre;
		private String descripcion;
		private Long marcaId;
		private Long categoriaId;
		private Long unidadMedidaId;
		private BigDecimal precioDeCompra;
		private BigDecimal precioMinorista;
		private BigDecimal precioMayorista;
		private BigDecimal precioCredito;
		private BigDecimal ivaPercent;
		private BigDecimal stockMin;
		private BigDecimal stock;
		private Boolean serializable;
		private String imagenUrl;
		private Boolean activo;
		private String createdAt;
		private String updatedAt;
		private String createdBy;
		private String updatedBy;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public TipoItem getTipo() {
			return tipo;
		}

		public void setTipo(TipoItem tipo) {
			this.tipo = tipo;
		}

		public String getCodigoBarra() {
			return codigoBarra;
		}

		public void setCodigoBarra(String codigoBarra) {
			this.codigoBarra = codigoBarra;
		}

		public String getNombre() {
			return nombre;
		}

		public void setNombre(String nombre) {
			this.nombre = nombre;
		}

		public String getDescripcion() {
			return descripcion;
		}

		public void setDescripcion(String descripcion) {
			this.descripcion = descripcion;
		}

		public Long getMarcaId() {
			return marcaId;
		}

		public void setMarcaId(Long marcaId) {
			this.marcaId = marcaId;
		}

		public Long getCategoriaId() {
			return categoriaId;
		}

		public void setCategoriaId(Long categoriaId) {
			this.categoriaId = categoriaId;
		}

		public Long getUnidadMedidaId() {
			return unidadMedidaId;
		}

		public void setUnidadMedidaId(Long unidadMedidaId) {
			this.unidadMedidaId = unidadMedidaId;
		}

		public BigDecimal getPrecioDeCompra() {
			return precioDeCompra;
		}

		public void setPrecioDeCompra(BigDecimal precioDeCompra) {
			this.precioDeCompra = precioDeCompra;
		}

		public BigDecimal getPrecioMinorista() {
			return precioMinorista;
		}

		public void setPrecioMinorista(BigDecimal precioMinorista) {
			this.precioMinorista = precioMinorista;
		}

		public BigDecimal getPrecioMayorista() {
			return precioMayorista;
		}

		public void setPrecioMayorista(BigDecimal precioMayorista) {
			this.precioMayorista = precioMayorista;
		}

		public BigDecimal getPrecioCredito() {
			return precioCredito;
		}

		public void setPrecioCredito(BigDecimal precioCredito) {
			this.precioCredito = precioCredito;
		}

		public BigDecimal getIvaPercent() {
			return ivaPercent;
		}

		public void setIvaPercent(BigDecimal ivaPercent) {
			this.ivaPercent = ivaPercent;
		}

		public BigDecimal getStockMin() {
			return stockMin;
		}

		public void setStockMin(BigDecimal stockMin) {
			this.stockMin = stockMin;
		}

		public BigDecimal getStock() {
			return stock;
		}

		public void setStock(BigDecimal stock) {
			this.stock = stock;
		}

		public Boolean getSerializable() {
			return serializable;
		}

		public void setSerializable(Boolean serializable) {
			this.serializable = serializable;
		}

		public String getImagenUrl() {
			return imagenUrl;
		}

		public void setImagenUrl(String imagenUrl) {
			this.imagenUrl = imagenUrl;
		}

		public Boolean getActivo() {
			return activo;
		}

		public void setActivo(Boolean activo) {
			this.activo = activo;
		}

		public String getCreatedAt() {
			return createdAt;
		}

		public void setCreatedAt(String createdAt) {
			this.createdAt = createdAt;
		}

		public String getUpdatedAt() {
			return updatedAt;
		}

		public void setUpdatedAt(String updatedAt) {
			this.updatedAt = updatedAt;
		}

		public String getCreatedBy() {
			return createdBy;
		}

		public void setCreatedBy(String createdBy) {
			this.createdBy = createdBy;
		}

		public String getUpdatedBy() {
			return updatedBy;
		}

		public void setUpdatedBy(String updatedBy) {
			this.updatedBy = updatedBy;
		}
	}

	public static class StockPorDepositoDto {
		private Long depositoId;
		private BigDecimal cantidad;
		private BigDecimal reservado;

		public Long getDepositoId() {
			return depositoId;
		}

		public void setDepositoId(Long depositoId) {
			this.depositoId = depositoId;
		}

		public BigDecimal getCantidad() {
			return cantidad;
		}

		public void setCantidad(BigDecimal cantidad) {
			this.cantidad = cantidad;
		}

		public BigDecimal getReservado() {
			return reservado;
		}

		public void setReservado(BigDecimal reservado) {
			this.reservado = reservado;
		}
	}
}
