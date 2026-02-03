package com.multiservicios.migrator.dto;

import java.math.BigDecimal;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProductoCreateRequest {
	private String nombre;
	private String tipo;
	private BigDecimal precioMinorista;
	private BigDecimal ivaPercent;

	// Forma A (simple)
	private Long depositoId;
	private BigDecimal stock;

	// Forma B (pro)
	private List<StockPorDepositoItem> stockPorDeposito;

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public String getTipo() {
		return tipo;
	}

	public void setTipo(String tipo) {
		this.tipo = tipo;
	}

	public BigDecimal getPrecioMinorista() {
		return precioMinorista;
	}

	public void setPrecioMinorista(BigDecimal precioMinorista) {
		this.precioMinorista = precioMinorista;
	}

	public BigDecimal getIvaPercent() {
		return ivaPercent;
	}

	public void setIvaPercent(BigDecimal ivaPercent) {
		this.ivaPercent = ivaPercent;
	}

	public Long getDepositoId() {
		return depositoId;
	}

	public void setDepositoId(Long depositoId) {
		this.depositoId = depositoId;
	}

	public BigDecimal getStock() {
		return stock;
	}

	public void setStock(BigDecimal stock) {
		this.stock = stock;
	}

	public List<StockPorDepositoItem> getStockPorDeposito() {
		return stockPorDeposito;
	}

	public void setStockPorDeposito(List<StockPorDepositoItem> stockPorDeposito) {
		this.stockPorDeposito = stockPorDeposito;
	}

	@JsonInclude(JsonInclude.Include.NON_NULL)
	public static class StockPorDepositoItem {
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
