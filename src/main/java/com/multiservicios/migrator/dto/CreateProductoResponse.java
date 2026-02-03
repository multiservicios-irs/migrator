package com.multiservicios.migrator.dto;

public record CreateProductoResponse(boolean success, String message, Long id) {
	public static CreateProductoResponse ok(String message, Long id) {
		return new CreateProductoResponse(true, message, id);
	}

	public static CreateProductoResponse fail(String message) {
		return new CreateProductoResponse(false, message, null);
	}
}
