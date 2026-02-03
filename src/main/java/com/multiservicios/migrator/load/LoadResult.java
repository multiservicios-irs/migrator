package com.multiservicios.migrator.load;

public class LoadResult {
	private final boolean success;
	private final String message;
	private final Long createdId;

	public LoadResult(boolean success, String message, Long createdId) {
		this.success = success;
		this.message = message;
		this.createdId = createdId;
	}

	public static LoadResult ok(String message) {
		return new LoadResult(true, message, null);
	}

	public static LoadResult ok(String message, Long createdId) {
		return new LoadResult(true, message, createdId);
	}

	public static LoadResult fail(String message) {
		return new LoadResult(false, message, null);
	}

	public boolean isSuccess() {
		return success;
	}

	public String getMessage() {
		return message;
	}

	public Long getCreatedId() {
		return createdId;
	}
}
