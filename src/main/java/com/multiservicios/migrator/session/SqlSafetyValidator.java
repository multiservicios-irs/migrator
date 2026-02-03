package com.multiservicios.migrator.session;

import java.util.Locale;

public final class SqlSafetyValidator {
	private SqlSafetyValidator() {
	}

	public static void validateSelectOnly(String sql) {
		String s = sql == null ? "" : sql.trim();
		if (s.isEmpty()) {
			throw new IllegalArgumentException("SQL vacío");
		}

		String lower = s.toLowerCase(Locale.ROOT);
		if (!lower.startsWith("select")) {
			throw new IllegalArgumentException("Solo se permite SQL SELECT");
		}

		// Bloqueos básicos (no pretende ser un parser completo, solo barrera mínima).
		if (lower.contains(";")) {
			throw new IllegalArgumentException("SQL inválido: no se permite ';'");
		}
		String[] forbidden = {" insert ", " update ", " delete ", " drop ", " alter ", " truncate ", " create ", " grant ", " revoke "};
		String padded = " " + lower.replaceAll("\\s+", " ") + " ";
		for (String token : forbidden) {
			if (padded.contains(token)) {
				throw new IllegalArgumentException("SQL inválido: solo SELECT permitido");
			}
		}
	}
}
