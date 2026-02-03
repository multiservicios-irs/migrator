package com.multiservicios.migrator.load;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.springframework.stereotype.Component;

@Component
public class UniversalTableLoader {
	public record LoadResult(boolean success, String message) {
	}

	public LoadResult insertRow(DataSource destino, String schema, String table, Map<String, Object> values,
			Set<String> allowedColumnsLower, boolean dryRun) {
		if (destino == null) {
			return new LoadResult(false, "Destino es null");
		}
		String safeSchema = (schema == null || schema.isBlank()) ? "public" : schema.trim();
		String safeTable = table == null ? "" : table.trim();
		if (safeTable.isBlank()) {
			return new LoadResult(false, "Tabla destino es requerida");
		}
		if (values == null || values.isEmpty()) {
			return new LoadResult(false, "No hay columnas mapeadas para insertar");
		}

		// Filtrar por columnas permitidas (defensa fuerte)
		List<String> columns = new ArrayList<>();
		List<Object> params = new ArrayList<>();
		for (var e : values.entrySet()) {
			String col = e.getKey() == null ? "" : e.getKey().trim();
			if (col.isEmpty()) {
				continue;
			}
			if (allowedColumnsLower != null && !allowedColumnsLower.isEmpty()) {
				if (!allowedColumnsLower.contains(col.toLowerCase())) {
					continue;
				}
			}
			columns.add(col);
			params.add(e.getValue());
		}
		if (columns.isEmpty()) {
			return new LoadResult(false,
					"No hay columnas válidas para insertar (revisá mapeos vs columnas reales del destino)");
		}

		String sql = buildInsertSql(safeSchema, safeTable, columns);
		if (dryRun) {
			return new LoadResult(true, "DRY RUN: " + sql);
		}

		try (Connection c = destino.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
			for (int i = 0; i < params.size(); i++) {
				ps.setObject(i + 1, params.get(i));
			}
			int updated = ps.executeUpdate();
			return new LoadResult(updated == 1, "Insert OK en " + safeSchema + "." + safeTable + " (" + updated + ")");
		} catch (Exception ex) {
			return new LoadResult(false, ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}

	public Set<String> loadAllowedColumnsLower(DataSource destino, String schema, String table) throws SQLException {
		String safeSchema = (schema == null || schema.isBlank()) ? "public" : schema.trim();
		String safeTable = table == null ? "" : table.trim();
		Set<String> cols = new LinkedHashSet<>();
		try (Connection c = destino.getConnection()) {
			var md = c.getMetaData();
			try (ResultSet rs = md.getColumns(null, safeSchema, safeTable, null)) {
				while (rs.next()) {
					String col = rs.getString("COLUMN_NAME");
					if (col != null && !col.isBlank()) {
						cols.add(col.toLowerCase());
					}
				}
			}
		}
		return cols;
	}

	public List<String> listTables(DataSource destino, String schema) throws SQLException {
		String safeSchema = (schema == null || schema.isBlank()) ? "public" : schema.trim();
		List<String> tables = new ArrayList<>();
		try (Connection c = destino.getConnection()) {
			var md = c.getMetaData();
			try (ResultSet rs = md.getTables(null, safeSchema, "%", new String[] { "TABLE" })) {
				while (rs.next()) {
					String t = rs.getString("TABLE_NAME");
					if (t != null && !t.isBlank()) {
						tables.add(t);
					}
				}
			}
		}
		return tables;
	}

	public List<String> listColumns(DataSource destino, String schema, String table) throws SQLException {
		String safeSchema = (schema == null || schema.isBlank()) ? "public" : schema.trim();
		String safeTable = table == null ? "" : table.trim();
		List<String> cols = new ArrayList<>();
		try (Connection c = destino.getConnection()) {
			var md = c.getMetaData();
			try (ResultSet rs = md.getColumns(null, safeSchema, safeTable, null)) {
				while (rs.next()) {
					String col = rs.getString("COLUMN_NAME");
					if (col != null && !col.isBlank()) {
						cols.add(col);
					}
				}
			}
		}
		return cols;
	}

	private static String buildInsertSql(String schema, String table, List<String> columns) {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ");
		sb.append(quoteIdent(schema)).append('.').append(quoteIdent(table));
		sb.append(" (");
		for (int i = 0; i < columns.size(); i++) {
			if (i > 0) sb.append(", ");
			sb.append(quoteIdent(columns.get(i)));
		}
		sb.append(") values (");
		for (int i = 0; i < columns.size(); i++) {
			if (i > 0) sb.append(", ");
			sb.append('?');
		}
		sb.append(')');
		return sb.toString();
	}

	private static String quoteIdent(String ident) {
		String s = ident == null ? "" : ident.trim();
		// double quote escaping
		s = s.replace("\"", "\"\"");
		return "\"" + s + "\"";
	}
}
