package com.multiservicios.migrator.extract;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.multiservicios.migrator.model.RowData;

@Component
public class SqlExtractor {
	public List<RowData> extract(DataSource dataSource, String sql) {
		JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
		List<java.util.Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
		return rows.stream().map(RowData::new).toList();
	}
}
