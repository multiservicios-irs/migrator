package com.multiservicios.migrator.controller;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.multiservicios.migrator.session.MigrationProcessorService;
import com.multiservicios.migrator.session.MigrationSession;
import com.multiservicios.migrator.session.MigrationSessionStatus;
import com.multiservicios.migrator.session.SqlBufferService;
import com.multiservicios.migrator.session.requests.LoadSqlRequest;
import com.multiservicios.migrator.session.requests.RunAllRequest;
import com.multiservicios.migrator.session.requests.RunNextRequest;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/migration")
@Validated
public class MigrationSessionController {
	private final SqlBufferService bufferService;
	private final MigrationProcessorService processorService;

	public MigrationSessionController(SqlBufferService bufferService, MigrationProcessorService processorService) {
		this.bufferService = bufferService;
		this.processorService = processorService;
	}

	@PostMapping("/load-sql")
	public ResponseEntity<MigrationSessionStatus> loadSql(@Valid @RequestBody LoadSqlRequest req) {
		MigrationSession session = bufferService.createSession(req.getOrigenProfile(), req.getSql(), req.getMaxRows());
		return ResponseEntity.ok(bufferService.status(session.getId(), 20));
	}

	@PostMapping("/run-next")
	public ResponseEntity<MigrationSessionStatus> runNext(@Valid @RequestBody RunNextRequest req) {
		return ResponseEntity.ok(processorService.runNext(req.getSessionId(), req.isDryRun()));
	}

	@PostMapping("/run-all")
	public ResponseEntity<MigrationSessionStatus> runAll(@Valid @RequestBody RunAllRequest req) {
		return ResponseEntity.ok(processorService.runAll(req.getSessionId(), req.isDryRun(), req.getMaxItems()));
	}

	@GetMapping("/status")
	public ResponseEntity<?> status(
			@RequestParam(name = "sessionId", required = false) java.util.UUID sessionId,
			@RequestParam(name = "logs", defaultValue = "20") int logs) {
		int safeLogs = Math.max(1, Math.min(200, logs));
		if (sessionId != null) {
			return ResponseEntity.ok(bufferService.status(sessionId, safeLogs));
		}
		List<MigrationSessionStatus> list = bufferService.listStatuses(safeLogs);
		return ResponseEntity.ok(list);
	}

	@GetMapping("/errors")
	public ResponseEntity<?> errors(
			@RequestParam(name = "sessionId") java.util.UUID sessionId,
			@RequestParam(name = "max", defaultValue = "200") int max) {
		int safeMax = Math.max(1, Math.min(5000, max));
		return ResponseEntity.ok(bufferService.failedRows(sessionId, safeMax));
	}

	@org.springframework.web.bind.annotation.DeleteMapping("/session/{id}")
	public ResponseEntity<?> deleteSession(@org.springframework.web.bind.annotation.PathVariable("id") java.util.UUID id) {
		boolean removed = bufferService.delete(id);
		return ResponseEntity.ok(java.util.Map.of("deleted", removed));
	}

	@org.springframework.web.bind.annotation.DeleteMapping("/sessions")
	public ResponseEntity<?> deleteAllSessions() {
		int deleted = bufferService.deleteAll();
		return ResponseEntity.ok(java.util.Map.of("deleted", deleted));
	}
}
