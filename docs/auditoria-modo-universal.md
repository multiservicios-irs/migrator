# Auditoría — Modo Universal (Migrador Universal de Datos)

Fecha: 2026-02-03  
Sistema: `migrator` (Spring Boot)  
Alcance: UI y backend del **Modo Universal** (`/universal`)  

## 1) Objetivo (para qué existe)

El **Modo Universal** existe para migrar datos **entre bases relacionales** (actualmente, pensado principalmente para PostgreSQL) de forma **genérica**, sin “conocer” un dominio específico como productos/stock.

En términos prácticos:
- El usuario escribe un **SQL de origen** (lo que extrae datos).
- El usuario elige una **tabla destino** (schema + tabla).
- El usuario define un **mapeo** (columna de origen → columna destino) y reglas (Directo/Default/Constante/Expresión).
- El sistema ejecuta el proceso: **extraer → transformar (por reglas) → cargar**.

### Objetivos principales
- **Acelerar migraciones** de tablas “planas” (ej: personas, clientes, proveedores) sin implementar loaders específicos.
- Permitir **iteración rápida** con *preview* + *dry run*.
- Reducir errores de mapeo con autocompletado y ayudas de UX.

### No objetivos (por ahora)
- No es un ETL completo (sin control avanzado de incrementalidad, CDC, historización, etc.).
- No reemplaza migraciones “de negocio” complejas donde hay lógica de dominio (ej: Producto + Stock + Movimientos, normalizaciones, etc.).
- No es un administrador de BD ni un “SQL runner” para uso general; se usa para migración controlada.

## 2) Estado actual (cómo funciona hoy)

### 2.1 Endpoints principales
Backend controlador: [src/main/java/com/multiservicios/migrator/controller/UniversalMigrationController.java](../src/main/java/com/multiservicios/migrator/controller/UniversalMigrationController.java)

- `GET /universal`
  - Renderiza la página [src/main/resources/templates/universal.html](../src/main/resources/templates/universal.html)

- `POST /universal/test-connection`
  - Prueba conexión origen y destino (mensaje “✓/✗”).

- `POST /universal/preview`
  - Ejecuta preview seguro: envuelve el SQL en subquery y aplica `LIMIT`.
  - Devuelve `previewRows` y `previewColumns` a la vista.

- `POST /universal/list-tables`
  - Lista tablas del destino por schema.

- `POST /universal/describe-table`
  - Lista columnas de una tabla destino.

- `POST /universal/run`
  - Ejecuta la migración (runner universal) con `dryRun` opcional.
  - Devuelve `runResult` y `runLogs`.

### 2.2 UI/UX actual
Vista: [src/main/resources/templates/universal.html](../src/main/resources/templates/universal.html)

- **Paso 0**: credenciales origen/destino + botón **Probar conexión**.
- **Paso 1**: SQL (origen) + **Límite preview** + botón **Probar SQL** + tabla **Preview**.
- **Paso 2**: Selección de schema/tabla destino + botones para **Cargar tablas/columnas**.
- **Paso 3**: Mapeo (Manual)
  - `Campo Origen` ahora tiene autocompletado a partir de `previewColumns`.
  - `Campo Destino` autocompleta desde columnas leídas del destino.
  - Botón **Auto-map por nombre**: si una columna destino coincide (case-insensitive) con columna de origen, crea el mapping Directo.
  - Botón **Ver JSON** para inspección/edición.
- Logs: se muestran en pantalla y se pueden copiar.

### 2.3 Controles de seguridad básicos
- Sanitización de SQL:
  - Se eliminan `;` finales y se bloquean múltiples sentencias (`;` en el medio).
  - Se aplica `select * from (<sql>) t limit N` para preview.

> Nota: esto reduce riesgos de múltiples sentencias, pero **no elimina** la posibilidad de que un SQL sea destructivo si el DB user tiene permisos (p.ej. `select` no destruye, pero un SQL podría contener funciones peligrosas o hacer locking). Ver sección de riesgos.

## 3) Hallazgos de auditoría (qué falta vs el migrador de productos)

### 3.1 Brecha funcional/UX (comparado con el migrador de productos)
1) **No había Probar conexión / Preview / Límite**: se agregó para igualar el flujo.
2) **No había lista de columnas origen**: ahora existe vía `previewColumns` + `datalist`.
3) **No había ayudas de mapeo**: se agregó `Auto-map por nombre`.

Aun así, quedan diferencias importantes:
- El migrador de productos tiene **targetFields** específicos del DTO; Universal trabaja por columnas de tabla.
- El migrador de productos incluye “plantillas” de mapeo por negocio; Universal debería tener **plantillas genéricas** por tabla (opcional).

### 3.2 Riesgos técnicos
- **Permisos de DB**: si el usuario de conexión tiene permisos peligrosos, el SQL puede afectar el sistema.
- **SQL arbitrario**: aunque se limita a una sentencia, el SQL puede ser pesado (joins grandes, locks, full scans) y degradar la BD.
- **Contraseñas en UI**: se envían como texto; falta mejorar experiencia (tipo password) y posiblemente evitar que queden persistidas en logs.

### 3.3 Riesgos de calidad de datos
- **Mismatch de tipos**: el mapping “Directo” debe ser compatible con el tipo de columna destino.
- **Nullability**: no hay verificación previa de NOT NULL sin default.
- **Unicidad / duplicates**: no hay aún estrategia genérica de upsert (depende del loader universal). Esto puede causar errores al insertar.

## 4) Recomendaciones (mejoras propuestas)

### 4.1 Mejoras UX (alto impacto, baja complejidad)
1) **Botón “Auto-map inteligente”** (además del por nombre):
   - Soportar equivalencias `snake_case` ↔ `camelCase`.
   - Soportar prefijos comunes: `id_`, `fk_`, etc.
   - Soportar alias manuales (diccionario editable).

2) **Preview del destino** (opcional):
   - Mostrar tipos de columnas destino (no solo nombres).
   - Resaltar columnas NOT NULL.

3) **Validaciones antes del run**:
   - Chequear que todos los targets existan.
   - Avisar si faltan columnas NOT NULL sin mapeo.

4) **Mejor manejo de credenciales**:
   - Inputs password con `type="password"`.
   - Opción “no recordar”/“limpiar” en UI.

### 4.2 Mejoras de robustez (medio impacto)
1) **Limit de seguridad y timeout**:
   - Tiempo máximo de preview.
   - Límite de filas para run (modo “batch”) si corresponde.

2) **Registro de auditoría**:
   - Guardar por ejecución: sql hash, tabla, cantidad, ok/fail, fecha.
   - Exportar como JSON/CSV.

3) **Estrategia de upsert configurable**:
   - Insert-only vs upsert by key.
   - Mapeo de “clave natural” (por UI).

### 4.3 Mejoras de seguridad (importante si se usa en producción)
1) Usar usuarios de DB con **permiso mínimo** (ideal: solo SELECT en origen; INSERT/UPDATE acotado en destino).
2) Opcional: “modo seguro” que permita solo `SELECT` (bloqueando keywords peligrosas) para preview.
3) Cifrar/ocultar contraseñas en logs y evitar exponerlas en respuestas.

## 5) Pruebas recomendadas (checklist)

### Funcionales
- Probar conexión (origen OK, destino OK, credenciales incorrectas).
- Preview SQL con `LIMIT` y SQL con `;` (debe rechazar múltiples sentencias).
- Cargar tablas y columnas destino.
- Auto-map por nombre con columnas iguales y con mayúsculas/minúsculas.
- Ejecutar `dryRun=true` y `dryRun=false`.

### Datos
- Tabla destino con NOT NULL sin default: debe alertar (cuando implementemos validación).
- Tipos: numérico → varchar, varchar → numérico (ver comportamiento actual del loader universal).

### Performance
- Preview con join grande (asegurar límites).

## 6) Roadmap sugerido

**Ronda 1 (rápida, UX y validaciones)**
- Auto-map inteligente (snake_case/camelCase + diccionario de alias).
- Inputs password.
- Validación de targets y NOT NULL.

**Ronda 2 (calidad y control)**
- Auditoría persistida por ejecución.
- Upsert configurable.
- Timeouts.

**Ronda 3 (endurecimiento)**
- Modo seguro/whitelist.
- Roles mínimos y guías de despliegue.

## 7) “Definition of Done” (cómo sabemos que mejoró)

- El usuario puede correr una migración Universal sin tener que “adivinar” columnas.
- El preview muestra columnas reales y se puede mapear con autocompletado.
- El sistema advierte errores comunes antes del run (NOT NULL/targets inexistentes).
- Logs copiable y resultados claros (total/ok/fail).

---

## Apéndice A — Archivos relevantes
- [src/main/java/com/multiservicios/migrator/controller/UniversalMigrationController.java](../src/main/java/com/multiservicios/migrator/controller/UniversalMigrationController.java)
- [src/main/resources/templates/universal.html](../src/main/resources/templates/universal.html)
- Comparación UX: [src/main/resources/templates/index.html](../src/main/resources/templates/index.html)
