# ADR-025 — Workspace read-only: EROFS en open/write/setattr + skip en uploader

Fecha: 2026-09-12. Estado: aceptado e implementado
(`fuse/filesystem.rs`, `sync/uploader.rs`).

## Contexto

Claim 14: `open()` aceptaba todo (ignoraba `_flags`), `write()` validaba
cero. Un Doc/Sheet abierto desde el mirror (vía symlink → inodo FUSE) se
dejaba editar; el write marcaba dirty y el uploader reintentaba para siempre
un `update` binario que la API de Drive rechaza por diseño (los Workspace
nativos no aceptan contenido). Loop eterno quemando cuota + divergencia
silenciosa local↔Drive (el editor decía "guardado").

## Decisión

- `open()`: si mime es Workspace (`shortcuts::is_workspace_file`, ya
  existente y testeado) e intención de escritura (`O_ACCMODE != O_RDONLY`)
  → `EROFS`. Fail fast: el editor lo ve read-only. Los symlinks no necesitan
  fix propio (el kernel los resuelve al inodo destino).
- `write()` y rama size de `setattr()`: mismo `EROFS` (defensa en
  profundidad; truncate también genera dirty imposible).
- Uploader `update_file()`: skip sin quemar API para dirties previos al
  guard, con `warn` visible (requiere atención manual: el contenido local
  diverge y no hay camino automático para convergerlo).

## Verificación

Tests de `write_intent` (O_RDONLY vs WRONLY/RDWR/CREAT/TRUNC/APPEND) +
`is_workspace_file` preexistentes. Suite 173 passed, 0 failed; build
0 warnings.
