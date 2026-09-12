# ADR-034: Mirror degradado visible + Reiniciar en un clic

- **Estado**: Aceptado
- **Fecha**: 2026-09-12
- **Contexto**: Auditoría del claim "Mirror sin reinicio" (`main.rs` max_restarts=0, ADR-021).
  Confirmado con dos matices: (1) el texto de `Died` prometía "reiniciando…" para un
  servicio que jamás relanza; (2) peor: el watcher podía morir con la tarea viva
  (fallo de init en `spawn`, o de re-creación tras Refresh/RemoteDeleted/RemoteRestored)
  y entonces no había ni alarma del supervisor — ceguera total sin `Died` ni `GaveUp`.

## Decisión

Dos piezas pequeñas, sin tocar el canal del mirror (el bloqueador de ADR-021):

1. **Auto-recuperación dentro de la tarea** (`mirror/manager.rs`):
   - `ensure_watcher(why)` centraliza los 4 sitios que (re)crean el watcher.
   - Si falla: latcha `MIRROR_DEGRADED=true` + 1 entrada en el historial por transición
     (sin spam) y lo deja para un tick de 60 s en el `run_loop` que reintenta sin
     bloquear comandos. Si tiene éxito: limpia el flag + entrada de recuperación.
2. **Degradado visible + recuperación en un clic** (`main.rs`, `gui/app_model.rs`):
   - `GaveUp{service=="mirror"}` latcha `MIRROR_DEGRADED` (el `Died` ya no miente:
     dice "usa Reiniciar" en vez de "reiniciando…" solo para mirror).
   - La GUI sondea el flag en `RefreshActivity` (cada 2 s, sin canal nuevo — mismo
     idioma que `HARD_RESET_IN_PROGRESS`/`SHUTDOWN_REQUESTED`) y muestra banner
     persistente en Estado + fila "Reiniciar" en Cuenta (solo visibles degradado).
   - `AppMsg::Restart`: programa relanzado del binario actual con espera activa al
     propio PID (`while kill -0 …`, sin carrera con el desmontaje FUSE y el marcador
     limpio) y luego `request_shutdown()` coordinado — mismo patrón que HardReset
     pero con apagado limpio en vez de `exit(0)` inmediato.

## Alternativas descartadas

- **Swap de canal con `ArcSwap` del sender en ~12 sitios** (syncer struct+calls, ipc
  `Option`, bootstrap params, shutdown, factorías): coordinación con races (envíos
  durante el swap caen al canal muerto), 4 módulos tocados, para un evento sin
  frecuencia observada. Desproporción.
- **`catch_unwind` por mensaje**: cero `unwrap`/`expect`/`panic!` en código productivo
  de `manager.rs` (todo es `unwrap_or*` o tests) — blindar panics no observados en
  código disciplinado en `Result` es ruido.
- **Subir `max_restarts` sin más**: la factoría post-`take()` genera dummies vacíos que
  terminan al instante — serían `Died{panic:false}` cada 5 s hasta `GaveUp` a los
  ~30 s. Ruido, no recovery.
- **Nada (status quo)**: dejaba intacto el fallo silencioso real (watcher muerto con
  tarea viva).

## Consecuencias

- El espejo jamás queda ciego en silencio: o se recupera solo (tick) o queda
  latchado visible con recuperación en un clic.
- `AppMsg::Restart` no se testea en unit (relanzaría el proceso de test); el flujo
  se verifica manual. `ensure_watcher` sí: `ensure_watcher_degrada_y_recupera`.
- Sin cambios de firmas públicas salvo el re-export de `MIRROR_DEGRADED` en
  `mirror/mod.rs`; sin dependencias nuevas.
