# ADR-010: Shutdown Reactivo y Manejo de Señales Nativo (Tokio)

## Estado
Aceptado

## Contexto

El sistema original de cierre coordinado (`utils::shutdown`) dependía del crate `ctrlc` para interceptar señales OS (`SIGINT`, `SIGTERM`), y de una variable global `AtomicBool` evaluada mediante un ciclo infinito (`loop`) con retardos (`tokio::time::sleep` de 100ms) dentro de la función `wait_for_shutdown()`. Esto provocaba dos problemas principales:
1. **Busy Waiting**: El *executor* multi-hilo despertaba tareas múltiples veces por segundo sin justificación I/O, consumiendo ciclos inactivos del CPU.
2. **Colisión de Manejadores**: Al invocar `tokio::signal::ctrl_c()` dentro del backend, el *driver* nativo sobrescribía la interceptación que `ctrlc` había registrado pre-arranque, creando condiciones de carrera y pérdida potencial de interrupciones de sistema.

## Decisión

Se prescindió completamente de la biblioteca síncrona `ctrlc` asumiendo la orquestación directa por Tokio.

1. **Reemplazo del Bucle de Sondeo por Notificaciones (Notify)**
   La función `wait_for_shutdown()` ahora invoca `SHUTDOWN_NOTIFY.notified().await` (usando `tokio::sync::Notify::const_new()`). Los estados se suspenden hasta que ocurre el evento, maximizando la eficiencia computacional y bajando la latencia a 0 ms. Se mantuvo `SHUTDOWN_REQUESTED` (*AtomicBool*) como estado auxiliar para comprobaciones rápidas síncronas.

2. **Centralización de Señales de SO**
   En `main.rs`, en el corazón de `run_backend()`, se creó un `tokio::spawn` explícito escuchando simétrica y asíncronamente tanto `tokio::signal::unix::signal(SignalKind::terminate())` como `tokio::signal::ctrl_c()`. Este hilo emite `request_shutdown()` a todos los subsistemas.

3. **Inmunidad en las Pruebas (Tests)**
   Se introdujo un `std::sync::Mutex` dentro de `utils::shutdown::tests` para asegurar la exclusión mutua de las pruebas al alterar el `AtomicBool` global, evitando mutabilidades en paralelo.

## Consecuencias

- **Positivas**:
  - Remoción de una dependencia innecesaria del binario y tiempos de enlace (`Cargo.toml`).
  - Resolución del consumo superfluo de energía térmica/batería por la supresión del *sleep-looping*.
  - Ciclo de vida estandarizado en el ecosistema asíncrono; sin hilos ocultos para `sigaction`.
- **Negativas**:
  - Ausencia de registro de señales antes del primer microsegundo post-inicialización del UI. Dada su irrelevancia marginal, es asumible.

## Archivos Afectados

| Archivo | Cambio |
|---------|--------|
| `g-drive-xp/Cargo.toml` | Eliminación de dependencia `ctrlc`. |
| `g-drive-xp/src/utils/shutdown.rs` | Refactor a `Notify`, borrado de `register_shutdown_handlers`, exclusión mutua para suite de testing. |
| `g-drive-xp/src/main.rs` | Supresión de intercepciones estáticas. Unificación del loop de finalización. |
