# ADR-005: GUI Relm4 en hilo principal, Backend Tokio en hilo separado

## Estado
Aceptada

## Contexto
GTK4 requiere que todas las operaciones de widgets ocurran en el hilo principal. El backend (FUSE, sync, API) es completamente asíncrono sobre Tokio. Ejecutar ambos en el mismo hilo causaría bloqueos mutuos.

## Decisión
- `main()` inicia `RelmApp` (GTK main loop en el hilo principal).
- `AppModel::init()` lanza `run_backend()` en `std::thread::spawn`.
- `run_backend()` crea su propio `tokio::runtime::Builder::new_multi_thread()`.
- La comunicación backend → GUI se realiza via `ComponentSender<AppModel>` (mensajes `AppMsg`).

## Justificación
- **Separación de runtimes**: GTK main loop y Tokio runtime coexisten sin interferencia.
- **Thread safety**: Relm4 garantiza que los mensajes se despachan al hilo principal GTK.
- **Terminación**: `std::process::exit(0)` fuerza la salida porque GTK no responde a señales del backend.

## Consecuencias
- El backend no puede acceder a widgets GTK directamente; todo pasa por `AppMsg`.
- La señal Ctrl+C se captura en el backend thread via `tokio::signal::ctrl_c()`.
- `HARD_RESET_IN_PROGRESS` (AtomicBool global) coordina el cierre entre ambos hilos.
