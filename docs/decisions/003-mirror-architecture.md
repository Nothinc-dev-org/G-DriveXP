# ADR-003: Arquitectura Espejo (Mirror) con FUSE oculto

## Estado
Aceptada

## Contexto
El usuario necesita ver sus archivos en `~/GoogleDrive/`, pero el montaje FUSE directo tiene limitaciones para aplicaciones Flatpak (sandbox) y no permite diferenciar entre archivos "Online Only" y "Local & Online".

## Decisión
Arquitectura de dos capas:
1. **FUSE Mount** (oculto): `~/GoogleDrive/FUSE_Mount/` — montaje real de fuse3.
2. **Mirror** (visible): `~/GoogleDrive/` — directorio espejo gestionado por `MirrorManager`.

Los archivos "Online Only" son symlinks que apuntan a `FUSE_Mount/`. Los "Local & Online" son copias reales.

## Justificación
- **Flatpak**: las apps sandboxed pueden acceder a archivos reales pero no siempre a mountpoints FUSE.
- **Flexibilidad**: el usuario puede elegir qué archivos mantener localmente.
- **`.hidden`**: el directorio `FUSE_Mount` se oculta en Nautilus via archivo `.hidden`.

## Consecuencias
- El `MirrorManager` DEBE iniciarse DESPUÉS de montar FUSE (race condition → deadlock).
- El `MirrorWatcher` debe ignorar eventos dentro de `FUSE_Mount/` para evitar loops infinitos.
- Cambios locales en el espejo (edición de archivos reales) son detectados por el watcher y subidos por el Uploader.
