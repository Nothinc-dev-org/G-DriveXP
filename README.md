# G-DriveXP

Cliente nativo de Google Drive para Fedora Workstation/GNOME, escrito en Rust.
Monta un sistema de archivos virtual FUSE asíncrono, sincroniza metadatos y
contenido bidireccionalmente, y se integra con Nautilus mediante extensión nativa.

## Estructura

| Directorio      | Contenido |
|-----------------|-----------|
| `g-drive-xp/`   | Crate principal: daemon, GUI (GTK4/Libadwaita), FUSE, sync, SQLite |
| `nautilus-ext/` | Extensión de Nautilus (cdylib). Espejo del repo [G-DriveXp-nautilus-ext](https://github.com/Nothinc-dev-org/G-DriveXp-nautilus-ext) |
| `docs/`         | Arquitectura (`architecture.md`) y decisiones (`decisions/`) |

## Compilar y empaquetar

```bash
make build    # ambos crates en release
make package  # tarball dist/g-drive-xp-<ver>-x86_64.tar.gz
make rpm      # RPM de Fedora en dist/
make install  # instala en el sistema (DESTDIR soportado)
```

## Instalación (usuarios)

```bash
curl -fsSL https://raw.githubusercontent.com/Nothinc-dev-org/G-DriveXP/v1.1.0/g-drive-xp/packaging/install.sh | bash
```

O descarga el RPM desde [Releases](https://github.com/Nothinc-dev-org/G-DriveXP/releases/latest).

## Documentación

- [docs/architecture.md](docs/architecture.md) — arquitectura canónica
- [g-drive-xp/README.md](g-drive-xp/README.md) — características e instalación del cliente
- [Plan-Desarrollo.md](Plan-Desarrollo.md) — plan técnico
