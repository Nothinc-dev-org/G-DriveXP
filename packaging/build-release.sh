#!/bin/bash
# Build Release — Genera todos los artefactos de release de G-DriveXP.
#
# Ejecutar desde la raíz del repo g-drive-xp:
#   ./packaging/build-release.sh
#
# El repo nautilus-ext debe estar clonado como directorio hermano.
# Ejemplo de estructura esperada:
#   parent/
#   ├── G-DriveXP/              (este repo)
#   └── G-DriveXp-nautilus-ext/ (repo de la extensión)
#
# Requisitos: cargo, rpmbuild (opcional, para RPM)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PARENT_DIR="$(cd "$REPO_ROOT/.." && pwd)"
VERSION="1.0.0"
ARCH="x86_64"
NAME="g-drive-xp"
TARBALL="${NAME}-${VERSION}-${ARCH}.tar.gz"
DIST_DIR="$REPO_ROOT/dist"

info()  { echo -e "\033[1;34m==>\033[0m $*"; }
ok()    { echo -e "\033[1;32m==>\033[0m $*"; }
error() { echo -e "\033[1;31m==>\033[0m $*" >&2; exit 1; }

# Detectar directorio de nautilus-ext como hermano
NAUTILUS_DIR=""
for candidate in "$PARENT_DIR/nautilus-ext" "$PARENT_DIR/G-DriveXp-nautilus-ext"; do
    if [ -f "$candidate/Cargo.toml" ]; then
        NAUTILUS_DIR="$candidate"
        break
    fi
done

if [ -z "$NAUTILUS_DIR" ]; then
    error "No se encontró el repo nautilus-ext. Clónalo como directorio hermano:
    git clone https://github.com/Nothinc-dev-org/G-DriveXp-nautilus-ext.git $(dirname "$REPO_ROOT")/G-DriveXp-nautilus-ext"
fi

info "Compilando g-drive-xp (release)..."
cd "$REPO_ROOT" && cargo build --release

info "Compilando nautilus-ext (release)..."
cd "$NAUTILUS_DIR" && cargo build --release

info "Preparando tarball..."
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR/${NAME}-${VERSION}"

cp "$REPO_ROOT/target/release/g-drive-xp" "$DIST_DIR/${NAME}-${VERSION}/"
cp "$NAUTILUS_DIR/target/release/libgdrivexp_nautilus.so" "$DIST_DIR/${NAME}-${VERSION}/"
cp "$REPO_ROOT/data/org.gnome.FedoraDrive.desktop" "$DIST_DIR/${NAME}-${VERSION}/"
cp "$REPO_ROOT/assets/icons/org.gnome.FedoraDrive-48.png" "$DIST_DIR/${NAME}-${VERSION}/"
cp "$REPO_ROOT/assets/icons/org.gnome.FedoraDrive-128.png" "$DIST_DIR/${NAME}-${VERSION}/"
cp "$REPO_ROOT/assets/icons/org.gnome.FedoraDrive-256.png" "$DIST_DIR/${NAME}-${VERSION}/"
cp "$NAUTILUS_DIR"/icons/emblem-gdrivexp-*.svg "$DIST_DIR/${NAME}-${VERSION}/"

cd "$DIST_DIR"
tar czf "$TARBALL" "${NAME}-${VERSION}/"
ok "Tarball: $DIST_DIR/$TARBALL"

if command -v rpmbuild &>/dev/null; then
    info "Construyendo RPM..."
    mkdir -p ~/rpmbuild/{SPECS,SOURCES,BUILD,RPMS,SRPMS}
    cp "$DIST_DIR/$TARBALL" ~/rpmbuild/SOURCES/
    cp "$REPO_ROOT/packaging/g-drive-xp.spec" ~/rpmbuild/SPECS/
    rpmbuild -bb ~/rpmbuild/SPECS/g-drive-xp.spec
    RPM_FILE=$(find ~/rpmbuild/RPMS/"$ARCH"/ -name "${NAME}-${VERSION}-*.rpm" -type f | head -1)
    if [ -n "$RPM_FILE" ]; then
        cp "$RPM_FILE" "$DIST_DIR/"
        ok "RPM: $DIST_DIR/$(basename "$RPM_FILE")"
    fi
else
    info "rpmbuild no encontrado — saltando construcción de RPM."
    info "Instala con: sudo dnf install rpm-build"
fi

cp "$REPO_ROOT/packaging/install.sh" "$DIST_DIR/"

echo ""
ok "Release v${VERSION} lista."
echo ""
echo "  Artefactos en $DIST_DIR/:"
ls -1 "$DIST_DIR/" | grep -v "^${NAME}-${VERSION}$" | sed 's/^/    /'
echo ""
