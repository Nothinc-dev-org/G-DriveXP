#!/bin/bash
# G-DriveXP Installer
# Descarga e instala G-DriveXP con su extensión de Nautilus desde GitHub Releases.
#
# Uso:
#   curl -fsSL https://raw.githubusercontent.com/Nothinc-dev-org/G-DriveXP/v1.0.0/packaging/install.sh | bash

set -euo pipefail

VERSION="1.0.0"
REPO="Nothinc-dev-org/G-DriveXP"
ARCH="$(uname -m)"
TARBALL="g-drive-xp-${VERSION}-${ARCH}.tar.gz"
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/v${VERSION}/${TARBALL}"
TMP_DIR="$(mktemp -d)"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

info()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
error() { echo -e "\033[1;31m[ERROR]\033[0m $*" >&2; exit 1; }
ok()    { echo -e "\033[1;32m[OK]\033[0m    $*"; }

if [ "$ARCH" != "x86_64" ]; then
    error "Arquitectura no soportada: $ARCH. Solo x86_64 está disponible."
fi

if [ "$(id -u)" -eq 0 ]; then
    error "No ejecutes este script como root. Se pedirá sudo cuando sea necesario."
fi

info "Instalando dependencias del sistema..."
sudo dnf install -y --quiet \
    fuse3 gtk4 libadwaita sqlite gnome-keyring nautilus 2>/dev/null || \
    info "Algunas dependencias ya estaban instaladas."

info "Descargando G-DriveXP v${VERSION}..."
curl -fSL "$DOWNLOAD_URL" -o "$TMP_DIR/$TARBALL" || \
    error "No se pudo descargar el paquete. Verifica tu conexión o que la versión exista."

info "Extrayendo archivos..."
tar xzf "$TMP_DIR/$TARBALL" -C "$TMP_DIR"

SRC="$TMP_DIR/g-drive-xp-${VERSION}"

info "Instalando binario..."
sudo install -Dm755 "$SRC/g-drive-xp" /usr/bin/g-drive-xp

info "Instalando extensión de Nautilus..."
sudo install -Dm755 "$SRC/libgdrivexp_nautilus.so" /usr/lib64/nautilus/extensions-4/libgdrivexp-nautilus.so

info "Instalando archivo .desktop..."
sudo install -Dm644 "$SRC/org.gnome.FedoraDrive.desktop" /usr/share/applications/org.gnome.FedoraDrive.desktop

info "Instalando iconos..."
sudo install -Dm644 "$SRC/org.gnome.FedoraDrive.png" /usr/share/icons/hicolor/256x256/apps/org.gnome.FedoraDrive.png
for emblem in "$SRC"/emblem-gdrivexp-*.svg; do
    sudo install -Dm644 "$emblem" "/usr/share/icons/hicolor/scalable/emblems/$(basename "$emblem")"
done

info "Actualizando cachés..."
sudo gtk-update-icon-cache -f -t /usr/share/icons/hicolor/ 2>/dev/null || true
sudo update-desktop-database /usr/share/applications/ 2>/dev/null || true

echo ""
ok "G-DriveXP v${VERSION} instalado correctamente."
echo ""
echo "  Ejecuta:  g-drive-xp"
echo ""
echo "  Para que los emblemas de Nautilus aparezcan, reinicia el explorador:"
echo "    nautilus -q"
echo ""
