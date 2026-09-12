#!/bin/bash
# Script de instalación de iconos y archivo .desktop para G-DriveXP
# Ejecutar desde la raíz del proyecto: ./scripts/install-icons.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

APP_ID="org.gnome.FedoraDrive"
ICON_SOURCE="$PROJECT_DIR/assets/logo.png"

echo "🎨 Instalando iconos de G-DriveXP..."

# Crear directorios de iconos
mkdir -p ~/.local/share/icons/hicolor/48x48/apps
mkdir -p ~/.local/share/icons/hicolor/128x128/apps
mkdir -p ~/.local/share/icons/hicolor/256x256/apps
mkdir -p ~/.local/share/applications

# Redimensionar y copiar iconos
echo "  → Generando iconos en múltiples resoluciones..."

if command -v convert &> /dev/null; then
    convert "$ICON_SOURCE" -resize 48x48 ~/.local/share/icons/hicolor/48x48/apps/${APP_ID}.png
    convert "$ICON_SOURCE" -resize 128x128 ~/.local/share/icons/hicolor/128x128/apps/${APP_ID}.png
    convert "$ICON_SOURCE" -resize 256x256 ~/.local/share/icons/hicolor/256x256/apps/${APP_ID}.png
else
    echo "  ⚠️  ImageMagick no encontrado. Copiando icono sin redimensionar..."
    cp "$ICON_SOURCE" ~/.local/share/icons/hicolor/128x128/apps/${APP_ID}.png
fi

# Copiar archivo .desktop
echo "  → Instalando archivo .desktop..."
cp "$PROJECT_DIR/data/${APP_ID}.desktop" ~/.local/share/applications/

# Crear symlink del binario en ~/.local/bin/ (GIO valida Exec y descarta el .desktop si no encuentra el binario)
echo "  → Creando symlink del binario en ~/.local/bin/..."
mkdir -p ~/.local/bin
BINARY="$PROJECT_DIR/target/release/g-drive-xp"
if [ ! -f "$BINARY" ]; then
    BINARY="$PROJECT_DIR/target/debug/g-drive-xp"
fi
if [ -f "$BINARY" ]; then
    ln -sf "$BINARY" ~/.local/bin/g-drive-xp
    echo "    Enlace: ~/.local/bin/g-drive-xp → $BINARY"
else
    echo "  ⚠️  Binario no encontrado. Compile con 'cargo build --release' primero."
fi

# Actualizar caché de iconos
echo "  → Actualizando caché de iconos..."
if command -v gtk-update-icon-cache &> /dev/null; then
    gtk-update-icon-cache -f -t ~/.local/share/icons/hicolor/ 2>/dev/null || true
fi

# Actualizar base de datos de aplicaciones
if command -v update-desktop-database &> /dev/null; then
    update-desktop-database ~/.local/share/applications/ 2>/dev/null || true
fi

echo ""
echo "✅ Instalación completada."
echo ""
echo "📌 Notas:"
echo "   - Es posible que necesite reiniciar GNOME Shell (Alt+F2 → 'r' → Enter)"
echo "   - O cerrar sesión y volver a iniciar"
echo "   - El icono debería aparecer en el dock al ejecutar la aplicación"
