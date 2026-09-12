#!/bin/bash
set -e

echo "🚀 Compilando extensión de Nautilus (Release)..."
cd nautilus-ext
cargo build --release

LIB_NAME="libgdrivexp_nautilus.so"
SYSTEM_LIB_NAME="libgdrivexp-nautilus.so"
SYSTEM_DIR="/usr/lib64/nautilus/extensions-4"
TARGET_DIR_1="$HOME/.local/lib64/nautilus/extensions-4"
TARGET_DIR_2="$HOME/.local/lib/nautilus/extensions-4"

echo "📂 Creando directorios de destino..."
mkdir -p "$TARGET_DIR_1"
mkdir -p "$TARGET_DIR_2"

echo "📦 Instalando en $SYSTEM_DIR/$SYSTEM_LIB_NAME (sistema)..."
sudo cp "target/release/$LIB_NAME" "$SYSTEM_DIR/$SYSTEM_LIB_NAME"

echo "📦 Instalando en $TARGET_DIR_1..."
cp "target/release/$LIB_NAME" "$TARGET_DIR_1/"

echo "📦 Instalando en $TARGET_DIR_2..."
cp "target/release/$LIB_NAME" "$TARGET_DIR_2/"

echo "✅ Instalación completada."
echo "⚠️  Reiniciando Nautilus..."
nautilus -q 2>/dev/null || true
