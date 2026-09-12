
import ctypes
import os

lib_path = os.path.expanduser("~/.local/share/nautilus/extensions-4/libgdrivexp_nautilus.so")

print(f"Loading library from: {lib_path}")

try:
    lib = ctypes.CDLL(lib_path, mode=ctypes.RTLD_GLOBAL)
    print("Library loaded successfully!")
except OSError as e:
    print(f"Failed to load library: {e}")
    exit(1)

try:
    init_func = lib.nautilus_module_initialize
    print(f"Symbol 'nautilus_module_initialize' found: {init_func}")
except AttributeError:
    print("Symbol 'nautilus_module_initialize' NOT FOUND!")
    exit(1)

try:
    list_func = lib.nautilus_module_list_types
    print(f"Symbol 'nautilus_module_list_types' found: {list_func}")
except AttributeError:
    print("Symbol 'nautilus_module_list_types' NOT FOUND!")
    exit(1)

print("✅ Library seems valid and exports required symbols.")
