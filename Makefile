VERSION    = 1.1.1
RELEASE    = 2
ARCH       = x86_64
NAME       = g-drive-xp
TARBALL    = $(NAME)-$(VERSION)-$(ARCH).tar.gz

PREFIX     = /usr
BINDIR     = $(PREFIX)/bin
LIBDIR     = $(PREFIX)/lib64
DATADIR    = $(PREFIX)/share
NAUTILUS_EXTDIR = $(LIBDIR)/nautilus/extensions-4

CLIENT_DIR   = g-drive-xp
NAUTILUS_DIR  = nautilus-ext
DIST_DIR     = dist

.PHONY: build install package rpm clean

build:
	cd $(CLIENT_DIR) && cargo build --release
	cd $(NAUTILUS_DIR) && cargo build --release

install:
	install -Dm755 $(CLIENT_DIR)/target/release/g-drive-xp $(DESTDIR)$(BINDIR)/g-drive-xp
	install -Dm755 $(NAUTILUS_DIR)/target/release/libgdrivexp_nautilus.so $(DESTDIR)$(NAUTILUS_EXTDIR)/libgdrivexp-nautilus.so
	install -Dm644 $(CLIENT_DIR)/data/org.gnome.FedoraDrive.desktop $(DESTDIR)$(DATADIR)/applications/org.gnome.FedoraDrive.desktop
	install -Dm644 $(CLIENT_DIR)/assets/logo.png $(DESTDIR)$(DATADIR)/icons/hicolor/256x256/apps/org.gnome.FedoraDrive.png
	install -Dm644 $(NAUTILUS_DIR)/icons/emblem-gdrivexp-synced.svg $(DESTDIR)$(DATADIR)/icons/hicolor/scalable/emblems/emblem-gdrivexp-synced.svg
	install -Dm644 $(NAUTILUS_DIR)/icons/emblem-gdrivexp-cloud.svg $(DESTDIR)$(DATADIR)/icons/hicolor/scalable/emblems/emblem-gdrivexp-cloud.svg
	install -Dm644 $(NAUTILUS_DIR)/icons/emblem-gdrivexp-local.svg $(DESTDIR)$(DATADIR)/icons/hicolor/scalable/emblems/emblem-gdrivexp-local.svg
	install -Dm644 $(NAUTILUS_DIR)/icons/emblem-gdrivexp-error.svg $(DESTDIR)$(DATADIR)/icons/hicolor/scalable/emblems/emblem-gdrivexp-error.svg

package: build
	rm -rf $(DIST_DIR)
	mkdir -p $(DIST_DIR)/$(NAME)-$(VERSION)
	cp $(CLIENT_DIR)/target/release/g-drive-xp $(DIST_DIR)/$(NAME)-$(VERSION)/
	cp $(NAUTILUS_DIR)/target/release/libgdrivexp_nautilus.so $(DIST_DIR)/$(NAME)-$(VERSION)/
	cp $(CLIENT_DIR)/data/org.gnome.FedoraDrive.desktop $(DIST_DIR)/$(NAME)-$(VERSION)/
	cp $(CLIENT_DIR)/assets/logo.png $(DIST_DIR)/$(NAME)-$(VERSION)/org.gnome.FedoraDrive.png
	cp $(NAUTILUS_DIR)/icons/emblem-gdrivexp-*.svg $(DIST_DIR)/$(NAME)-$(VERSION)/
	cd $(DIST_DIR) && tar czf $(TARBALL) $(NAME)-$(VERSION)/
	@echo "Tarball creado: $(DIST_DIR)/$(TARBALL)"

rpm: package
	mkdir -p ~/rpmbuild/{SPECS,SOURCES,BUILD,RPMS,SRPMS}
	cp $(DIST_DIR)/$(TARBALL) ~/rpmbuild/SOURCES/
	cp $(CLIENT_DIR)/packaging/g-drive-xp.spec ~/rpmbuild/SPECS/
	rpmbuild -bb ~/rpmbuild/SPECS/g-drive-xp.spec
	cp ~/rpmbuild/RPMS/$(ARCH)/$(NAME)-$(VERSION)-$(RELEASE).fc*.$(ARCH).rpm $(DIST_DIR)/ 2>/dev/null || true
	@echo "RPM disponible en $(DIST_DIR)/"

clean:
	cd $(CLIENT_DIR) && cargo clean
	cd $(NAUTILUS_DIR) && cargo clean
	rm -rf $(DIST_DIR)
