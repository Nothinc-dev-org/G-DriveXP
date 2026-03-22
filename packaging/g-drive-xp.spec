%global debug_package %{nil}

Name:           g-drive-xp
Version:        1.0.0
Release:        1%{?dist}
Summary:        Cliente nativo de Google Drive para Fedora/GNOME
License:        GPL-3.0-only
URL:            https://github.com/Nothinc-dev-org/G-DriveXP
Source0:        %{name}-%{version}-x86_64.tar.gz

ExclusiveArch:  x86_64

Requires:       fuse3
Requires:       gtk4
Requires:       libadwaita
Requires:       sqlite
Requires:       gnome-keyring
Requires:       nautilus

%description
G-DriveXP es un cliente nativo de Google Drive para Fedora Workstation/GNOME.
Monta un sistema de archivos virtual FUSE, sincroniza metadatos y contenido
bidireccionalmente, e incluye una extensión de Nautilus con emblemas de estado.

%prep
%setup -q -n %{name}-%{version}

%install
install -Dm755 g-drive-xp %{buildroot}%{_bindir}/g-drive-xp
install -Dm755 libgdrivexp_nautilus.so %{buildroot}%{_libdir}/nautilus/extensions-4/libgdrivexp-nautilus.so
install -Dm644 org.gnome.FedoraDrive.desktop %{buildroot}%{_datadir}/applications/org.gnome.FedoraDrive.desktop
install -Dm644 org.gnome.FedoraDrive.png %{buildroot}%{_datadir}/icons/hicolor/256x256/apps/org.gnome.FedoraDrive.png
install -Dm644 emblem-gdrivexp-synced.svg %{buildroot}%{_datadir}/icons/hicolor/scalable/emblems/emblem-gdrivexp-synced.svg
install -Dm644 emblem-gdrivexp-cloud.svg %{buildroot}%{_datadir}/icons/hicolor/scalable/emblems/emblem-gdrivexp-cloud.svg
install -Dm644 emblem-gdrivexp-local.svg %{buildroot}%{_datadir}/icons/hicolor/scalable/emblems/emblem-gdrivexp-local.svg
install -Dm644 emblem-gdrivexp-error.svg %{buildroot}%{_datadir}/icons/hicolor/scalable/emblems/emblem-gdrivexp-error.svg

%post
/usr/bin/gtk-update-icon-cache -f -t %{_datadir}/icons/hicolor/ 2>/dev/null || :
/usr/bin/update-desktop-database %{_datadir}/applications/ 2>/dev/null || :

%postun
/usr/bin/gtk-update-icon-cache -f -t %{_datadir}/icons/hicolor/ 2>/dev/null || :
/usr/bin/update-desktop-database %{_datadir}/applications/ 2>/dev/null || :

%files
%{_bindir}/g-drive-xp
%{_libdir}/nautilus/extensions-4/libgdrivexp-nautilus.so
%{_datadir}/applications/org.gnome.FedoraDrive.desktop
%{_datadir}/icons/hicolor/256x256/apps/org.gnome.FedoraDrive.png
%{_datadir}/icons/hicolor/scalable/emblems/emblem-gdrivexp-synced.svg
%{_datadir}/icons/hicolor/scalable/emblems/emblem-gdrivexp-cloud.svg
%{_datadir}/icons/hicolor/scalable/emblems/emblem-gdrivexp-local.svg
%{_datadir}/icons/hicolor/scalable/emblems/emblem-gdrivexp-error.svg

%changelog
* Sat Mar 22 2026 Nothinc Dev <dev@nothinc.org> - 1.0.0-1
- Release inicial v1.0.0
