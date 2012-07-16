# Maintainer: Emil Renner Berthing <esmil@mailme.dk>

pkgname=lem-sqlite3
pkgver=0.3
pkgrel=1
pkgdesc='SQLite3 library for the Lua Event Machine'
arch=('i686' 'x86_64' 'armv5tel' 'armv7l')
url='https://github.com/esmil/lem-sqlite3'
license=('GPL')
depends=('lem' 'sqlite3')
source=()

build() {
  cd "$startdir"

  make
}

package() {
  cd "$startdir"

  make DESTDIR="$pkgdir/" PREFIX='/usr' install
}

# vim:set ts=2 sw=2 et:
