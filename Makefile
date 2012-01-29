CC         = gcc
CFLAGS    ?= -O2 -pipe -Wall -Wextra -Wno-variadic-macros
CFLAGS    += -std=c99
PKGCONFIG  = pkg-config
STRIP      = strip
INSTALL    = install
UNAME      = uname

OS         = $(shell $(UNAME))
CFLAGS    += $(shell $(PKGCONFIG) --cflags lem)
LUA_PATH   = $(shell $(PKGCONFIG) --variable=path lem)
LUA_CPATH  = $(shell $(PKGCONFIG) --variable=cpath lem)

ifeq ($(OS),Darwin)
SHARED     = -dynamiclib -Wl,-undefined,dynamic_lookup
STRIP_ARGS = -x
else
SHARED     = -shared
endif

clibs = core.so
libs  = sqlite3.lua queued.lua

ifdef NDEBUG
CFLAGS += -DNDEBUG
endif

ifdef V
E=@\#
Q=
else
E=@echo
Q=@
endif

.PHONY: all strip install clean
.PRECIOUS: %.o

all: $(clibs)

core.so: sqlite3.c
	$E '  CCLD $@'
	$Q$(CC) $(CFLAGS) -fPIC -nostartfiles $(SHARED) $< -o $@ $(LDFLAGS) -lpthread -lsqlite3

%-strip: %
	$E '  STRIP $<'
	$Q$(STRIP) $(STRIP_ARGS) $<

strip: $(clibs:%=%-strip)

path-install:
	$E '  INSTALL -d $(LUA_PATH)/lem/sqlite3'
	$Q$(INSTALL) -d $(DESTDIR)$(LUA_PATH)/lem/sqlite3

sqlite3.lua-install: sqlite3.lua path-install
	$E '  INSTALL $<'
	$Q$(INSTALL) -m644 $< $(DESTDIR)$(LUA_PATH)/lem/$<

%.lua-install: %.lua path-install
	$E '  INSTALL $<'
	$Q$(INSTALL) -m644 $< $(DESTDIR)$(LUA_PATH)/lem/sqlite3/$<

cpath-install:
	$E '  INSTALL -d $(LUA_CPATH)/lem/sqlite3'
	$Q$(INSTALL) -d $(DESTDIR)$(LUA_CPATH)/lem/sqlite3

%.so-install: %.so cpath-install
	$E '  INSTALL $<'
	$Q$(INSTALL) $< $(DESTDIR)$(LUA_CPATH)/lem/sqlite3/$<

install: $(clibs:%=%-install) $(libs:%=%-install)

clean:
	rm -f $(clibs) *.o *.c~ *.h~
