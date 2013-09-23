CC         = $(CROSS_COMPILE)gcc -std=gnu99
PKG_CONFIG = $(CROSS_COMPILE)pkg-config
STRIP      = $(CROSS_COMPILE)strip
INSTALL    = install
UNAME      = uname

OS         = $(shell $(UNAME))
CFLAGS    ?= -O2 -pipe -Wall -Wextra
CFLAGS    += $(shell $(PKG_CONFIG) --cflags lem)
LIBS       = -lsqlite3
lmoddir    = $(shell $(PKG_CONFIG) --variable=INSTALL_LMOD lem)
cmoddir    = $(shell $(PKG_CONFIG) --variable=INSTALL_CMOD lem)

ifeq ($(OS),Darwin)
SHARED     = -dynamiclib -Wl,-undefined,dynamic_lookup
STRIP     += -x
else
SHARED     = -shared
endif

llibs = lem/sqlite3.lua lem/sqlite3/queued.lua
clibs = lem/sqlite3/core.so

ifdef V
E=@\#
Q=
else
E=@echo
Q=@
endif

.PHONY: all debug strip install clean

all: CFLAGS += -DNDEBUG
all: $(clibs)

debug: $(clibs)

%.so: %.c
	$E '  CCLD  $@'
	$Q$(CC) $(CFLAGS) -fPIC -nostartfiles $(CPPFLAGS) $(SHARED) $^ -o $@ $(LDFLAGS) $(LIBS)

%-strip: %
	$E '  STRIP $<'
	$Q$(STRIP) $<

strip: $(clibs:%=%-strip)

$(DESTDIR)$(lmoddir)/% $(DESTDIR)$(cmoddir)/%: %
	$E '  INSTALL $@'
	$Q$(INSTALL) -d $(dir $@)
	$Q$(INSTALL) -m 644 $< $@

install: \
	$(llibs:%=$(DESTDIR)$(lmoddir)/%) \
	$(clibs:%=$(DESTDIR)$(cmoddir)/%)

clean:
	rm -f $(clibs)
