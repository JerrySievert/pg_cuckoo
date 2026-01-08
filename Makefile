# pg_cuckoo Makefile - PostgreSQL Cuckoo Filter Index Extension

MODULE_big = cuckoo
EXTENSION = cuckoo
DATA = cuckoo--1.0.sql
PGFILEDESC = "cuckoo index access method - cuckoo filter based index"

SRCS = src/ckutils.cpp \
       src/ckinsert.cpp \
       src/ckscan.cpp \
       src/ckvacuum.cpp \
       src/ckvalidate.cpp \
       src/ckcost.cpp

OBJS = $(SRCS:.cpp=.o)

REGRESS = cuckoo

# PostgreSQL configuration
ifdef PG_CONFIG_PATH
PG_CONFIG = $(PG_CONFIG_PATH)
else
PG_CONFIG = pg_config
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
INCLUDEDIR := $(shell $(PG_CONFIG) --includedir)
INCLUDEDIR_SERVER := $(shell $(PG_CONFIG) --includedir-server)

# C++ compiler flags
override PG_CXXFLAGS += -I$(CURDIR)/src -std=c++17 -fPIC
override PG_CFLAGS += -I$(CURDIR)/src

# Linker flags for C++
SHLIB_LINK += -lstdc++

# Platform-specific settings
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    # macOS specific flags if needed
endif
ifeq ($(UNAME_S),Linux)
    override PG_CXXFLAGS += -Wno-register
endif

include $(PGXS)

# Compile C++ sources
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(PG_CXXFLAGS) $(CPPFLAGS) -I$(INCLUDEDIR_SERVER) -c -o $@ $<

# Additional targets
.PHONY: format lint clean-all docs

format:
	clang-format -i src/*.cpp src/*.h

lint:
	clang-tidy $(SRCS) -- -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -I$(CURDIR)/src -std=c++17

clean-all: clean
	rm -f src/*.o

docs:
	doxygen Doxyfile
