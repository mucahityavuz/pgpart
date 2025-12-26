EXTENSION = pgpart
DATA = sql/pgpart--1.0.sql
PGFILEDESC = "pgpart extension"

PG_CONFIG = /usr/lib/postgresql/17/bin/pg_config
MODULES =

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

dist:
	tar czvf $(EXTENSION)-1.0.tar.gz \
		pgpart.control \
		Makefile \
		sql/
