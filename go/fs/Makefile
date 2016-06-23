PKG := $(shell go list)
BENCHOPT := $(shell echo "-benchmem -run NNN -bench")
.PHONY: list
list:
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$' | xargs

cover:
	../../scripts/cover.sh

bench-200mw:
	go test $(BENCHOPT) File200MW $(PKG)

bench-200dw:
	go test $(BENCHOPT) File200DW $(PKG)
