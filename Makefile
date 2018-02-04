DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	public_key mnesia syntax_tools compiler
PULSE_TESTS = worker_pool_pulse

.PHONY: deps test

all: compile

compile: deps
	rebar3 compile

clean:
	rebar3 clean

distclean: clean

include tools.mk
