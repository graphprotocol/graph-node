TS_FILES=$(wildcard *.ts)
WASM_FILES=$(patsubst %.ts,%.wasm,$(TS_FILES))

all: $(WASM_FILES)

%.wasm: %.ts
	@asc $< -b $@ --validate

clean:
	rm $(WASM_FILES)
