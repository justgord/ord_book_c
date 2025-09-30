
test: chunks
	./chunks

chunks: chunks.c
	gcc -O3 chunks.c -lm  -o chunks

chunks_clang: chunks.c
	clang -O3 chunks.c -lm -o chunks_clang

clean:
	rm -f chunks chunks_clang

