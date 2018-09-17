CC=g++
CFLAGS=-O2 -std=c++11
TARGET=$(patsubst %.cxx, %, $(wildcard *.cxx))

all: $(TARGET)

$(TARGET): % : %.cxx *.h
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -rf $(TARGET)
