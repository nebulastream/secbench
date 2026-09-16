CXX := g++
CXXFLAGS := -std=c++17 -O3 -pthread
TARGET := rtracker
SRC := rtracker.cpp

.PHONY: all clean run

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(TARGET)

run: $(TARGET)
	./$(TARGET) config.yaml

clean:
	rm -f $(TARGET)
