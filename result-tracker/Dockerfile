FROM gcc:13 AS build

WORKDIR /app
COPY rtracker.cpp .
RUN g++ -std=c++17 -O3 -pthread rtracker.cpp -o rtracker

FROM gcc:13

WORKDIR /app
COPY --from=build /app/rtracker /app/rtracker
COPY config.yaml /app/config.yaml

EXPOSE 9000

ENTRYPOINT ["./rtracker"]
CMD ["config.yaml"]
