# About this library

This library provides some useful building blocks for making your data pipeline (i.e. ETL)
more maintainable, testable and documented. It's highly recommended to checkout [this presentation](https://docs.google.com/presentation/d/1vdIrRx6JeW9KGb7XHav1U3x2nJlLjcvTRBkF-cVJ7ZE/edit?usp=sharing)
for better understanding and motivation behind it.

# Building

This project requires Java SDK.

```sh
./gradlew build
```

# Releasing new version

1. Change version in `VERSION` file
2. Run `./release.sh`
3. Run `git add VERSION releases`
4. Commit files

# TODO

There's plenty of things to do:

- Integrate more processing engines (currently there's only support for Apache Spark)
- Add more Cucumber steps
  - Mocking/checking data in Apache Kafka
  - Mocking time
  - etc
- Optimize `TestHive` Spark Context for faster feedback loop on local machine
