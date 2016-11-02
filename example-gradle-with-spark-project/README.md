This is an example project with Apache Spark using Gradle as a build tool.

This project allows you to get a sense of how it looks and feels when you're using `cucumber-specs-for-spark` library.

# Playing around

## Executable specifications

You can find executable specifications in `src/cucumberTest/resources/features` directory. At the moment
there's one successful spec (`upper_function.feature`) and one failing spec (`failing_upper_function.feature`).

## Running tests

To execute aforementioned specifications, simply run this command:

```sh
./gradlew test
```

It will download all the necessary dependencies, the only thing you need is Java SDK on your machine.

## Development cycle

You can also run the tests in development-friendly mode with continuous build on:

```sh
./gradlew test -t
```

or if you want faster feedback loop and run only those specifications that have `@dev` marker as a first line in the file,
it's highly recommended to just use this handy script:

```sh
./autotest.sh
```

This way you can simply edit your files and see test results as soon as the specification or implementation files are saved.

# Future

Currently this project is quite small and poor (spec-wise), but it should give you an idea how you could make
your data pipelines more maintainable, testable and documented.
