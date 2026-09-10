# Contributing to flask-mqtt

We welcome contributions to [flask-mqtt](https://github.com/sabuhish/flask-mqtt)

## Issues

Feel free to submit issues and enhancement requests.

[Flask-MQTT Issues](https://github.com/sabuhish/flask-mqtt/issues)

## Contributing

Please refer to each project's style and contribution guidelines for submitting patches and additions. In general, we follow the "fork-and-pull" Git workflow.

1.  **Fork** the repo on GitHub
2.  **Clone** the project to your own machine
3.  **Commit** changes to your own branch
4.  **Push** your work
5.  Submit a **Pull request** so that we can review your changes

## Before contributing, here is how to install

```sh
git clone https://github.com/sabuhish/flask-mqtt.git
cd flask-mqtt
poetry install
# activate the poetry virtualenv
poetry shell
# to make changes and validate them
pre-commit install
pre-commit install-hooks
pre-commit run --all-files
# to run the test suite
pytest
```

Explore the Flask app **examples** and run them with the `flask` CLI

```sh
flask --app examples.app run --port 8000
```

NOTE: Be sure to merge the latest from "upstream" before making a pull request!

### Code formatting

This project uses `pre-commit` to apply multiple linters to the code changes _before_ it's commited.

You can invoke it anytime by running `pre-commit run --all-files`.

Install the hook with `pre-commit install-hooks` to trigger it when commiting changes.
