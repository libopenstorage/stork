# Contributing

* All changes should be submitted through a PR to master.
* Each PR should have an Issue associated with it whether it is a bug or a feature.
* All bugs and features are expected to have Unit Tests and/or Integration tests with the PR.
The exceptions are when it is a very difficult race. In this case the race condition should be clearly documented
in the Issue and the PR.

## Adding New Drivers
* Please file a bug and submit a PR for adding new volume drivers.
* Author(s) are expected to run all the integration tests with their driver and provide the logs of the result with the PR.
* Author(s) will be asked to run the integration tests before each release to make sure there are no issues.
* Driver which aren't maintained regularly or fall behind will be removed after providing warnings to the Author(s).

# Licensing
The specification and code is licensed under the Apache 2.0 license found in the LICENSE file of this repository.
