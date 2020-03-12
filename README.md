**This crate is currently under active development and is not ready for use yet.**

# jac 

[![Released API docs](https://docs.rs/jac/badge.svg)](https://docs.rs/jac)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


> **j**ust **a**nother **c**ache.

**jac** is a distributed key-value store backed by Redis. It has a few really nice properties.

* Versioned values in redis
* Default invalidation strategies to optimise network IO (*Latest* invalidation strategy will always get latest)
* Compositional invalidation strategies (For example implementing a *TTL* invalidation strategy over the *Latest* invalidation strategy.)

The main aim of **jac** can be summed up in 2 points:

* To have a reliable KV store which will not allow concurrent modification of a value. Each version is well defined with a linear history.
* To be blazingly fast while always giving you the most recent value (unless otherwise specified by your invalidation strategy)

![ferris](./resources/ferris-jac.png)

## Origin

At [OpenQuery](openquery.io) we are building a distributed data lake driver. And that required us to maintain a global state where we can quickly and correctly put and retrieve arbitrary pieces of data.

Hence **jac** was born. It was intended to behave as close to a Mutex as possible. However due to the fact that hitting underlying store requires making a network call, this was not possible.


## Example

// todo

## License

**jac** is licensed under the Apache License, Version 2.0. See [LICENSE](./LICENSE) for the full license text.

## Contribution

See [Contributing](./CONTRIBUTING.md).


