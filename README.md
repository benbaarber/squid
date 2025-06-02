# Squid

See the [wiki](https://gitlab.com/groups/VivumComputing/scientific/-/wikis/Squid).

## Installation

To use Squid, install the Squid client CLI.

First, if you haven't already, [install Rust](https://www.rust-lang.org/tools/install).

Then, install the CLI by running

```sh
cargo install --git ssh://git@gitlab.com/VivumComputing/scientific/tools/squid.git squid
```

If that fails, ensure you have an SSH key registered with GitLab and that it is loaded into your ssh-agent.
