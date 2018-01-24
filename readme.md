<p align="center">
  <img src="http://www.witheve.com/logo.png" alt="Eve logo" width="10%" />
</p>

---
 
Eve is a programming language based on years of research into building a human-first programming platform. 

**This repository hosts a preview of Eve v0.4 alpha, which is unfortunately no longer under development.**

## Getting Started with Eve v0.4 alpha

### From Source

Start by installing [Node](https://nodejs.org/en/download/) for your platform, and [Rust](https://www.rust-lang.org/en-US/install.html) via the `rustup` tool. Use [`rustup`](https://www.rustup.rs) to install the latest ["nightly" build of Rust](https://github.com/rust-lang-nursery/rustup.rs/blob/master/README.md#working-with-nightly-rust):

```sh
rustup install nightly
rustup default nightly
```

Then clone and build the [Eve repository](https://github.com/kodowa/eve-native):

```sh
git clone git@github.com:Kodowa/eve-native.git
cd eve-native
npm install
```

You can start an Eve server running a given `*.eve` file:

```sh
cargo run --bin server -- examples/test.eve libraries
```

This builds Eve and runs an Eve server at `localhost:8081`, which hosts the program's output. Each connection will create a new instance of the supplied program(s).

**Note:** running `cargo run` with the `--release` flag will execute 10x - 30x faster, but will take longer to build:

```sh
cargo run --release --bin server -- examples/test.eve libraries
```

## Learning Eve

You can learn about Eve with the following resources:

- [Read the Quick Start Tutorial](http://play.witheve.com/) (use Chrome for best results)
- [Syntax Quick Reference](https://witheve.github.io/assets/docs/SyntaxReference.pdf)
- [Language Handbook (draft)](http://docs.witheve.com)

Also, the [mailing list archive](https://groups.google.com/forum/#!forum/eve-talk) is a good resource for help and inspiration. In particular, the [Puzzles & Paradoxes series](https://groups.google.com/forum/#!searchin/eve-talk/Puzzles$20$26$20Paradoxes%7Csort:date) answers a lot of questions beginners face about the Eve langauge.

## Get Involved

### Join the Community

The Eve community is small but constantly growing, and everyone is welcome!

- Join or start a discussion on our [mailing list](https://groups.google.com/forum/#!forum/eve-talk).
- Impact the future of Eve by getting involved with our [Request for Comments](https://github.com/witheve/rfcs) process.
- Read our [development blog](http://incidentalcomplexity.com/).
- Follow us on [Twitter](https://twitter.com/with_eve).

### How to Contribute

The best way to contribute right now is to write Eve code and report your experiences. [Let us know](https://groups.google.com/forum/#!forum/eve-talk) what kind of programs you’re trying to write, what barriers you are facing in writing code (both mental and technological), and any errors you encounter along the way.

### How to File an Issue

Please file any issues in this repository. Before you file an issue, please take a look to see if the issue already exists. When you file an issue, please include:

1. The steps needed to reproduce the bug
2. Your operating system and browser.
3. If applicable, the `.*eve` file that causes the bug.

## License

(c) 2017 Kodowa Inc, All Rights Reserved

## Disclaimer

Eve is currently at a very early, "alpha" stage of development. This means the language, tools, and docs are largely incomplete, but undergoing rapid and continuous development. If you encounter errors while using Eve, don't worry: it's likely our fault. Please bring the problem to our attention by [filing an issue](https://github.com/witheve/eve#how-to-file-an-issue).

As always, with pre-release software, don’t use this for anything important. We are continuously pushing to this codebase, so you can expect very rapid changes. At this time, we’re not prepared make the commitment that our changes will not break your code, but we’ll do our best to [update you](https://groups.google.com/forum/#!forum/eve-talk) on the biggest changes.
