# storm-tutorial - distributed wc

# ABOUT

Here's a funny sort of Hello World for distributed Java programming with Storm. This Storm topology performs a word count over the complete Sherlock Holmes, and displays the number of occurrences of the word `Watson`.

# EXAMPLE

Starting with a lot of text in an input directory:

```
$ tree resources/sherlock-holmes/
resources/sherlock-holmes/
├── agrange.txt
├── b-p_plan.txt
├── bascombe.txt
├── beryl.txt
├── blanced.txt
├── blkpeter.txt
├── bluecar.txt
├── cardbox.txt
├── caseide.txt
├── charles.txt
├── copper.txt
├── creeping.txt
├── crookman.txt
├── danceman.txt
├── devilsf.txt
├── doyle-adventures-380.txt
...
```

We write a Storm topology to divide up the work for counting words.

```
$ cat src/main/java/us/yellosoft/storm/tutorial/WordCountTopology.java
package us.yellosoft.storm.tutorial;

// ...

public class WordCountTopology {
  // ...
...
```

We compile and run the job,

```
$ mvn package
$ storm jar target/storm-tutorial-0.0.0.jar us.yellosoft.storm.tutorial.WordCount
...
```

This example is tested to successfully compile and run in Mac OS X, currently the only OS for which installing Hadoop isn't a painful exercise in *yak shaving*.

# REQUIREMENTS

* [JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html) 6+
* [Maven](http://maven.apache.org/) 3+
* [Storm](https://storm.apache.org/) 0.9.3+
* `make`
* `sh`
* Ensure the `storm` script is in `$PATH` (test with `which storm`).

## Optional

* `tree`
* `grep`
* [Ruby](https://www.ruby-lang.org/) 1.9+
* [Guard](http://guardgem.org/) 1.8.2+

Use `bundle` to install Guard.

## Mac OS X

Mac comes with JDK, `sh`, and `grep` by default. The remainder can be obtained thusly:

1. Install [Xcode](https://developer.apple.com/xcode/) via `App Store.app`.
2. Launch `Xcode.app`, install the Xcode command line tools.
3. Install [Homebrew](http://brew.sh/).
4. Launch `Terminal.app`, run `brew install maven storm`.

Optionally, run `brew install tree`.

## Huh?

Either `git clone https://github.com/mcandre/storm-tutorial.git`, or manually download and extract the [ZIP](https://github.com/mcandre/storm-tutorial/archive/master.zip)ball.

Consider forking [storm-tutorial](https://github.com/mcandre/storm-tutorial) and amending the README to help other Stormers along the way.

# CREDITS

* [Apache Storm Starter](https://github.com/apache/storm/tree/master/examples/storm-starter) - Source code for `WordCountTopology.java`
