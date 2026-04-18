# go-mydumper
Go language development is based on the mydumper-0.20.0-1 package and command line multi-threaded data tools

link: https://github.com/mydumper/mydumper

# Quick Start
``` bash
mydumper -h 127.0.0.1 -P 3306 -u root -p -t 16 -B test -T test.t1 -o /tmp/test
```
``` bash
myloader -h 127.0.0.1 -P 3306 -u root -p -t 16 -B test -d /tmp/test
```

# Build
```bash
make build
```

# Documentation

[MyDumper Help](docs/mydumper_help.md)

[MyLoader Help](docs/myloader_help.md)

