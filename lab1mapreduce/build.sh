#!/bin/bash
#create time: 2022-03-20 20:31:57

# comment this out to run the tests without the Go race detector.
RACE=-race

if [[ "$OSTYPE" = "darwin"* ]]
then
  if go version | grep 'go1.17.[012345]'
  then
    # -race with plug-ins on x86 MacOS 12 with
    # go1.17 before 1.17.6 sometimes crash.
    RACE=
    echo '*** Turning off -race since it may not work on a Mac'
    echo '    with ' `go version`
  fi
fi

# make sure software is freshly built.
(cd mrapps && go clean)
(cd .. && go clean)
(cd mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin early_exit.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(cd cmd && go build $RACE mrcoordinator.go) || exit 1
(cd cmd && go build $RACE mrworker.go) || exit 1
(cd cmd && go build $RACE mrsequential.go) || exit 1

