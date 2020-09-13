if [ "${TRAVIS_PULL_REQUEST_BRANCH:-$TRAVIS_BRANCH}" != "master" ]; then
    REMOTE_URL="$(git config --get remote.origin.url)";
    cd ${TRAVIS_BUILD_DIR}/.. && \
    git clone ${REMOTE_URL} "${TRAVIS_REPO_SLUG}-bench" && \
    # turn the detached message off
    git config --global advice.detachedHead false && \
    cd "${TRAVIS_REPO_SLUG}-bench" && \

    # Benchmark master
    git checkout master && \
    go test -run=^$ -bench=. -count=5 -timeout=60m -benchmem ./... > master.txt && \

    # Benchmark feature branch
    git checkout ${TRAVIS_COMMIT} && \
    go test -run=^$ -bench=. -count=5 -timeout=60m -benchmem ./... > feature.txt && \

    # compare two benchmarks
    go get -u golang.org/x/perf/cmd/benchstat && \
    benchstat master.txt feature.txt;
fi
