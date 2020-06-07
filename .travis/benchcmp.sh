if [ "${TRAVIS_PULL_REQUEST_BRANCH:-$TRAVIS_BRANCH}" != "master" ]; then
    REMOTE_URL="$(git config --get remote.origin.url)";
    cd ${TRAVIS_BUILD_DIR}/.. && \
    git clone ${REMOTE_URL} "${TRAVIS_REPO_SLUG}-bench" && \
    cd "${TRAVIS_REPO_SLUG}-bench" && \

    # Benchmark master
    git checkout master && \
    go test -run=XXX -bench=. ./... > master.txt && \

    # Benchmark feature branch
    git checkout ${TRAVIS_COMMIT} && \
    go test -run=XXX -bench=. ./... > feature.txt && \

    # compare two benchmarks
    go get -u golang.org/x/tools/cmd/benchcmp && \
    benchcmp master.txt feature.txt;
fi
