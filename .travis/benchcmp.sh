if [ "${TRAVIS_PULL_REQUEST_BRANCH:-$TRAVIS_BRANCH}" != "master" ]; then
    REMOTE_URL="$(git config --get remote.origin.url)";
    cd ${TRAVIS_BUILD_DIR}/.. && \
    git clone ${REMOTE_URL} "${TRAVIS_REPO_SLUG}-bench" && \
    cd "${TRAVIS_REPO_SLUG}-bench" && \
    # Benchmark master
    echo "Running benchmark tests on master branch"
    git checkout master && \
    go test -run=XXX -bench=. -loglevel=debug -v ./... > master.txt && \
    # Benchmark feature branch
    echo "Running benchmark tests on feature branch"
    git checkout ${TRAVIS_COMMIT} && \
    go test -run=XXX -bench=. -loglevel=debug -v ./... > feature.txt && \
    go get -u golang.org/x/tools/cmd/benchcmp && \
    # compare two benchmarks
    benchcmp master.txt feature.txt;
fi
