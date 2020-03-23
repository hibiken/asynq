if [ "${TRAVIS_PULL_REQUEST_BRANCH:-$TRAVIS_BRANCH}" != "master" ]; then
    REMOTE_URL="$(git config --get remote.origin.url)";
    cd ${TRAVIS_BUILD_DIR}/.. && \
    git clone ${REMOTE_URL} "${TRAVIS_REPO_SLUG}-goreportcard" && \
    cd "${TRAVIS_REPO_SLUG}-goreportcard" && \
    go get -u github.com/gojp/goreportcard/cmd/goreportcard-cli && \
    goreportcard-cli -v
fi
