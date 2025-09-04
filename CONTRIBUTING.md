# Contributing

Thank you for considering making contributions to ibc-explorer-backend!

Contributing to this repo can mean many things such as participating in
discussion or proposing code changes. To ensure a smooth workflow for all
contributors, the general procedure for contributing has been established:

1. either [open](https://github.com/irisnet/ibc-explorer-backend/issues/new) or [find](https://github.com/irisnet/ibc-explorer-backend/issues)  an issue you'd like to help with in the project's issue tracker,
2. participate in thoughtful discussion on that issue,
3. if you would then like to contribute code:
   1. if the issue is a proposal, ensure that the proposal has been accepted,
   2. ensure that nobody else has already begun working on this issue, if they have
      make sure to contact them to collaborate,
   3. if nobody has been assigned the issue and you would like to work on it
      make a comment on the issue to inform the community of your intentions
      to begin work,
   4. follow standard github best practices: fork the repo,
      if the issue is a bug fix, branch from the
      tip of `develop`, make some commits, and submit a PR to `develop`; if the issue is a new feature,
      branch from the tip of `feature/XXX`, make some commits, and submit a PR to `feature/XXX`
   5. include `WIP:` in the PR-title and submit your PR early, even if it's
      incomplete, this indicates to the community you're working on something and
      allows them to provide comments early in the development process. When the code
      is complete it can be marked as ready-for-review by replacing `WIP:` with
      `R4R:` in the PR-title.

Note that for very small or blatantly obvious problems (such as typos) it is 
not required to open an issue to submit a PR, but be aware that for more complex
problems/features, if a PR is opened before an adequate design discussion has
taken place in an issue, that PR runs a high likelihood of being rejected. 

Please make sure to use `gofmt` before every commit - the easiest way to do this is have your editor run it for you upon saving a file.

## Pull Requests

To accommodate review process we suggest that PRs are categorically broken up.
Ideally each PR addresses only a single issue. Additionally, as much as possible
code refactoring and cleanup should be submitted as separate PRs. The feature branch `feature/XXX` should be synced with `develop` regularly.

## Dependencies

We use Go modules to manage dependencies. The dependencies are defined in `go.mod` and `go.sum` files.

## Testing

The `Makefile` defines testing targets. For any new feature, appropriate tests must be provided:
- Unit tests for business logic
- Integration tests where necessary
- API tests for new endpoints

We expect tests to use `require` or `assert` rather than `t.Skip` or `t.Fail`,
unless there is a reason to do otherwise.

### PR Targeting

Ensure that you base and target your PR on the correct branch:

- `release/vxx.yy` for a merge into a release candidate
- `master` for a merge of a release
- `develop` in the usual case

All feature additions should be targeted against `feature/XXX`. Bug fixes for an outstanding release candidate
should be targeted against the release candidate branch. Release candidate branches themselves should be the
only pull requests targeted directly against master.

### Development Procedure

- the latest state of development is on `develop`
- `develop` must never fail tests
- no --force onto `develop` (except when reverting a broken commit, which should seldom happen)
- feature branches should be regularly synced with `develop`

### Pull Merge Procedure

- ensure `feature/XXX` is rebased on `develop`
- ensure pull branch is rebased on `feature/XXX`
- run tests to ensure that all tests pass
- merge pull request
- push `feature/XXX` into `develop` regularly

### Release Procedure

- start on `develop`
- prepare changelog/release issue
- bump versions
- push to `release-vX.X.X` to run CI
- merge to master
- merge master back to develop

### Hotfix Procedure

- start on `release-vX.X.X`
- make the required changes
  - these changes should be small and an absolute necessity
  - add a note to CHANGELOG.md
- bump versions
- merge `release-vX.X.X` to master if necessary
- merge `release-vX.X.X` to develop if necessary
