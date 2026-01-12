# Release Process

MultiCloudJ uses [Release Please](https://github.com/googleapis/release-please) for automated release management.

## How It Works

1. **Development**: Commit changes to feature branches using [Conventional Commits](https://www.conventionalcommits.org/) format:
   - `feat:` - New features
   - `fix:` - Bug fixes
   - `perf:` - Performance improvements
   - `blobstore:` - Blob Store changes
   - `docstore:` - Document Store changes
   - `sts:` - STS changes
   - `pubsub:` - PubSub changes
   - `iam:` - IAM changes
   - `refactor:` - Code refactoring
   - `docs:` - Documentation updates
   - `test:` - Test updates
   - `chore:` - Maintenance tasks

2. **Merge to Main**: When PRs are merged to the `main` branch, Release Please automatically:
   - Analyzes commit messages
   - Creates/updates a release PR with:
     - Updated version in `pom.xml`
     - Generated `CHANGELOG.md` entries
     - Release notes
   - ![#f03c15](https://placehold.co/15x15/f03c15/f03c15.png) Release PRs are of this format: `chore(main): release multicloudj <version>`
   - ![#f03c15](https://placehold.co/15x15/f03c15/f03c15.png) IMPORTANT: the release PR is updated on every merge to main, merge ONLY when ready to release. Example of the release PR https://github.com/salesforce/multicloudj/pull/218

3. **Create Release**: When the release PR is merged:
   - Maintainers get the notification to approve the release on email. Once approved:
   - Release Please create a GitHub release and tag. Example: https://github.com/salesforce/multicloudj/releases/tag/multicloudj-v0.2.22
   - [CHANGELOG](https://github.com/salesforce/multicloudj/blob/main/CHANGELOG.md) is updated.
   - GitHub Actions workflow automatically:
     - Builds the project (`mvn clean verify`)
     - Runs all tests
     - Deploys artifacts to Maven Central using the `release` profile
     - Signs artifacts with GPG
   
## Manual Steps

![#f03c15](https://placehold.co/15x15/f03c15/f03c15.png) No manual steps are required. The entire release process is automated through GitHub Actions.

