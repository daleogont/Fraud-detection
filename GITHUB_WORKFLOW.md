# GitHub Collaboration Guide
## How to Work Together on This Project

---

## 🌳 Branch Strategy

We follow a **Git Flow** branching model to keep the project organized and production-ready.

### Main Branches

```
main (production-ready, always stable)
  ← only merge from release branches or hotfixes
  ← tag with versions (v0.1.0, v0.2.0, etc.)

develop (staging, next release)
  ← merge completed feature/fix branches here
  ← this is your "master" branch for daily work

feature/* (your work branches)
  ├─ feature/kafka-producer (Farzaneh)
  ├─ feature/spark-streaming (Hontar)
  ├─ feature/ml-training (Hontar)
  ├─ feature/airflow-dags (Elif)
  ├─ feature/grafana-dashboards (Elif)
  └─ feature/docker-setup (Khurshid, Elif)

fix/* (bug fixes for develop)
  └─ fix/streaming-latency (example)

hotfix/* (critical production fixes, branch from main)
  └─ hotfix/memory-leak (example)

docs/* (documentation updates)
  └─ docs/readme-update (example)
```

### Branch Naming Convention

```
Type: feature | fix | hotfix | docs | test | chore

Format: <type>/<descriptive-name>

✅ Good:
  feature/kafka-producer-v1
  feature/bronze-layer-schema
  fix/null-value-handling
  docs/system-design
  test/streaming-integration

❌ Bad:
  feature1, fix_something, hotfix, update
  FEATURE/NAME, feature/a
```

---

## 👥 Branch Ownership

| Branch | Owner | Starts | Merges To | Duration |
|--------|-------|--------|-----------|----------|
| feature/kafka-producer | Farzaneh | Week 2 | develop | 2 weeks |
| feature/bronze-layer | Hontar | Week 2 | develop | 1 week |
| feature/silver-layer | Hontar | Week 2 | develop | 1.5 weeks |
| feature/gold-layer | Hontar | Week 2-3 | develop | 1 week |
| feature/ml-training | Hontar | Week 3 | develop | 1.5 weeks |
| feature/mlflow-tracking | Hontar | Week 3 | develop | 1 week |
| feature/docker-setup | Khurshid, Elif | Week 1 | develop | 1 week |
| feature/postgres-init | Elif | Week 1 | develop | 3 days |
| feature/airflow-dags | Elif | Week 4 | develop | 2 weeks |
| feature/grafana-dashboards | Elif | Week 5 | develop | 1.5 weeks |

---

## 📝 Commit Message Convention

Follow **Conventional Commits** for clear, standardized messages:

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types
- **feat**: New feature (feature branch)
- **fix**: Bug fix (fix branch)
- **docs**: Documentation updates
- **style**: Code formatting (no logic change)
- **refactor**: Code restructuring
- **perf**: Performance improvement
- **test**: Adding/updating tests
- **chore**: Dependency updates, build config

### Scope
Specific component being modified: `producer`, `spark`, `ml`, `airflow`, `grafana`, `postgres`, etc.

### Examples

✅ **Good Commit Messages**
```
feat(producer): add CSV replay with configurable TPS

- Load synthetic_fraud_dataset.csv
- Configure transactions per second via env variable
- Add fraud injection logic for 5 patterns
- Add logging for transaction counts every minute
- Closes #12

fix(spark): handle null values in feature engineering

Silver layer was crashing on null merchant_risk_score.
Added fillna logic to handle missing data.

docs(readme): update quick start instructions

Added environment setup steps and troubleshooting section.

feat(ml): integrate XGBoost model into streaming pipeline

- Load fraud_model.pkl from shared volume
- Compute ML score alongside rule score
- Fallback to rule_score only if model not found
- Add inference latency monitoring
- Related to #45
```

❌ **Bad Commit Messages**
```
update
fix bug
working version
ML stuff
kafka
changed things
```

---

## 🔄 Pull Request Process

### Step 1: Create Branch Locally

```bash
# Clone repo (first time only)
git clone https://github.com/khurshidnm/fraud-detection.git
cd fraud-detection

# Make sure you're on latest develop
git checkout develop
git pull origin develop

# Create your feature branch
git checkout -b feature/your-feature-name
```

### Step 2: Make Changes & Commit

```bash
# Make your changes...

# Stage changes
git add .

# Commit with clear message
git commit -m "feat(producer): add CSV replay logic"

# Keep committing (one logical change = one commit)
git commit -m "feat(producer): add fraud injection"
git commit -m "test(producer): add unit tests"
```

### Step 3: Push & Create PR

```bash
# Push branch to GitHub
git push origin feature/your-feature-name

# GitHub will show a "Compare & pull request" button
# OR go to https://github.com/khurshidnm/fraud-detection/pulls
# Click "New pull request"
```

### Step 4: Write PR Description

**PR Template** (auto-filled):

```markdown
## Description
Brief summary of what this PR does.

## Type of Change
- [ ] New feature (non-breaking)
- [ ] Bug fix (non-breaking)
- [ ] Documentation update
- [ ] Breaking change

## Changes Made
- Bullet point 1
- Bullet point 2
- Bullet point 3

## Testing
How did you test this?
- [ ] Unit tests (run `make test`)
- [ ] Integration tests
- [ ] Manual testing

## Related Issues
Closes #123

## Checklist
- [ ] My code follows the style guidelines
- [ ] I have self-reviewed my code
- [ ] I have commented complex logic
- [ ] I have added tests
- [ ] Tests pass locally (`make test`)
- [ ] Documentation updated
```

### Step 5: Request Review

- **Assign Reviewers**: Select 1-2 team members
- **Add Labels**: `feature`, `bug`, `urgent`, `in-progress`, etc.
- **Add Projects**: Select "Fraud Detection" project
- **Link Issues**: If related, use "Closes #123"

### Step 6: Code Review

#### As Reviewer:
1. **Read** the description and understand the change
2. **Review** code for:
   - Logic correctness
   - Error handling
   - Test coverage
   - Documentation
   - Code style consistency
3. **Comment** on issues (click line numbers)
4. **Approve** or "Request Changes"

#### Comment Types:
```
# Specific issue
Line 42: This variable name doesn't match our convention. 
Should be `transaction_count` not `txn_count`.

# Question
Why are we using XGBoost instead of LightGBM here?

# Praise
Great approach! The stateful aggregation handles the windowing perfectly.

# Suggestion
Consider adding a cache here for performance. Benchmark shows 10% improvement.
```

#### As Author:
1. **Read** all comments
2. **Respond** to questions/concerns
3. **Fix** issues locally:
   ```bash
   git add .
   git commit -m "fix(review): address feedback on variable naming"
   git push origin feature/your-feature-name
   ```
4. **Mark as resolved** once fixed
5. **Re-request** review when ready

### Step 7: Merge

✅ **When can we merge?**
- [x] At least 1 approval from reviewer
- [x] All conversations resolved
- [x] CI checks pass (tests, linting)
- [x] No conflicts with `develop` branch

✅ **Merge Options**:
- **Squash and merge**: One commit per PR (preferred for features)
- **Create a merge commit**: Keep all commits (only if very granular)
- **Rebase and merge**: Linear history (use rarely)

**Preferred**: Squash for cleaner git history

```bash
# After approval, click "Squash and merge" on GitHub
# Confirm with message:
feat(producer): add CSV replay with fraud injection

- Load synthetic_fraud_dataset.csv with configurable TPS
- Implement 5 fraud patterns: high amount, velocity, off-hours, geo, merchant
- Add comprehensive logging and error handling
- Closes #12
```

### Step 8: Delete Branch

```bash
# After merging, delete the remote branch
# (GitHub offers this as a button)

# Clean up locally
git checkout develop
git pull origin develop
git branch -d feature/your-feature-name
```

---

## 🚨 Special Cases

### Conflict Resolution

If your branch has conflicts with `develop`:

```bash
# Fetch latest develop
git fetch origin

# Rebase your branch on latest develop
git rebase origin/develop

# Fix conflicts in your editor
# Git will mark conflicts like:
# <<<<<<< HEAD
# your code
# =======
# their code
# >>>>>>>

# After fixing, continue rebase
git add .
git rebase --continue

# Force push to your branch
git push origin feature/your-feature-name -f
```

### Hotfixes (Critical Bugs in Production)

```bash
# Create from main branch
git checkout main
git pull origin main
git checkout -b hotfix/critical-fix

# Make fix...
git commit -m "fix: critical memory leak in fraud scoring"
git push origin hotfix/critical-fix

# Create PR targeting MAIN (not develop!)
# Once merged to main:
git checkout main && git pull
git tag v0.1.1  # Increment patch version
git push origin main --tags

# Backport to develop
git checkout develop
git merge main
git push origin develop
```

### Reverting a Merge

If a merged PR breaks things:

```bash
# Find the PR number (e.g., #123)
# Go to GitHub, view the commit hash
git revert <commit-hash> -m 1
git push origin develop

# Or manually undo the changes:
git reset --hard HEAD~1  # Remove last commit (DANGEROUS!)
git push origin develop -f  # Force push (DANGEROUS!)
```

---

## 🔐 Merge Protection Rules

**Enforceable on GitHub**:

✅ Our configuration:
```
Require status checks to pass:
  ✓ All tests (pytest)
  ✓ Code formatting (black)
  ✓ Linting (pylint)

Require code reviews:
  ✓ 1 approval required
  ✓ Dismiss stale reviews on new commits
  ✓ Require code owner review: YES (for ML components)

Include administrators:
  ✓ YES (even Khurshid must go through PR)

Restrict force pushes:
  ✓ Force push blocked on main, develop
  ✓ Allowed on feature branches (for rebasing)

Auto-merge:
  ✗ Disabled (manual merge only)
```

---

## 📊 GitHub Projects Board

We use GitHub Projects (Kanban) to track progress:

### Board Columns

```
📋 Backlog → 🔄 In Progress → 👀 In Review → ✅ Done
```

### Workflow

1. **Backlog**: New issues/features go here
2. **In Progress**: When you start working (assign to yourself)
3. **In Review**: When PR created (auto-moved)
4. **Done**: When PR merged (auto-moved)

### Task Links

Each task should be an Issue (not just a comment):

```markdown
## Issue Title
feat(producer): implement Kafka producer for transaction replay

## Description
[Details from TEAM_TASK_BREAKDOWN.md]

## Acceptance Criteria
- [ ] Producer sends 10-100 TPS
- [ ] Messages valid JSON
- [ ] Error handling works
- [ ] Logging implemented

## Assignee
Farzaneh

## Labels
feature, producer, week-2

## Related
PR #45
```

---

## 🧪 Pre-Commit Checklist

Before pushing, verify:

```bash
# 1. Run tests
make test

# 2. Check linting
pylint producer/*.py
# or: black --check .

# 3. Update documentation
# Edit component README

# 4. Verify no secrets
grep -r "password\|token\|key" . --exclude-dir=.git

# 5. Check for large files
find . -size +10M  # Should be empty

# 6. Verify .gitignore
git status  # Should not show temp files

# 7. Run locally
make up
# Test your changes manually

# 8. Create meaningful commit
git log --oneline -5  # Review your commits
```

---

## 📞 Getting Help

**If stuck**:
1. Search GitHub Issues for similar problem
2. Post in #fraud-detection-project Slack channel
3. Request help from relevant team member
4. Escalate to Khurshid if blocking

**Code Review Feedback**:
- React with ✅ to approve
- Comment with suggestions
- React with ❓ to ask questions
- Be kind and constructive

---

## 🎯 Success Metrics

Track team progress:

```bash
# View PR statistics
git log --oneline develop --graph  # See merge history

# Count commits per person
git shortlog -sn develop

# See who worked on what
git blame <filename>
```

---

## 📚 Additional Resources

- [GitHub Flow Guide](https://guides.github.com/introduction/flow/)
- [Conventional Commits](https://www.conventionalcommits.org/)
- [Git Workflows](https://www.atlassian.com/git/tutorials/comparing-workflows)
- [Pro Git Book](https://git-scm.com/book/en/v2)

---

**Remember**: 
- **Communicate early** - Ask questions before spending 10 hours on wrong approach
- **Small PRs are better** - Easier to review, faster to merge
- **Review promptly** - Don't let team members blocked waiting for review
- **Help each other** - This is a team project, not individual work

**Questions?** Ask Khurshid! 🚀

---

**Last Updated**: May 13, 2026
