# 📋 Team Task Planning - Quick Index

## 🎯 Start Here

Welcome to the Fraud Detection System team project! This document helps you navigate all task planning materials.

**Project**: Real-Time Financial Fraud Detection System  
**Duration**: 6 weeks  
**Team**: 4 members  
**Status**: Completed and Validated ✅

---

## 👥 Team Members

| Name | Role | Focus | Document |
|------|------|-------|----------|
| **Khurshid Normurodov** | Initial Project Architect (Phase 1) | Initial architecture & planning | [View Card](TEAM_TASK_SUMMARY.md#-khurshid-normurodov---project-lead--data-architect) |
| **Farzaneh Barzegar** | Project Lead / Data Ingestion Engineer | Project coordination, Kafka pipeline, repository management | [View Card](TEAM_TASK_SUMMARY.md#-farzaneh-barzegar---data-ingestion-engineer) |
| **Hontar Daniil** | Data Processing & ML Engineer | Spark Streaming, ML Models | [View Card](TEAM_TASK_SUMMARY.md#-hontar-daniil---data-processing--ml-engineer) |
| **Elif Sila Okutucu** | Analytics & DevOps Engineer | Airflow, PostgreSQL, Grafana, Monitoring & Validation | [View Card](TEAM_TASK_SUMMARY.md#-elif-sila-okutucu---analytics--devops-engineer--scrum-master) |

---

## 📚 Documentation Map

### 1️⃣ For Quick Overview (Read First)
- **This file** - Task planning index (you are here)
- [TEAM_TASK_SUMMARY.md](TEAM_TASK_SUMMARY.md) - One-page card per team member
  - Your tasks
  - Responsibilities
  - Deliverables
  - Timeline & metrics

### 2️⃣ For Detailed Task Breakdown (Reference During Work)
- [TEAM_TASK_BREAKDOWN.md](TEAM_TASK_BREAKDOWN.md) - Comprehensive task list
  - All task groups (6 phases)
  - Detailed acceptance criteria
  - Dependencies & blockers
  - Gantt chart
  - Completion checklist

### 3️⃣ For GitHub Collaboration (Before Contributing)
- [GITHUB_WORKFLOW.md](GITHUB_WORKFLOW.md) - How to work together
  - Branch strategy
  - Commit conventions
  - PR process
  - Code review guidelines
  - Conflict resolution

### 4️⃣ For Project Context (Background)
- [documentation/01_problem_analysis.md](documentation/01_problem_analysis.md) - Problem statement
- [documentation/02_system_design.md](documentation/02_system_design.md) - Architecture & design
- [documentation/03_project_plan.md](documentation/03_project_plan.md) - Original solo project plan
- [documentation/04_tools_and_technologies.md](documentation/04_tools_and_technologies.md) - Tech stack
- [documentation/05_datasets.md](documentation/05_datasets.md) - Dataset details
- [documentation/evidence/README.md](documentation/evidence/README.md) - Final validation evidence

---

## ✅ Project Completion Checklist

### Before Week 1 Starts
- [x] Everyone clones the GitHub repository
- [x] Everyone sets up Python environment (read SETUP_HELP.md)
- [x] Everyone reads this file and their task card
- [x] Team lead (Khurshid) reviews TEAM_TASK_BREAKDOWN.md
- [x] Everyone reviews GITHUB_WORKFLOW.md
- [x] Schedule first team sync meeting

### During Week 1
- [x] Khurshid: Create GitHub repo with all settings
- [x] Khurshid & Elif: Get docker-compose.yml working
- [x] Elif: Initialize PostgreSQL
- [x] Khurshid & Farzaneh: Load and validate dataset
- [x] **Success Criterion**: `make up` starts all services cleanly

### During Week 2-3
- [x] Farzaneh: Kafka producer sending transactions
- [x] Hontar: Bronze/Silver/Gold layers processing
- [x] **Success Criterion**: Data flowing through all 3 layers

### During Week 4
- [x] Elif: Airflow DAGs scheduling
- [x] Hontar: ML training on schedule
- [x] **Success Criterion**: Daily retraining at 00:00 UTC

### During Week 5
- [x] Elif: Grafana dashboards live
- [x] All: Unit tests written & passing
- [x] **Success Criterion**: Monitoring fully functional

### During Week 6
- [x] All: Documentation complete
- [x] Hontar: Performance baseline established
- [x] **Success Criterion**: Ready for production/portfolio

---

## 📊 Task Distribution at a Glance

### By Phase

| Phase | Week | Focus | Lead | Team |
|-------|------|-------|------|------|
| **Infrastructure & Setup** | 1 | GitHub, Docker, PostgreSQL, Dataset | Khurshid | +Elif, +Farzaneh |
| **Streaming Pipeline** | 2-3 | Kafka, Spark (Bronze/Silver/Gold) | Hontar | +Farzaneh |
| **ML Training** | 3 | XGBoost, MLflow, Model Evaluation | Hontar | - |
| **Orchestration** | 4 | Airflow DAGs, Data Quality Monitoring | Elif | +Hontar |
| **Analytics** | 5 | Grafana Dashboards, Monitoring | Elif | +Hontar |
| **Testing & Docs** | 6 | Tests, Documentation, Finalization | All | - |

### By Person - Total Hours

| Khurshid | Farzaneh | Hontar | Elif |
|----------|----------|--------|------|
| **88 hours** | **56 hours** | **152 hours** | **144 hours** |
| 16-18h/week | 8-12h/week | 25-30h/week | 24-28h/week |

---

## 🎯 Each Team Member's First Task

### Khurshid
**Task**: Create GitHub repository  
**Location**: [TEAM_TASK_BREAKDOWN.md - Task 1.1](TEAM_TASK_BREAKDOWN.md#task-group-11---github-repository-setup-khurshid)  
**Time**: Week 1, 8-12 hours  
**Deliverable**: GitHub repo with branch protection, templates, board

### Farzaneh
**Task**: Validate dataset  
**Location**: [TEAM_TASK_BREAKDOWN.md - Task 1.4](TEAM_TASK_BREAKDOWN.md#task-group-14---synthetic-dataset-preparation-khurshid--farzaneh)  
**Time**: Week 1, 4-8 hours  
**Deliverable**: Validated dataset with quality checks

### Hontar
**Task**: Wait for infrastructure, then build Bronze layer  
**Location**: [TEAM_TASK_BREAKDOWN.md - Task 2.2](TEAM_TASK_BREAKDOWN.md#task-group-22---bronze-layer-hontar)  
**Time**: Week 2, 8 hours  
**Deliverable**: Schema-enforced raw data layer

### Elif
**Task**: Set up Docker & PostgreSQL  
**Location**: [TEAM_TASK_BREAKDOWN.md - Tasks 1.2 & 1.3](TEAM_TASK_BREAKDOWN.md#task-group-12---docker-compose--environment-setup-elif--khurshid)  
**Time**: Week 1, 24 hours  
**Deliverable**: Docker services running + initialized database

---

## 🔄 Critical Dependencies

```
⏱️  WEEK 1: Infrastructure (blocks everything else)
    ├─ GitHub repo created
    ├─ Docker services running
    ├─ PostgreSQL initialized
    └─ Dataset loaded

⏱️  WEEK 2: Data Ingestion & Processing
    ├─ Kafka producer sending data
    └─ Spark layers processing

⏱️  WEEK 3: ML Training
    ├─ XGBoost model trained
    └─ MLflow tracking experiments

⏱️  WEEK 4: Orchestration
    ├─ Airflow daily retraining DAG
    └─ Hourly DQ monitoring DAG

⏱️  WEEK 5: Monitoring
    ├─ Grafana dashboards live
    └─ Alerting configured

⏱️  WEEK 6: Finalization
    ├─ All tests passing
    └─ Documentation complete
```

---

## 📅 Project Coordination Schedule (Historical)

**Every Monday 10:00 UTC**
- Sprint planning
- Blockers & dependencies
- Week goals

**Every Thursday 15:00 UTC**
- Progress update
- Course correction
- Upcoming blockers

**Format**: 30 minutes, all 4 team members

---

## ❓ FAQ

### Q: Where do I find my task list?
A: [TEAM_TASK_SUMMARY.md](TEAM_TASK_SUMMARY.md) has your card with all tasks.

### Q: How do I know what to work on this week?
A: Look at [TEAM_TASK_BREAKDOWN.md](TEAM_TASK_BREAKDOWN.md) and match the phase to the current week.

### Q: I'm blocked waiting for something. What do I do?
A: Raise issue on GitHub or post in #fraud-detection-project Slack. Farzaneh will help unblock.

### Q: When do I create my GitHub branch?
A: After GitHub repo is created (Week 1). See [GITHUB_WORKFLOW.md](GITHUB_WORKFLOW.md#-branch-strategy).

### Q: How do I commit my work?
A: Follow [GITHUB_WORKFLOW.md](GITHUB_WORKFLOW.md#-commit-message-convention) for commit messages.

### Q: Can I start before Week 1 is done?
A: Yes, if infrastructure (Docker, GitHub) is ready. Ask Farzaneh.

### Q: What if my task is done early?
A: Talk to Farzaneh or look at next week's tasks to see if you can start early.

### Q: How do I report a bug?
A: Create a GitHub Issue with label `bug`. Tag relevant person.

### Q: What's the expected code quality?
A: 80%+ test coverage, clear naming, documented functions. See code review guidelines.

---

## 📞 Communication Channels

| Channel | Use Case | Frequency |
|---------|----------|-----------|
| **Weekly Syncs** | Planning, blockers, progress | 2x/week |
| **Slack** (#fraud-detection-project) | Daily updates, quick questions | Daily |
| **GitHub Issues** | Task tracking, bugs, features | As needed |
| **GitHub PRs** | Code review, discussions | As needed |
| **Email** | Formal communication | As needed |

---

## ✅ Definition of Done (Per Task)

Every task is complete when:
- [x] Code written and tested
- [x] All acceptance criteria met
- [x] PR reviewed and approved
- [x] Merged to develop branch
- [x] Documentation updated
- [x] Next person can pick up their dependent task

---

## 🎓 Learning Resources

### For Kafka (Farzaneh)
- [Apache Kafka in 100 Seconds](https://www.youtube.com/watch?v=JalUUaNzYVU)
- [Kafka Official Docs](https://kafka.apache.org/documentation/)

### For Spark Streaming (Hontar)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Overview](https://docs.delta.io/)

### For XGBoost (Hontar)
- [XGBoost Docs](https://xgboost.readthedocs.io/)
- [XGBoost with Imbalanced Data](https://xgboost.readthedocs.io/en/stable/faq.html#why-does-my-imbalanced-dataset-produce-poor-results)

### For Airflow (Elif)
- [Apache Airflow Concepts](https://airflow.apache.org/docs/apache-airflow/stable/concepts/overview.html)
- [Airflow Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)

### For Grafana (Elif)
- [Grafana Getting Started](https://grafana.com/grafana/download)
- [Grafana Dashboards](https://grafana.com/docs/grafana/latest/dashboards/)

---

## 🚨 Troubleshooting

### "Docker services won't start"
→ See [SETUP_HELP.md](SETUP_HELP.md)

### "I can't find my task"
→ Check [TEAM_TASK_BREAKDOWN.md](TEAM_TASK_BREAKDOWN.md) Phase section

### "What's the merge process?"
→ Read [GITHUB_WORKFLOW.md - Pull Request Process](GITHUB_WORKFLOW.md#-pull-request-process)

### "I broke something in develop"
→ Contact Khurshid immediately (might need hotfix)

### "I need help from another team member"
→ Create GitHub Issue and @ mention them

---

## 📈 Success Metrics (Project-Level)

By end of Week 6:
- ✅ All code committed to GitHub
- ✅ All tests passing (80%+ coverage)
- ✅ System processes 100+ TPS
- ✅ ML model PR-AUC ≥ 0.75
- ✅ All dashboards live
- ✅ Documentation complete
- ✅ Team ready to present/deploy

---

## 🎯 Final Outcome

1. ✅ End-to-end fraud detection pipeline implemented
2. ✅ Airflow orchestration validated
3. ✅ PostgreSQL analytics layer validated
4. ✅ Grafana monitoring dashboard deployed
5. ✅ Documentation and evidence completed

---

## 📖 Document Versions

| Document | Purpose | Length | Read Time |
|----------|---------|--------|-----------|
| **TEAM_TASK_INDEX.md** (this file) | Navigation & overview | 2 pages | 5 min |
| **TEAM_TASK_SUMMARY.md** | Individual task cards | 8 pages | 15 min |
| **TEAM_TASK_BREAKDOWN.md** | Detailed tasks & phases | 25+ pages | 45 min |
| **GITHUB_WORKFLOW.md** | Collaboration guide | 12 pages | 20 min |

**Total Reading Time**: ~1.5 hours for full context  
**Recommended**: Read in this order ↑

---

## 📝 Document Maintenance

- Updated: June 2026
- Author: Khurshid Normurodov
- Review Cycle: Project Complete
- Feedback: Add to GitHub Issues with label `documentation`

---

## 🎉 Project Successfully Completed

1. ✅ Tasks clearly defined
2. ✅ Team roles assigned
3. ✅ Timeline established
4. ✅ Dependencies mapped
5. ✅ Collaboration process documented

Fraud Detection System successfully implemented, validated, and documented. 🚀

---

**Quick Links**:
- [My Task Card](TEAM_TASK_SUMMARY.md)
- [Detailed Tasks](TEAM_TASK_BREAKDOWN.md)
- [GitHub Workflow](GITHUB_WORKFLOW.md)
- [GitHub Repository](https://github.com/khurshidnm/fraud-detection)

**Questions?** Ask Khurshid on Slack or in GitHub Issues.
