# Working notes

Context that does not belong in the design or the rules but would otherwise be lost between sessions. Not a source of truth for the design: `docs/design.md` and the `CLAUDE.md` decision log win.

## Why this product
The shortlist for "search (Elasticsearch) + analytics streaming + Kafka" was:
1. **Observability platform** (chosen): logs/events, windowed error rates and percentiles, alerts, full-text search. Deepest backend story (high-volume ingest, windows, retention, alerting) and a natural load generator.
2. E-commerce search with live ranking: Postgres -> CDC -> Kafka -> ES, click signals feed ranking. Overlaps `change-data-capture-pipeline`.
3. Real-time trend detector over a public firehose (Wikipedia edits, HN, GitHub events): best live demo, free real data.
4. Fraud/risk monitor: stateful stream rules, analyst UI.
5. Clickstream product analytics (mini Amplitude): sessionization, funnels, retention.
6. LLM-era search over a live corpus: embeddings, hybrid search. Overlaps `llm-request-gateway` and the document pipeline.

Runner-up if the observability platform ever feels too heavy: #3 (trend detector), which reuses the same Kafka + window + ES machinery.

## Process we settled on (and why)
- **Design before code.** Planning sessions should be thorough, not short. The user rejected two short plans in a row and asked for explicit code-quality, coverage, and review rules.
- **Fresh-context review before every commit**, against `CODE_REVIEW.md`, with all findings fixed (disagreements flagged to the user, never skipped silently). The design commit took 12 passes; findings fell from 3 blockers/7 majors to none. The reviews caught real bugs (tenant-unscoped dedup IDs, the closed-window version conflict, output lost across a checkpoint, cap-induced watermark deadlock, unbounded memory claims). Expect code reviews to be worth it too.
- **Reviewer prompts that worked:** state the repo state, tell the reviewer to read `CODE_REVIEW.md` first and report only (no edits), list the specific changes since the last pass, ask for concrete failure scenarios rather than vague worries, cap the report length, and require an explicit "nothing remains" statement listing what was verified. Run reviews as a subagent for fresh context.
- **Verify edits landed.** Mid-session, a patch script silently dropped edits to `CLAUDE.md` (it read the file again and overwrote its own in-memory changes). A later reviewer noticed stale text and the grep confirmed it. When scripting multi-file edits: assert every replacement matches exactly once, call save for every file, and grep afterwards.
- **Docs-only commits are exempt from `pnpm verify`** only until the enforcement commit (Phase 1 step b), as in rule 1. They still get a review. The commit that introduces `pnpm verify` (step a) is not docs-only and must itself pass it, including with no tests yet.

## Deployment, hosting, and access (discussed, intentionally out of scope for now)
Phase 6 (a live demo) is deferred until after Phase 5, per the cross-project rule to prefer `docker compose` plus load scripts over cloud infrastructure. If it comes back:

### What Claude needs to build, push, merge, and deploy
- **GitHub:** `gh` CLI authenticated (`! gh auth login` in the session). Git identity is already configured on this machine. If using a token, make it fine-grained: Contents, Pull requests, Workflows, scoped to specific repos. Protect `main` so changes arrive via PR; merging is outward-facing, so confirm before merging unless told otherwise.
- **AWS:** IAM Identity Center (SSO) profile, `! aws sso login --profile <name>`; never the root account; a dedicated sandbox account or a tightly scoped role; a budget alert before creating anything (NAT gateways and RDS are the classic surprise bills); a named region and profile.
- **IaC:** Terraform, CDK, or SAM so infrastructure is reviewable in a PR. Plan first, apply only after review.
- **CI/CD:** GitHub Actions deploying to AWS via an OIDC-assumed role, with no long-lived AWS keys stored as secrets. The role's trust policy must be restricted by the `sub` claim to the specific repo and branch, otherwise any repo can assume it. Claude can write the workflow and the trust policy; the one-time role creation is either done by the user or needs extra permissions.
- **Claude Code permissions:** allowlist routine commands (`git`, `gh pr`, `terraform plan`); leave `terraform apply`, `cdk deploy`, and `gh pr merge` as prompts. The `update-config` skill sets this up.

### UI testing and hosting
- Claude in Chrome extension for visual checks (screenshots, console, network); Playwright for repeatable e2e in CI.
- Static SPA: S3 + CloudFront. SSR: Amplify, or containers on ECS/Fargate. Auth, if users log in: Cognito (or Auth0/Clerk). Secrets: SSM Parameter Store or Secrets Manager.
- Preview deployments per PR are extra cost and complexity; skip at first.

### Domain, DNS, certificates
- Claude can manage Route 53 hosted zones and records, request ACM certificates with DNS validation (CloudFront certs must be in `us-east-1`), and point the domain at CloudFront/ALB, all via IaC with a plan reviewed before apply.
- **The user buys the domain** (cost, mostly non-refundable, name is their choice) or names the exact domain and budget. Do not grant standing `route53domains:RegisterDomain`.
- If the registrar is not Route 53, the user pastes the four Route 53 nameservers at the registrar. That is also where DNSSEC DS records get added.
- DNSSEC is optional for a portfolio project and risky (a wrong DS record or deleted KMS key takes the domain offline for validating resolvers). If wanted: enable it last, KMS key in `us-east-1` (ECC_NIST_P256), verify with `dig +dnssec`.
- Before touching an existing domain, read its current records; a bad MX/TXT change can silently break mail.
- Scoped permissions: `route53:ChangeResourceRecordSets`/`ListHostedZones`/`GetChange`/`ListResourceRecordSets` on the specific zone, `acm:Request/Describe/List/DeleteCertificate`, plus the CloudFront and S3 permissions.

### Easier hosts (for a quick live demo)
Vercel/Netlify/Cloudflare Pages (frontends), Railway/Render/Fly.io (backends, workers, Postgres/Redis; long-running processes fit these better than serverless), Supabase/Firebase (BaaS), Modal/Replicate (AI inference), App Runner/Amplify/Lightsail (managed AWS). Same Dockerfiles work everywhere, so start on the easiest and move later.
**Recommendation:** Docker Compose locally for the real architecture; Railway or Fly.io for a fast demo URL; AWS only where infrastructure is part of the interview story.

## Machine facts at project start (2026-10-20)
- macOS (Darwin 25.6), zsh. Node v25.9.0 (non-LTS) and **no pnpm**. Docker 29.7.2 present. Git identity configured.
- Repo had no commits before `babb40f` (the design commit); branch `main`; no remote yet.
