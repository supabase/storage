# Security Policy

Thank you for helping keep Supabase Storage and its users safe.

## Reporting a vulnerability

Please report suspected vulnerabilities through our HackerOne program:
<https://hackerone.com/supabase>. Please do not open public GitHub issues or
pull requests for security problems.

This repository contains the Supabase Storage server, the service behind
Supabase Storage: buckets and objects, the REST and S3-compatible APIs,
resumable (TUS) uploads, signed and presigned URLs, image transformations, and
the access rules that decide who can read or write what. It runs on the hosted
Supabase platform and is also run by people who self-host it. We accept reports
for both, and we triage them differently (see "Hosted vs. self-hosted" below).

## What we consider a vulnerability

In general, we want to hear about anything that lets an attacker break the
security guarantees Storage makes to a project and its users: reading, writing,
overwriting or deleting objects they are not allowed to, escaping from Storage
into the host's file system, or crossing a boundary between buckets, projects
or tenants. The attacker should not need the service-role key or other
privileged credentials, and the problem should be present on the hosted
platform or in a default or documented configuration.

Examples of what this includes (these are illustrations, not a complete list):
- **Access that ignores bucket and object rules.** Reading or writing an object
  the caller's role, a bucket policy or RLS should have denied, including
  through the S3-compatible protocol, resumable uploads, copy and move
  operations, or any other route that skips the check the main route enforces.
- **Signed and presigned URL abuse.** A signature, policy or token that is
  accepted beyond what it was issued for: an upload policy that is not compared
  with the actual bucket, key or expiry of the request, a download token
  accepted by an upload endpoint, or a one-time upload token that can be reused.
- **Path traversal.** Object keys, bucket names, upload IDs or other
  caller-supplied path segments that let a user create, read, overwrite or
  delete files outside the storage root of a file-backed deployment. This
  includes sibling code paths that miss a check applied elsewhere, and bypasses
  of an existing guard.
- **Content served in a way that runs in a first-party origin.** A way to get
  Storage to run an uploaded object as active content, for example by getting
  HTML or SVG served inline, to drop or weaken the protections it applies to
  user content, or to serve user content from an origin that holds credentials.
- **Cross-tenant access.** One project's objects, buckets, keys or cache
  entries being reachable from another project.
- **Exposure of Storage's own administration.** An admin or internal API that
  can be reached without authentication, or that lets a lower-privileged
  caller act with Storage's own privileges, on a default deployment.
- **Injection into queries or commands.** Caller-supplied values that reach SQL
  or a shell without being parameterized or escaped.
- **Secrets exposed to someone who should not see them.** Keys, tokens or
  signing material in API responses, URLs, logs or error messages.

## What we do not consider a vulnerability

Reports that rely on the behavior below will normally be closed as
informative or not applicable.

**By design**
- **Storage does not sanitize your files.** Storage works like an S3-style
  service: it stores the file it was given and does not modify it. An SVG or
  HTML file that contains script is stored as is, and the presence of script in
  an uploaded file is not by itself a vulnerability. On the hosted platform,
  file types that browsers can run as active content are served defensively.
  For example, HTML is served as plain text, and SVG is served as a download
  with a restrictive `Content-Security-Policy`, with `X-Content-Type-Options:
  nosniff`. Ordinary files such as images are served as they are. To be
  accepted, a report has to show script execution in a first-party origin, or
  access to credentials, because of something Storage did. For example, a way
  to make Storage serve an active content type inline, or without these
  protections, is in scope. Sanitizing content is the application developer's
  responsibility. Each bucket can also restrict the MIME types it accepts and
  set a file size limit, for example to refuse `image/svg+xml` in a bucket that
  only needs photos.
- **Public buckets are readable by anyone.** That is what "public" means, and
  RLS policies do not apply to reading them.
- **Analytics (Iceberg) buckets do not use Storage RLS.** They are built for
  analytics workloads, have no per-user permission model, and are accessed with
  privileged tokens. A report must not depend on already holding the signing
  keys or the service-role key.
- **Customer-owned rules.** Whether your bucket policies and RLS policies are
  correct is your responsibility. A bypass must be shown on a bucket with
  correct policies.

**Prerequisites that make the attack pointless**
- The attacker needs the service-role key, trusted signing keys, or access to
  Storage's internal admin API from inside the deployment's network. Admin API
  ports of self-hosted services are meant for internal networks only and must
  not be exposed publicly.
- The attacker needs to modify the code or build of the service first.

**Impact limited to yourself**
- A bucket name, folder name or other input that breaks the reporter's own
  project or dashboard view. Denial of service against your own project is not a
  security issue.
- Inconsistent validation between two operations, such as creating and renaming
  a folder, with no exploit path.

**Reports that are not evidence of a flaw**
- **A PoC that does not use the product.** Traversal or injection shown only by
  calling extracted code in isolation, on a modified build or with stubbed
  dependencies, is not accepted. Show the issue through a real Storage
  endpoint or library API on an unmodified install.
- **A flaw that has already been fixed.** Check the public commit history and
  releases. Reports of something already fixed on the default branch are not
  eligible.
- **AI-generated reports that assume privileged keys** or never ran against a
  real deployment.

## Out of scope for every Supabase asset

These are excluded program-wide (see also
<https://supabase.com/.well-known/security.txt>). They are repeated here so
you do not need to leave this repository to find them.
- Clickjacking on pages with no sensitive actions.
- Unauthenticated, logout and login CSRF.
- Attacks requiring MITM or physical access to a user's device.
- Attacks requiring social engineering.
- Any activity that could lead to the disruption of our service (DoS).
  This means volumetric attacks, load testing and flooding. A vulnerability in
  our code that lets an unprivileged attacker crash a service or exhaust
  resources for other users or tenants is in scope. Denial of service that only
  affects the attacker's own project, or that needs admin or owner privileges,
  is not.
- Content spoofing and text injection issues without showing an attack vector,
  or without being able to modify HTML/CSS.
- Email spoofing.
- Missing DNSSEC, CAA or CSP headers.
- Lack of Secure or HttpOnly flags on non-sensitive cookies.
- Dead links.
- User enumeration.

## Known and documented behavior

These are described in our public documentation. Please check them before
reporting.
- **Storage access control** is configured by access policies on the objects in
  your bucket. See the Storage access-control documentation.
- **Content you upload is stored unmodified.** Developers who need sanitization
  can add it before the content reaches Storage.
- **Buckets can restrict uploads.** When you create a bucket you can set the
  allowed MIME types and a file size limit. Uploads that do not meet the
  restrictions are rejected. List the specific types you need.

## Hosted vs. self-hosted

We accept self-hosted findings when the shipped defaults or the documented
setup lead to the problem, and we treat them as lower severity when the
operator has to misconfigure the deployment first, such as by publishing an
admin API port to the internet, keeping an unchanged signing secret, or putting
a service-role key in a client. Label your report as described in the next
section.

## How to write a report that gets triaged quickly

- **Say what you tested, at the top of the report and in the title.** Use one of
  these labels, for example `[Production] ...` in the title and a "Target" line
  at the start of the description:
  - **Production**: the hosted Supabase platform, against a project you own.
    Include the project ref. Never test another customer's project.
  - **Self-hosted, default configuration**: the code in this repository run
    exactly as shipped or as our self-hosting guide describes, with no changed
    settings other than the secrets the guide tells you to set. Include the
    version or commit and how you started it, and whether you used the Docker
    images or a build from source.
  - **Self-hosted, custom configuration**: you changed settings, added a proxy,
    or modified code. List every setting you changed and why. We triage these
    lower unless the setting is one we recommend or document.

  Reports that do not say which of the three they are about are slower to
  triage, and are often closed with a request to retest. A finding that works
  only on a custom configuration is not a finding against production.

- **Test a real deployment.** Code reading alone is rarely enough. Static
  analysis findings need a working proof of concept against one of the targets
  above, through the actual API or endpoint, on an unmodified install.
- **Give a short, runnable PoC.** Exact requests, a script or a screen
  recording, and the output showing the impact. For path issues, show the file
  that was created, read or removed, and where. Confirm that the attacker really
  got something they should not have.
- **State the attacker's starting position.** What do they hold (an anonymous
  request, a user JWT for one bucket, a signed URL for one object, the
  service-role key)? Reports that quietly assume privileged keys are our most
  common reason to close.
- **Name the boundary you crossed.** Which bucket, project or tenant could not
  be reached before? If nothing was crossed, it is probably a hardening
  suggestion.
- **Say what it cannot do.** Honest limits, for example "this cannot overwrite
  existing objects", make a report easier to trust and to triage.
- **Check the docs, this file and recent commits first.** If the behavior is
  documented, or fixed on the default branch, explain why it still fails.
- **Include versions.** The Storage version or commit, the storage backend (file
  system or S3), and the relevant environment settings.
- **One issue per report.** Link variants of the same root cause instead of
  filing each variant separately.
- **Do not send unreviewed AI output.** Reports that were not reproduced by a
  human and that ignore the documentation are closed. AI-assisted analysis is
  fine when you have verified the result.
- **Respond to follow-up questions.** Reports that go quiet after we ask for a
  reproduction are closed.

## Severity

We rate severity by realistic impact on a default deployment. Issues that need
an unusual configuration, an existing privilege, or a strong prerequisite are
rated lower even when they are valid, and we may fix them as defense in depth.

## Testing and disclosure

These are our program-wide rules. They also apply to AI agents and automated
tools acting on your behalf.

**Testing**
- Do not run automated scanners against other customers' projects. Scanners can
  run up costs for our users, can disrupt services, and our own security
  systems cannot tell hostile reconnaissance from research. If you want to run
  an automated scanner, notify security@supabase.com first and run it only on
  your own Supabase project.
- Do not attack projects that belong to other customers.
- Do not take advantage of a vulnerability you find, for example by downloading
  more data than necessary to demonstrate it, or by deleting or modifying other
  people's data.

**Disclosure**
- Do not reveal the problem to others until we have researched and fixed it and
  informed affected customers.
- If you want to share your research publicly (a conference, a blog or any
  other public forum), send us a draft for review and approval at least 30 days
  before the publication date. Do not include data about Supabase customer
  projects, customers' data, or information about Supabase employees,
  contractors or partners.

**What we promise**
- We will respond to your report within 5 business days with our evaluation and
  an expected resolution date.
- If you have followed these instructions, we will not take legal action against
  you in regard to the report.
- We will handle your report with strict confidentiality and will not pass on
  your personal details to third parties without your permission.
- We will keep you informed of our progress.
- In public information about the problem, we will name you as the discoverer
  unless you prefer otherwise.
