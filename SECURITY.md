# Security policy

## Reporting a vulnerability

Please do not report a security vulnerability in a public issue, a pull request or a discussion.

Report it privately through GitHub: open this repository's **Security** tab and choose **Report a vulnerability**. GitHub's guide is [Privately reporting a security vulnerability](https://docs.github.com/en/code-security/how-tos/report-and-fix-vulnerabilities/report-privately). The report and the discussion that follows stay private to you and the maintainers until an advisory is published.

If that button is not there, open an issue that asks for a private contact and leave every detail of the vulnerability out of it.

Include what you can of:

- the decree version you tested, or the commit if you run `main`;
- the component: the store (`Decree.Store`), the framework (`Decree`), or a Mix task;
- the steps to reproduce it, and what an attacker gains.

## Supported versions

decree is pre-1.0. Fixes land on `main` and ship in the next patch release of the latest minor version, 0.3.x today. Older versions are not maintained.
