# Security

## Reporting a vulnerability

Report it in private: on GitHub, open the repository's **Security** tab and
choose **Report a vulnerability**. Do not open a public issue for it.

Say which version, kernel (`python -m ringloop` prints both) and backend
you use, and how to reproduce the problem.

## What counts

ringloop runs network servers, so these are vulnerabilities:

- A remote peer can crash the process, corrupt memory, or read memory that
  is not its own.
- A remote peer can make a server leak connections, descriptors or memory
  without limit, or stop it from serving others, with traffic that asyncio's
  own loop handles.
- Data of one connection reaches another connection.

Fixes go into the latest release.
