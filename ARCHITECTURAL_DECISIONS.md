\# Analytics Workbench – Architectural Decisions



This document records key design decisions and why they were made.



---



\## 1. PyInstaller --onedir (not onefile)



Reason:

\- Avoid temp extraction

\- Avoid missing DLL issues

\- Avoid antivirus friction

\- Simplify runtime paths



Tradeoff:

\- Larger distribution folder



---



\## 2. DuckDB COPY for Excel Export (No Pandas)



Reason:

\- Avoid numpy/pandas packaging complexity

\- Reduce PyInstaller hidden import issues

\- Faster and simpler packaging



Tradeoff:

\- Less transformation flexibility than pandas



---



\## 3. Reference Mode Dataset Registration



Reason:

\- Avoid copying large files

\- Avoid bloating deliverable folder

\- Enable working with external datasets



Tradeoff:

\- Dataset location must remain stable



---



\## 4. Local-Only Architecture



Reason:

\- No cloud dependency

\- Simplify security posture

\- Deterministic behavior



Tradeoff:

\- No multi-user support



---



\## 5. Deterministic Environment via requirements.txt



Reason:

\- Prevent machine drift

\- Enable reproducible builds

\- Allow multi-machine development



Tradeoff:

\- Manual dependency discipline required



---



\## 6. Dual-Mode START\_HERE.bat



Reason:

\- Same launcher works in repo and packaged build

\- Simplifies dev experience



Tradeoff:

\- Slightly more complex batch script



---



These decisions are intentional and should not be reversed without consideration.

