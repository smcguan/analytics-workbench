\# Analytics Workbench – Development SOP



This document defines how development is performed.



---



\# Branch Policy



main = stable, always buildable  

dev = active development  



Never develop directly on main.



---



\# Daily Workflow



\## Start Session



git checkout dev

git pull





\## Activate environment



..venv\\Scripts\\activate





\## Work + Commit



git add -A

git commit -m "Clear description"

git push





Small commits. No large mystery commits.



---



\# Adding Dependencies



py -m pip install <package>

py -m pip freeze > backend\\requirements.txt

git add backend\\requirements.txt

git commit -m "Deps: add <package>"

git push





Never install dependencies locally without updating requirements.



---



\# Release Procedure



1\. Stabilize dev

2\. Merge dev → main

3\. Tag version

4\. Clean build from fresh venv

5\. Package deliverable outside repo



---



\# What Must Never Be Committed



\- .venv/

\- dist/

\- build/

\- release zips

\- datasets (except demo)

\- logs/

\- exports/



---



\# Pre-Release Checklist



\- App launches

\- Scan works

\- Run works

\- Export works

\- No runtime errors

\- DOCTOR.bat passes

\- Clean build from scratch succeeds



---



Follow this strictly.





