\# Analytics Workbench – Operational Playbook (Phase 2)



\## Prime Directive

Source code is truth.  

Build artifacts are disposable.  

Datasets are not source code.



---



\# Branch Model



\## main

\- Always stable

\- Must build clean

\- Represents shippable state



\## dev

\- All development happens here first

\- Can be unstable

\- Must build before merging to main



---



\# Daily Workflow (Either Machine)



\## Start Session



git checkout dev

git pull





\## Activate Environment



..venv\\Scripts\\activate





\## Commit Discipline

Small commits. Clear messages.





git add -A

git commit -m "Clear description of change"

git push





---



\# Dependency Discipline (Non-Negotiable)



If you install a package:





py -m pip install <package>

py -m pip freeze > backend\\requirements.txt

git add backend\\requirements.txt

git commit -m "Deps: add <package>"

git push





Never install a dependency on only one machine.



---



\# Dataset Discipline



Only allowed in repo:





data/datasets/demo/





Everything else:

\- Local only

\- Ignored by .gitignore

\- Never committed



Sanity check:





git ls-files | findstr /i ".parquet .zip .exe"





---



\# Release Procedure



\## 1) Stabilize dev

Verify:

\- App launches

\- Scan/register works

\- Export works

\- No runtime errors



\## 2) Merge dev → main



git checkout main

git pull

git merge dev

git push





\## 3) Tag

Semantic versioning:

\- PATCH = packaging / small fix

\- MINOR = new feature

\- MAJOR = structural change





git tag -a v1.0.1 -m "Release description"

git push origin v1.0.1





\## 4) Clean Build



rmdir /s /q .venv

py -m venv .venv

..venv\\Scripts\\activate

py -m pip install -r backend\\requirements.txt

BUILD\_RELEASE.bat





Deliverable:



dist\\AnalyticsWorkbench\\





---



\# Repo Health Check



Before shipping:





git status

git branch -vv

git ls-files | findstr /i ".parquet .zip .exe .db"





If large artifacts appear, stop and fix .gitignore.



---



\# Definition of Done (Phase 2)



✔ Desktop builds clean  

✔ Laptop builds clean  

✔ main always builds  

✔ dev is working branch  

✔ No datasets in repo (except demo)  

✔ Tags represent real release states  

✔ START\_HERE works in both repo and dist contexts  



---



Operate like a product team, not a script folder.



Save it.

