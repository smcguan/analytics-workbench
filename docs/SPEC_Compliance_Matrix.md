Analytics Workbench v1.0.0

SPEC Compliance Matrix

For each architectural contract defined in SPEC.md: where it is
implemented, how to verify it is working, and what commonly causes it to
fail.

  ----------------------------------------------------------------------------------------------------
  Contract (SPEC        Implemented In             How to Verify               Common Failure Modes
  Section)                                                                     
  --------------------- -------------------------- --------------------------- -----------------------
  Bootstrap Launcher    backend/run_workbench.py → Launch START_HERE.bat in    Missing .venv → Python
  (run_workbench.py)    main()                     dev mode. App opens in      not found. Wrong
                                                   browser. Check              base_dir resolution →
                                                   logs/boot.log for START     frontend or data path
                                                   entry.                      wrong.

  Directory Creation    run_workbench.py →         Delete exports/ and logs/   Permission denied on
  (exports, logs,       ensure_dirs()              folders, relaunch. Confirm  restricted Windows
  datasets)                                        both are recreated          paths. Symlink edge
                                                   automatically.              cases on non-standard
                                                                               installs.

  Environment Variable  run_workbench.py → main()  Hit /api/version in         App imported before env
  Injection             os.environ.setenv calls    browser. Confirm all four   vars set → paths
  (AW_FRONTEND_DIR      before app import          dir paths display correctly resolve to wrong
  etc.)                                            in the JSON response.       defaults. Order
                                                                               matters.

  Single-Instance       run_workbench.py →         Launch app twice. Second    Port not released fast
  Enforcement           ensure_single_instance()   launch should kill the      enough on slow machines
                                                   first and reuse port 8000,  → bumps to alternate
                                                   or bump to 8001.            port. Packaged-only
                                                                               kill behavior; dev mode
                                                                               just bumps.

  Structured File       run_workbench.py →         After launch, open          Null stdout in
  Logging (app.log,     configure_file_logging() → logs/boot.log and           –noconsole mode
  boot.log)             boot_log()                 logs/app.log. Both should   (mitigated by
                                                   contain timestamped         _ensure_std_streams).
                                                   entries.                    Log dir not created
                                                                               before first write.

  FastAPI Application   backend/app/main.py → app  Open browser to             frontend/index.html
  (main.py)             (FastAPI instance)         http://127.0.0.1:8000/ui/ — missing at resolved
                                                   UI loads. Hit /api/version  path → RuntimeError on
                                                   — returns JSON.             startup. Import order
                                                                               issues.

  Static Frontend Mount main.py → app.mount(‘/ui’, Navigate to                 FRONTEND_DIR resolves
  (/ui/ route)          StaticFiles(…))            http://127.0.0.1:8000/ —    to wrong location in
                                                   should redirect to /ui/.    packaged mode → 404 on
                                                   Page renders fully.         all assets.

  Preset-Only Query     main.py → _sql_for()       Hit /api/presets — returns  Preset ID mismatch
  Model                 app/presets/doge.py →      list of two presets. No     between frontend and
                        PRESETS                    endpoint accepts raw SQL.   backend. The word
                                                                               ‘dataset’ appearing in
                                                                               SQL column names would
                                                                               be corrupted by the
                                                                               token replace.

  DuckDB In-Process     main.py → _connect() →     Run a preset query. Result  DuckDB binary not
  Engine (stateless per api_run(), api_export()    returns. Verify no          bundled by PyInstaller
  request)                                         persistent .db file is      → ImportError on first
                                                   created anywhere.           query. XLSX extension
                                                                               unavailable → export
                                                                               falls back to CSV
                                                                               silently.

  Parquet Registration  main.py →                  Register a dataset in       Reference path becomes
  (reference + copy     register_dataset() →       reference mode. Confirm     invalid if source file
  modes)                dataset_source_path()      _reference.txt created in   moves. Copy mode with
                                                   datasets//. Run a query     large files may be slow
                                                   against it.                 with no progress
                                                                               indicator.

  Export (XLSX / CSV    main.py → api_export()     Run a query, click Export.  XLSX extension
  fallback)             FileResponse download      File downloads in browser.  unavailable → silent
                                                   Check exports/ folder for   fallback to CSV.
                                                   the output file.            exports/ dir missing
                                                                               write permissions.

  Deterministic Build   BUILD_RELEASE.bat (sole    Run BUILD_RELEASE.bat on a  Wrong Python version
  (BUILD_RELEASE.bat)   authoritative script)      clean machine. Confirm      selected by ‘py’
                                                   dist/AnalyticsWorkbench/ is launcher. DuckDB
                                                   produced with exe,          binaries not collected
                                                   frontend/, data/, exports/, → import fails at
                                                   logs/.                      runtime. PyInstaller
                                                                               spec regenerated,
                                                                               losing icon config.

  Shutdown Endpoint     main.py → api_shutdown()   Click Quit button in UI.    Response doesn’t flush
  (/api/shutdown)       os._exit(0) via            Browser tab goes idle.      before exit on very
                        BackgroundTasks            Process no longer appears   fast machines.
                                                   in Task Manager.            window.close() blocked
                                                                               by browser — expected,
                                                                               process still exits.
  ----------------------------------------------------------------------------------------------------

Notes

• All verification steps assume dev mode unless otherwise noted.
Packaged mode verification requires running
dist/AnalyticsWorkbench/AnalyticsWorkbench.exe.

• ‘Hit /api/…’ means open that URL directly in the browser while the app
is running.

• BUILD_RELEASE.bat is the sole authoritative build script.
BUILD_RELEASEsav.bat and build_release_FIXED.bat are deprecated and
should be ignored.

• Open risk: Tkinter folder browse dialog not yet verified in packaged
mode. Fix is known (–collect-all tkinter in PyInstaller call) but will
only be applied if verification fails.
