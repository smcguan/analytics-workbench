from PyInstaller.utils.hooks import collect_data_files
from PyInstaller.utils.hooks import collect_dynamic_libs
from PyInstaller.utils.hooks import collect_submodules

# -----------------------------------------------------------------------------
# File: AnalyticsWorkbench.spec
#
# Purpose
# -------
# PyInstaller build specification for the Analytics Workbench desktop launcher.
#
# Responsibilities
# ----------------
# - package the FastAPI backend runner into a Windows executable
# - include the backend application package under the bundled app path
# - collect dynamic libraries required by DuckDB and PyArrow
# - force inclusion of multipart/upload and Excel import dependencies that
#   PyInstaller may miss through static analysis alone
#
# Important Notes
# ---------------
# - The dataset import pipeline now depends on:
#     * pyarrow            (Parquet read/write)
#     * python-multipart   (FastAPI UploadFile / Form handling)
#     * openpyxl           (Excel .xlsx import)
# - DuckDB and PyArrow both use native binaries, so we collect dynamic libs
#   explicitly to reduce runtime packaging failures.
# -----------------------------------------------------------------------------

datas = [
    ('backend\\app', 'app'),
]

import sys, pathlib
_py_base = pathlib.Path(sys.base_prefix)

binaries = [
    # PyInstaller sometimes fails to bundle the core Python DLLs from the
    # base interpreter into a venv-based build.  Force-include them.
    (str(_py_base / 'python313.dll'), '.'),
    (str(_py_base / 'python3.dll'),   '.'),
]

hiddenimports = [
    # Application entry/import roots
    'app.main',
    'duckdb',

    # Dataset import pipeline dependencies
    'pyarrow',
    'pyarrow.parquet',
    'multipart',
    'python_multipart',
    'openpyxl',
]

# Collect package data and native binaries needed at runtime.
datas += collect_data_files('duckdb')
datas += collect_data_files('pyarrow')
datas += collect_data_files('openpyxl')

binaries += collect_dynamic_libs('duckdb')
binaries += collect_dynamic_libs('pyarrow')

# Collect submodules for packages that rely on dynamic imports.
hiddenimports += collect_submodules('app')
hiddenimports += collect_submodules('duckdb')
hiddenimports += collect_submodules('pyarrow')
hiddenimports += collect_submodules('openpyxl')


a = Analysis(
    ['backend\\run_workbench.py'],
    pathex=['backend'],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='AnalyticsWorkbench',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[
        'python3.dll',
        'python313.dll',
        'vcruntime140.dll',
        'vcruntime140_1.dll',
        '_asyncio.pyd',
        '_decimal.pyd',
        '_elementtree.pyd',
        '_hashlib.pyd',
        '_multiprocessing.pyd',
        '_overlapped.pyd',
        '_queue.pyd',
        '_socket.pyd',
        '_ssl.pyd',
        'unicodedata.pyd',
    ],
    name='AnalyticsWorkbench',
)
