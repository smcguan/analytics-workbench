Set objShell = CreateObject("WScript.Shell")

' Set working directory to repo root
objShell.CurrentDirectory = "C:\dev\analytics-workbench"

' Start Python server bound to IPv4 explicitly
objShell.Run "python -m http.server 8765 --bind 127.0.0.1", 0, False

' Pause to let server start
WScript.Sleep 800

' Open browser
objShell.Run "http://127.0.0.1:8765/bug-dashboard.html"