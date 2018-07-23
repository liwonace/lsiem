:_loop
wevtutil qe "C:\Windows\System32\winevt\Logs\Security.evtx" /lf /f:text  > "C:\Users\shhong\Desktop\to_kafka\Security.txt"
wevtutil qe "C:\Windows\System32\winevt\Logs\System.evtx" /lf /f:text  > "C:\Users\shhong\Desktop\to_kafka\System.txt"
wevtutil qe "C:\Windows\System32\winevt\Logs\Application.evtx" /lf /f:text  > "C:\Users\shhong\Desktop\to_kafka\Application.txt"
timeout /t 60
goto _loop
