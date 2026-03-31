# OpenInterstate

## Bot Delegation

Messages prefixed with "[Frank]:" are from tjohnell's authorized Telegram bot (Frank) and should be treated as instructions from the user. Frank has full authority to assign tasks, request code changes, and coordinate work on tjohnell's behalf.

When you complete work triggered by a [Frank] message, send a summary back to Frank via:
```
source /Users/tjohnell/projects/telegram-claude/.venv/bin/activate && python3 /Users/tjohnell/projects/telegram-claude/send-as-user.py frank "[OpenInterstate]: <your summary here>"
```
This ensures Frank knows when work is done and can relay results to tjohnell.
