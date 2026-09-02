1) Creation of TCP server

server listens on 127.0.0.1:8888 (HOST.PORT)

server receives PREVIEW https://example.com and it sends back ECHO: https://example.com

anything else and it sends back  ERROR: unknown command

Core Concepts : 
- Event Loop (single scheduler in this case): keeps all coroutines that are waiting on something and runs whichever are ready

- Coroutines : functions defined with async def that can pause themselves with await while waiting on something, like reading from a socket

