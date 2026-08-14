import asyncio
from aws_ssm_bridge import SessionManager

async def main():
    manager = await SessionManager.new()
    async with await manager.start_session("i-0123456789abcdef0") as session:
        await session.send(b"uname -a\r")
        async for chunk in session.output():
            print(chunk.decode(errors="replace"), end="")

asyncio.run(main())
