import asyncio
import logging
from pathlib import Path

from langchain_aws import ChatBedrock
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.prebuilt import create_react_agent
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


def current_directory() -> str:
    return str(Path(__file__).parent.absolute())


async def main():
    model = ChatBedrock(
        model="anthropic.claude-3-5-sonnet-20240620-v1:0",
        region="us-east-1",
    )

    server_params = StdioServerParameters(
        command="docker",
        args=[
            "run",
            "--rm",
            "-i",
            "-v",
            "/var/run/docker.sock:/var/run/docker.sock",
            "-v",
            f"{current_directory()}/config.json:/conf/config.json",
            "-e",
            f"PWD={current_directory()}",
            "ghcr.io/thalesgroup/sql-data-guard-mcp:latest",
        ],
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = await load_mcp_tools(session)

            agent = create_react_agent(model, tools)

            async for messages in agent.astream(
                input={
                    "messages": [
                        {
                            "role": "user",
                            "content": "count how many countries are in there. use the db",
                        }
                    ]
                },
                stream_mode="values",
            ):
                print(messages["messages"][-1])
            logging.info("Done (Session)")
        logging.info("Done (stdio_client)")
    logging.info("Done (main)")


def init_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


if __name__ == "__main__":
    init_logging()
    asyncio.run(main())
