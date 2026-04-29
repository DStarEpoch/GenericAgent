# -*- coding: utf-8 -*-
"""
GenericAgent ↔ SoulAgent (AIMI) Bot Adapter
============================================
Phase 1 单实例打通脚本。

用法：
    cd GenericAgent
    python frontends/aimi_bot_adapter.py --bot-token <你的bot_token>

依赖：
    - 已配置好 mykey.py（GenericAgent 根目录）
    - 已安装 aimi_sdk：pip install -e ../aimi_sdk/aimi_py
"""

import asyncio
import queue
import threading
import sys
import os

# ── 路径处理：确保能 import agentmain 和 mykey ──
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_GENERIC_AGENT_ROOT = os.path.dirname(_SCRIPT_DIR)
if _GENERIC_AGENT_ROOT not in sys.path:
    sys.path.insert(0, _GENERIC_AGENT_ROOT)

from agentmain import GeneraticAgent
from aimi import Client


class GenericAgentAimiBot:
    def __init__(self, bot_token: str, gateway_url: str = None, api_url: str = None):
        self.client = Client(
            bot_token=bot_token,
            gateway_url=gateway_url,
            api_url=api_url,
        )
        self.agent = GeneraticAgent()
        self._agent_thread = None

    def _start_agent_loop(self):
        """GenericAgent.run() 是阻塞无限循环，必须在后台线程跑。"""
        self._agent_thread = threading.Thread(target=self.agent.run, daemon=True)
        self._agent_thread.start()

    async def start(self):
        self._start_agent_loop()

        @self.client.event
        async def on_message(payload):
            await self._handle_message(payload)

        await self.client.start()
        print(f"[GenericAgent Bot] 已连接，agent_id={self.client.agent_id}")

        # 保持运行，直到连接断开
        try:
            while self.client.is_connected():
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self.close()

    async def close(self):
        print("[GenericAgent Bot] 正在关闭...")
        self.agent.abort()  # 中止当前正在运行的任务（如有）
        await self.client.close()

    async def _handle_message(self, payload: dict):
        """
        payload 结构示例：
        {
            "type": "CHAT_MESSAGE_RECEIVED",
            "data": {
                "session_id": "xxx",
                "sender_id": "xxx",
                "content_obj": {
                    "text": "你好",
                    "msg_type": "text",
                    "sender_role": "user"
                }
            }
        }
        """
        data = payload.get("data", {})
        content_obj = data.get("content_obj") or {}

        text = content_obj.get("text", "")
        session_id = data.get("session_id", "")
        sender_role = content_obj.get("sender_role", "")

        if not text:
            return

        # 忽略自己发出去的消息回音（assistant 角色且 sender_id 是自己）
        if sender_role == "assistant" and data.get("sender_id") == self.client.agent_id:
            return

        print(f"[GenericAgent Bot] 收到消息: {text[:120]}...")

        # 把任务扔进 GenericAgent 队列
        display_q = self.agent.put_task(text, source="user")

        # 消费结果并回传
        result = await self._consume_queue(display_q)
        if result:
            await self.client.send_message(session_id=session_id, text=result)

    async def _consume_queue(self, display_q: queue.Queue) -> str:
        """
        GenericAgent 的 put_task 返回一个 queue.Queue，里面会收到：
            {"next": "..."}  —— 增量输出（默认不实时转发，等最后 done）
            {"done": "..."}  —— 最终结果
        """
        chunks = []
        while True:
            try:
                item = await asyncio.to_thread(display_q.get, True, 1.0)
            except queue.Empty:
                # 如果 agent 已经不在运行且队列空了，说明出问题了，直接退出
                if not self.agent.is_running and display_q.empty():
                    break
                continue

            if "next" in item:
                # Phase 1 先不流式转发，等 done 一次性发回。
                # 如需流式，可在这里调用 self.client.send_message(session_id, text=...)
                pass
            elif "done" in item:
                chunks.append(item["done"])
                break

        return "".join(chunks)


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="GenericAgent SoulAgent Bot Adapter")
    parser.add_argument("--bot-token", required=True, help="SoulAgent 分配给该 Agent 的 bot_token")
    parser.add_argument("--gateway", default=None, help="WebSocket 网关地址（默认用 aimi_sdk 内置地址）")
    parser.add_argument("--api", default=None, help="HTTP API 地址（默认用 aimi_sdk 内置地址）")
    args = parser.parse_args()

    bot = GenericAgentAimiBot(
        bot_token=args.bot_token,
        gateway_url=args.gateway,
        api_url=args.api,
    )
    await bot.start()


if __name__ == "__main__":
    asyncio.run(main())
