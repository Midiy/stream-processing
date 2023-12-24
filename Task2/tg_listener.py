import asyncio
from argparse import ArgumentParser, Namespace

import telethon.events as events
from telethon import TelegramClient
from telethon.tl.types import User, Chat, Channel
from async_timer import Timer

from message_set import MessageSet


_UNKNOWN_USER_NAME = "unknown"

def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--api_id", type=int, required=True)
    parser.add_argument("--api_hash", type=str, required=True)
    parser.add_argument("--chats", type=list[str], nargs="*", default=None)
    parser.add_argument("--update_frequency", type=int, default=60, help="Length of the data accumulation period in seconds.")
    parser.add_argument("--session_name", type=str, default="default_session")
    parser.add_argument("--output_filename_template", 
                        type=str, 
                        default="tg_messages/messages_{start}_{end}.parquet", 
                        help="Filename template for output parquet-files with messages. \
                              Could contain {start} and {end} placeholders for start and end timestamps respectively.")
    return parser.parse_args()


async def main():
    args = parse_args()
    message_set = MessageSet(args.output_filename_template)
    async with Timer(args.update_frequency, target=get_save_messages(message_set)):
        async with TelegramClient(f"{args.session_name}", args.api_id, args.api_hash) as client:
            client.add_event_handler(get_on_new_message(message_set), events.NewMessage(incoming=True, chats=args.chats))
            await client.run_until_disconnected()
        
def get_on_new_message(message_set: MessageSet):
    async def on_new_message(message_event: events.NewMessage.Event):
        message = message_event.message

        chat = await message_event.get_chat()
        if isinstance(chat, User):
            source = chat.username if chat.username else _UNKNOWN_USER_NAME
        elif isinstance(chat, (Chat, Channel)):
            source = chat.title
        else:
            print(f"Unknown type of chat: {type(chat)}.")
            return

        await message_set.add_message(message.date, source, message.message, message.media is not None)
    return on_new_message

def get_save_messages(message_set: MessageSet):
    async def save_messages():
        await message_set.save_messages()
    return save_messages

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()