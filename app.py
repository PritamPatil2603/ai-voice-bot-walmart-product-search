import os
import asyncio
from openai import AsyncAzureOpenAI
import chainlit as cl
from uuid import uuid4
from chainlit.logger import logger

from realtime import RealtimeClient
from realtime.tools import tools

client = AsyncAzureOpenAI(api_key=os.environ["AZURE_OPENAI_API_KEY"],
                        azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
                        azure_deployment=os.environ["AZURE_OPENAI_DEPLOYMENT"],
                        api_version="2024-10-01-preview")    

async def setup_openai_realtime(system_prompt: str):
    """Instantiate and configure the OpenAI Realtime Client"""
    openai_realtime = RealtimeClient(system_prompt = system_prompt)
    cl.user_session.set("track_id", str(uuid4()))
    
    async def handle_conversation_updated(event):
        item = event.get("item")
        delta = event.get("delta")
        """Currently used to stream audio back to the client."""
        if delta:
            # Only one of the following will be populated for any given event
            if 'audio' in delta:
                audio = delta['audio']  # Int16Array, audio added
                await cl.context.emitter.send_audio_chunk(cl.OutputAudioChunk(mimeType="pcm16", data=audio, track=cl.user_session.get("track_id")))
                
            if 'arguments' in delta:
                arguments = delta['arguments']  # string, function arguments added
                pass
            
    async def handle_item_completed(item):
        """Generate the transcript once an item is completed and populate the chat context."""
        try:
            transcript = item['item']['formatted']['transcript']
            if transcript != "":
                await cl.Message(content=transcript).send()
        except:
            pass
    
    async def handle_conversation_interrupt(event):
        """Used to cancel the client previous audio playback."""
        cl.user_session.set("track_id", str(uuid4()))
        await cl.context.emitter.send_audio_interrupt()
        
    async def handle_input_audio_transcription_completed(event):
        item = event.get("item")
        delta = event.get("delta")
        if 'transcript' in delta:
            transcript = delta['transcript']
            if transcript != "":
                await cl.Message(author="You", type="user_message", content=transcript).send()
        
    async def handle_error(event):
        logger.error(event)
        
    
    openai_realtime.on('conversation.updated', handle_conversation_updated)
    openai_realtime.on('conversation.item.completed', handle_item_completed)
    openai_realtime.on('conversation.interrupted', handle_conversation_interrupt)
    openai_realtime.on('conversation.item.input_audio_transcription.completed', handle_input_audio_transcription_completed)
    openai_realtime.on('error', handle_error)

    cl.user_session.set("openai_realtime", openai_realtime)
    coros = [openai_realtime.add_tool(tool_def, tool_handler) for tool_def, tool_handler in tools]
    await asyncio.gather(*coros)
    
    
system_prompt = """You are a customer service assistant for ShopMe.

IMPORTANT FOR DEMO:
1. Always ask for customer ID at the start if not set
2. Use 'identify_customer' function to set the customer
3. Remember the customer throughout the conversation
4. When searching products and customer wants to add them, use 'add_item_to_order'
5. Always confirm operations were successful before telling the customer

Example interaction flow:
- "What's your customer ID?" → Use identify_customer function
- "Search for milk" → Use product_search function
- "Add it to order 1" → Use add_item_to_order function with the product details from search
- "Show me order 1 items" → Use list_order_items function

Available Functions:
- identify_customer: Set the customer for the session
- get_customer_info: Get customer profile
- check_order_status: Check order status
- list_order_items: List all items in an order (NEW!)
- get_order_item: Get details of a specific item
- add_item_to_order: Add new items to existing orders
- update_order_item: Update existing items
- cancel_order: Cancel an order
- product_search: Search for products
- update_account_info: Update customer information
- schedule_callback: Schedule a callback

When customer asks to see items in an order, use list_order_items function.
When adding items from search results to orders:
1. First search for the product
2. Show the results to customer
3. When customer selects one, use add_item_to_order with exact product name and price from search

Always greet user with "Welcome to ShopMe!" for the first interaction only."""

@cl.on_chat_start
async def start():
    await cl.Message(
        content="Hi, Welcome to ShopMe! To get started, please tell me your customer ID. Press `P` to talk!"
    ).send()
    cl.user_session.set("customer_id", None)  # Don't assume customer ID
    await setup_openai_realtime(system_prompt=system_prompt)

@cl.on_message
async def on_message(message: cl.Message):
    openai_realtime: RealtimeClient = cl.user_session.get("openai_realtime")
    if openai_realtime and openai_realtime.is_connected():
        await openai_realtime.send_user_message_content([{ "type": 'input_text', "text": message.content}])
    else:
        await cl.Message(content="Please activate voice mode before sending messages!").send()

@cl.on_audio_start
async def on_audio_start():
    try:
        openai_realtime: RealtimeClient = cl.user_session.get("openai_realtime")
        # TODO: might want to recreate items to restore context
        # openai_realtime.create_conversation_item(item)
        await openai_realtime.connect()
        logger.info("Connected to OpenAI realtime")
        return True
    except Exception as e:
        await cl.ErrorMessage(content=f"Failed to connect to OpenAI realtime: {e}").send()
        return False

@cl.on_audio_chunk
async def on_audio_chunk(chunk: cl.InputAudioChunk):
    openai_realtime: RealtimeClient = cl.user_session.get("openai_realtime")
    if openai_realtime:        
        if openai_realtime.is_connected():
            await openai_realtime.append_input_audio(chunk.data)
        else:
            logger.info("RealtimeClient is not connected")

@cl.on_audio_end
@cl.on_chat_end
@cl.on_stop
async def on_end():
    openai_realtime: RealtimeClient = cl.user_session.get("openai_realtime")
    if openai_realtime and openai_realtime.is_connected():
        await openai_realtime.disconnect()