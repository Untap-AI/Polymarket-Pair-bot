"""Allow running as: python -m src"""
import asyncio
from .main import main

asyncio.run(main())
