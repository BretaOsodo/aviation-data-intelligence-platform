import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Optional

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request, Depends, Path, Query