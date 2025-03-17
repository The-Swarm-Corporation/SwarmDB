"""
Agent Messaging System API Server

This module provides a production-grade FastAPI server for the Agent Messaging System,
with comprehensive API endpoints, authentication, rate limiting, and health checks.

The server is designed to be deployed with Gunicorn and Uvicorn for production use.
"""

import os
import time
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import jwt
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from starlette.middleware.base import (
    BaseHTTPMiddleware,
    RequestResponseEndpoint,
)

# Import the SwarmsDB and related models
from agent_messaging_system import (
    SwarmsDB,
    KafkaConfig,
    Message,
    MessagePriority,
    MessageStatus,
    MessageType,
)

# Environment variables
API_ENV = os.getenv("API_ENV", "development")
JWT_SECRET = os.getenv(
    "JWT_SECRET", "supersecretkey"
)  # Change in production
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
TOKEN_EXPIRE_MINUTES = int(
    os.getenv("TOKEN_EXPIRE_MINUTES", "1440")
)  # 24 hours
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC_PREFIX = os.getenv(
    "KAFKA_TOPIC_PREFIX", "agent_messaging_"
)

# Configure the messaging system
kafka_config = KafkaConfig(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=f"api_server_{uuid.uuid4().hex[:8]}",
    auto_offset_reset="earliest",
    num_partitions=int(os.getenv("KAFKA_NUM_PARTITIONS", "6")),
    replication_factor=int(
        os.getenv("KAFKA_REPLICATION_FACTOR", "3")
    ),
)

# Initialize the messaging system
messaging_system = SwarmsDB(
    base_topic=f"{KAFKA_TOPIC_PREFIX}messages",
    config=kafka_config,
    save_dir=os.getenv("MESSAGE_HISTORY_DIR", "./message_history"),
    auto_save=True,
    save_interval=int(
        os.getenv("SAVE_INTERVAL_SECONDS", "300")
    ),  # 5 minutes
)

# Initialize FastAPI
app = FastAPI(
    title="Agent Messaging System API",
    description="API for agent communication and LLM load balancing in multi-agent systems",
    version="1.0.0",
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBearer()


# Pydantic Models for Request/Response
class UserCredentials(BaseModel):
    """User credentials for authentication."""

    username: str
    password: str


class Token(BaseModel):
    """JWT token response."""

    access_token: str
    token_type: str


class ApiKey(BaseModel):
    """API key for agent authentication."""

    key: str
    agent_id: str
    description: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None


class MessageTypeEnum(str, Enum):
    """Enum for message types in API."""

    CHAT = "chat"
    COMMAND = "command"
    FUNCTION_CALL = "function_call"
    FUNCTION_RESULT = "function_result"
    SYSTEM = "system"
    ERROR = "error"
    STATUS = "status"


class MessagePriorityEnum(int, Enum):
    """Enum for message priorities in API."""

    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class MessageStatusEnum(str, Enum):
    """Enum for message statuses in API."""

    PENDING = "pending"
    DELIVERED = "delivered"
    READ = "read"
    PROCESSED = "processed"
    FAILED = "failed"


class MessageRequest(BaseModel):
    """Model for message request."""

    content: Union[str, Dict[str, Any], List[Any]]
    receiver_id: Optional[str] = None
    message_type: MessageTypeEnum = MessageTypeEnum.CHAT
    priority: MessagePriorityEnum = MessagePriorityEnum.NORMAL
    metadata: Optional[Dict[str, Any]] = None
    visible_to: Optional[List[str]] = None


class MessageResponse(BaseModel):
    """Model for message response."""

    id: str
    sender_id: str
    receiver_id: Optional[str]
    content: Union[str, Dict[str, Any], List[Any]]
    type: MessageTypeEnum
    priority: MessagePriorityEnum
    timestamp: float
    status: MessageStatusEnum
    metadata: Dict[str, Any]
    token_count: Optional[int] = None
    visible_to: List[str]

    @classmethod
    def from_message(cls, message: Message) -> "MessageResponse":
        """Convert internal Message to MessageResponse."""
        return cls(
            id=message.id,
            sender_id=message.sender_id,
            receiver_id=message.receiver_id,
            content=message.content,
            type=MessageTypeEnum(message.type.value),
            priority=MessagePriorityEnum(message.priority.value),
            timestamp=message.timestamp,
            status=MessageStatusEnum(message.status.value),
            metadata=message.metadata,
            token_count=message.token_count,
            visible_to=message.visible_to,
        )


class BroadcastRequest(BaseModel):
    """Model for broadcast request."""

    content: Union[str, Dict[str, Any], List[Any]]
    message_type: MessageTypeEnum = MessageTypeEnum.CHAT
    priority: MessagePriorityEnum = MessagePriorityEnum.NORMAL
    metadata: Optional[Dict[str, Any]] = None
    exclude_agents: Optional[List[str]] = None


class AgentRegistrationRequest(BaseModel):
    """Model for agent registration."""

    agent_id: str
    description: Optional[str] = None
    capabilities: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None


class AgentGroupRequest(BaseModel):
    """Model for agent group creation."""

    group_name: str
    agent_ids: List[str]


class GroupMessageRequest(BaseModel):
    """Model for group message request."""

    group_name: str
    content: Union[str, Dict[str, Any], List[Any]]
    message_type: MessageTypeEnum = MessageTypeEnum.CHAT
    priority: MessagePriorityEnum = MessagePriorityEnum.NORMAL
    metadata: Optional[Dict[str, Any]] = None


class MessageQueryParams(BaseModel):
    """Parameters for querying messages."""

    sender_id: Optional[str] = None
    receiver_id: Optional[str] = None
    message_type: Optional[MessageTypeEnum] = None
    status: Optional[MessageStatusEnum] = None
    after_timestamp: Optional[float] = None
    before_timestamp: Optional[float] = None
    limit: int = 100
    skip: int = 0


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str
    environment: str
    kafka_connected: bool
    timestamp: float = Field(default_factory=time.time)


class SystemStats(BaseModel):
    """System statistics response."""

    total_messages: int
    active_agents: int
    messages_by_type: Dict[str, int]
    messages_by_status: Dict[str, int]
    messages_by_agent: Dict[str, Dict[str, int]]
    last_save_time: float


class RateLimiter(BaseHTTPMiddleware):
    """Rate limiter middleware."""

    def __init__(self, app, rate_limit_per_minute: int = 100):
        super().__init__(app)
        self.rate_limit = rate_limit_per_minute
        self.requests = {}

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> JSONResponse:
        # Get client IP
        client_ip = request.client.host

        # Check if IP is in requests dict
        if client_ip not in self.requests:
            self.requests[client_ip] = []

        # Remove requests older than 1 minute
        current_time = time.time()
        self.requests[client_ip] = [
            timestamp
            for timestamp in self.requests[client_ip]
            if current_time - timestamp < 60
        ]

        # Check if rate limit is exceeded
        if len(self.requests[client_ip]) >= self.rate_limit:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "error": "Rate limit exceeded, please try again later."
                },
            )

        # Add current request timestamp
        self.requests[client_ip].append(current_time)

        # Process the request
        return await call_next(request)


# Add rate limiter middleware
app.add_middleware(
    RateLimiter,
    rate_limit_per_minute=int(
        os.getenv("RATE_LIMIT_PER_MINUTE", "300")
    ),
)


# Authentication functions
def create_access_token(data: dict, expires_delta: timedelta = None):
    """Create a JWT access token."""
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(
            minutes=TOKEN_EXPIRE_MINUTES
        )

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(
        to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM
    )

    return encoded_jwt


def get_current_agent(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Validate JWT token and return agent ID."""
    try:
        payload = jwt.decode(
            credentials.credentials,
            JWT_SECRET,
            algorithms=[JWT_ALGORITHM],
        )
        agent_id = payload.get("sub")
        if agent_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return agent_id


# API Routes
@app.post("/auth/token", response_model=Token)
async def login_for_access_token(credentials: UserCredentials):
    """
    Get an access token for API authentication.

    In a production system, this would validate against a user database.
    For demonstration, we're using a simplified approach.
    """
    # In a real system, validate credentials against a database
    # For demo purposes, we'll accept any username/password
    if not credentials.username or not credentials.password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(minutes=TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": credentials.username},
        expires_delta=access_token_expires,
    )

    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/agents/register", status_code=status.HTTP_201_CREATED)
async def register_agent(
    registration: AgentRegistrationRequest,
    agent_id: str = Depends(get_current_agent),
):
    """Register an agent with the messaging system."""
    # Only allow agents to register themselves or admin to register anyone
    if agent_id != registration.agent_id and agent_id != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only register yourself or need admin privileges",
        )

    try:
        messaging_system.register_agent(registration.agent_id)

        # Store additional agent metadata if provided
        if (
            registration.metadata
            or registration.capabilities
            or registration.description
        ):
            metadata_dict = {
                "description": registration.description,
                "capabilities": registration.capabilities,
                **(registration.metadata or {}),
            }

            # A real implementation would store this in a database
            # For now, we'll store it in the messaging system's metadata
            if not hasattr(messaging_system, "agent_metadata"):
                messaging_system.agent_metadata = {}

            messaging_system.agent_metadata[registration.agent_id] = (
                metadata_dict
            )

        return {
            "status": "success",
            "agent_id": registration.agent_id,
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to register agent: {str(e)}",
        )


@app.delete("/agents/{agent_id}")
async def deregister_agent(
    agent_id: str,
    current_agent: str = Depends(get_current_agent),
):
    """Deregister an agent from the messaging system."""
    # Only allow agents to deregister themselves or admin to deregister anyone
    if current_agent != agent_id and current_agent != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only deregister yourself or need admin privileges",
        )

    try:
        messaging_system.deregister_agent(agent_id)

        # Remove agent metadata if exists
        if (
            hasattr(messaging_system, "agent_metadata")
            and agent_id in messaging_system.agent_metadata
        ):
            del messaging_system.agent_metadata[agent_id]

        return {"status": "success", "agent_id": agent_id}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to deregister agent: {str(e)}",
        )


@app.post("/messages", response_model=MessageResponse)
async def send_message(
    message_request: MessageRequest,
    current_agent: str = Depends(get_current_agent),
):
    """Send a message from one agent to another."""
    try:
        # Convert API enums to internal enums
        message_type = MessageType(message_request.message_type.value)
        priority = MessagePriority(message_request.priority.value)

        # Send the message
        message_id = messaging_system.send_message(
            sender_id=current_agent,
            content=message_request.content,
            receiver_id=message_request.receiver_id,
            message_type=message_type,
            priority=priority,
            metadata=message_request.metadata,
            visible_to=message_request.visible_to,
        )

        # Get the message object
        message = messaging_system.get_message(message_id)

        # Convert to response model
        return MessageResponse.from_message(message)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to send message: {str(e)}",
        )


@app.post("/messages/broadcast", response_model=List[str])
async def broadcast_message(
    broadcast_request: BroadcastRequest,
    current_agent: str = Depends(get_current_agent),
):
    """Broadcast a message to all agents."""
    try:
        # Convert API enums to internal enums
        message_type = MessageType(
            broadcast_request.message_type.value
        )
        priority = MessagePriority(broadcast_request.priority.value)

        # Send the broadcast message
        message_id = messaging_system.broadcast_message(
            sender_id=current_agent,
            content=broadcast_request.content,
            message_type=message_type,
            priority=priority,
            metadata=broadcast_request.metadata,
            exclude_agents=broadcast_request.exclude_agents,
        )

        return {"status": "success", "message_id": message_id}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to broadcast message: {str(e)}",
        )


@app.get("/messages/{message_id}", response_model=MessageResponse)
async def get_message(
    message_id: str,
    current_agent: str = Depends(get_current_agent),
):
    """Get a specific message by ID."""
    message = messaging_system.get_message(message_id)

    if not message:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Message {message_id} not found",
        )

    # Check if the agent is allowed to see this message
    if (
        current_agent != "admin"
        and current_agent != message.sender_id
        and current_agent != message.receiver_id
        and (
            message.visible_to
            and current_agent not in message.visible_to
        )
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to view this message",
        )

    return MessageResponse.from_message(message)


@app.get("/messages", response_model=List[MessageResponse])
async def query_messages(
    sender_id: Optional[str] = None,
    receiver_id: Optional[str] = None,
    message_type: Optional[MessageTypeEnum] = None,
    status: Optional[MessageStatusEnum] = None,
    after_timestamp: Optional[float] = None,
    before_timestamp: Optional[float] = None,
    limit: int = 100,
    current_agent: str = Depends(get_current_agent),
):
    """Query messages based on various filters."""
    try:
        # Convert API enums to internal enums if provided
        internal_message_type = None
        if message_type:
            internal_message_type = MessageType(message_type.value)

        internal_status = None
        if status:
            internal_status = MessageStatus(status.value)

        # Limit scope to the current agent unless admin
        if current_agent != "admin":
            # Regular agents can only see messages they sent or received
            if sender_id and sender_id != current_agent:
                if receiver_id != current_agent:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail="You can only query messages you sent or received",
                    )

        # Query the messages
        messages = messaging_system.query_messages(
            sender_id=sender_id,
            receiver_id=receiver_id,
            message_type=internal_message_type,
            status=internal_status,
            after_timestamp=after_timestamp,
            before_timestamp=before_timestamp,
            limit=limit,
        )

        # Convert to response models
        return [MessageResponse.from_message(msg) for msg in messages]

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to query messages: {str(e)}",
        )


@app.get(
    "/agents/{agent_id}/messages",
    response_model=List[MessageResponse],
)
async def get_agent_messages(
    agent_id: str,
    status: Optional[MessageStatusEnum] = None,
    limit: int = 100,
    skip: int = 0,
    current_agent: str = Depends(get_current_agent),
):
    """Get all messages for a specific agent."""
    # Check permissions
    if current_agent != agent_id and current_agent != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only access your own messages",
        )

    try:
        # Convert API enum to internal enum if provided
        internal_status = None
        if status:
            internal_status = MessageStatus(status.value)

        # Get agent messages
        messages = messaging_system.get_agent_messages(
            agent_id=agent_id,
            status=internal_status,
            limit=limit,
            skip=skip,
        )

        # Convert to response models
        return [MessageResponse.from_message(msg) for msg in messages]

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get agent messages: {str(e)}",
        )


@app.post("/agents/receive", response_model=List[MessageResponse])
async def receive_messages(
    max_messages: int = 100,
    timeout: float = 1.0,
    current_agent: str = Depends(get_current_agent),
):
    """Receive messages for the current agent."""
    try:
        messages = messaging_system.receive_messages(
            agent_id=current_agent,
            max_messages=max_messages,
            timeout=timeout,
        )

        # Convert to response models
        return [MessageResponse.from_message(msg) for msg in messages]

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to receive messages: {str(e)}",
        )


@app.put("/messages/{message_id}/status")
async def update_message_status(
    message_id: str,
    status: MessageStatusEnum,
    current_agent: str = Depends(get_current_agent),
):
    """Update the status of a message."""
    message = messaging_system.get_message(message_id)

    if not message:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Message {message_id} not found",
        )

    # Check if the agent is allowed to update this message
    if (
        current_agent != "admin"
        and current_agent != message.receiver_id
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only update status of messages you received",
        )

    try:
        # Update message status
        # Normally we would use a dedicated method, but we'll update directly
        if status == MessageStatusEnum.PROCESSED:
            messaging_system.mark_message_as_processed(message_id)
        else:
            # For other statuses, update manually
            messaging_system.messages[message_id].status = (
                MessageStatus(status.value)
            )

        return {"status": "success", "message_id": message_id}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update message status: {str(e)}",
        )


@app.post("/groups", status_code=status.HTTP_201_CREATED)
async def create_agent_group(
    group_request: AgentGroupRequest,
    current_agent: str = Depends(get_current_agent),
):
    """Create a named group of agents."""
    try:
        messaging_system.add_agent_group(
            group_name=group_request.group_name,
            agent_ids=group_request.agent_ids,
        )

        return {
            "status": "success",
            "group_name": group_request.group_name,
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create agent group: {str(e)}",
        )


@app.post("/groups/message", response_model=List[str])
async def send_group_message(
    group_message: GroupMessageRequest,
    current_agent: str = Depends(get_current_agent),
):
    """Send a message to all agents in a group."""
    try:
        # Convert API enums to internal enums
        message_type = MessageType(group_message.message_type.value)
        priority = MessagePriority(group_message.priority.value)

        # Send to group
        message_ids = messaging_system.send_to_group(
            sender_id=current_agent,
            group_name=group_message.group_name,
            content=group_message.content,
            message_type=message_type,
            priority=priority,
            metadata=group_message.metadata,
        )

        return {"status": "success", "message_ids": message_ids}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to send group message: {str(e)}",
        )


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Check Kafka connection
        kafka_connected = True
        try:
            # Try to list topics as a connection test
            messaging_system.admin_client.list_topics(timeout=2)
        except Exception:
            kafka_connected = False

        return HealthResponse(
            status="ok",
            version="1.0.0",
            environment=API_ENV,
            kafka_connected=kafka_connected,
        )

    except Exception:
        return HealthResponse(
            status="error",
            version="1.0.0",
            environment=API_ENV,
            kafka_connected=False,
        )


@app.get("/stats", response_model=SystemStats)
async def system_stats(
    current_agent: str = Depends(get_current_agent),
):
    """Get system statistics."""
    # Only admin can access system stats
    if current_agent != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required",
        )

    try:
        stats = messaging_system.get_stats()
        return stats

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get system stats: {str(e)}",
        )


@app.post("/admin/save", status_code=status.HTTP_200_OK)
async def trigger_save(
    current_agent: str = Depends(get_current_agent),
):
    """Manually trigger saving of message history."""
    # Only admin can trigger saves
    if current_agent != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required",
        )

    try:
        messaging_system.save_message_history()
        return {"status": "success", "timestamp": time.time()}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save message history: {str(e)}",
        )


@app.post("/admin/flush", status_code=status.HTTP_200_OK)
async def flush_old_messages(
    older_than: Optional[float] = None,
    current_agent: str = Depends(get_current_agent),
):
    """Flush old messages from the system."""
    # Only admin can flush messages
    if current_agent != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required",
        )

    try:
        count = messaging_system.flush_old_messages(older_than)
        return {"status": "success", "flushed_count": count}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to flush old messages: {str(e)}",
        )


@app.post("/admin/resend_failed", status_code=status.HTTP_200_OK)
async def resend_failed_messages(
    current_agent: str = Depends(get_current_agent),
):
    """Resend all failed messages."""
    # Only admin can resend failed messages
    if current_agent != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required",
        )

    try:
        resent_ids = messaging_system.resend_failed_messages()
        return {
            "status": "success",
            "resent_count": len(resent_ids),
            "message_ids": resent_ids,
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to resend failed messages: {str(e)}",
        )


@app.post("/admin/scale_partitions", status_code=status.HTTP_200_OK)
async def auto_scale_partitions(
    current_agent: str = Depends(get_current_agent),
):
    """Automatically scale Kafka partitions based on load."""
    # Only admin can scale partitions
    if current_agent != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required",
        )

    try:
        messaging_system.auto_scale_partitions()
        return {"status": "success", "timestamp": time.time()}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to scale partitions: {str(e)}",
        )


# Shutdown event
@app.on_event("shutdown")
def shutdown_event():
    """Clean up on server shutdown."""
    try:
        messaging_system.close()
    except Exception as e:
        print(f"Error closing messaging system: {e}")


# Entry point for running the application directly
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=os.getenv("API_ENV") == "development",
    )
