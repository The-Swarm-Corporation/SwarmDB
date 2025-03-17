import datetime
import json
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

import yaml
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic
from loguru import logger
from pydantic import BaseModel, Field, validator


class MessageType(str, Enum):
    """Types of messages that can be sent through the messaging system."""
    
    CHAT = "chat"
    COMMAND = "command"
    FUNCTION_CALL = "function_call"
    FUNCTION_RESULT = "function_result"
    SYSTEM = "system"
    ERROR = "error"
    STATUS = "status"


class MessagePriority(int, Enum):
    """Priority levels for messages."""
    
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class MessageStatus(str, Enum):
    """Status of a message in the system."""
    
    PENDING = "pending"
    DELIVERED = "delivered"
    READ = "read"
    PROCESSED = "processed"
    FAILED = "failed"


class Message(BaseModel):
    """
    Represents a message in the agent communication system.
    
    Attributes:
        id: Unique identifier for the message
        sender_id: ID of the agent sending the message
        receiver_id: ID of the agent receiving the message (None for broadcast)
        content: Content of the message (can be string or structured data)
        type: Type of message (chat, command, function_call, etc.)
        priority: Priority level of the message
        timestamp: When the message was created
        status: Current status of the message
        metadata: Additional information about the message
        token_count: Number of tokens in the message (for LLM context tracking)
        visible_to: List of agent IDs that can see this message
    """
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    sender_id: str
    receiver_id: Optional[str] = None
    content: Union[str, Dict[str, Any], List[Any]]
    type: MessageType = MessageType.CHAT
    priority: MessagePriority = MessagePriority.NORMAL
    timestamp: float = Field(default_factory=time.time)
    status: MessageStatus = MessageStatus.PENDING
    metadata: Dict[str, Any] = Field(default_factory=dict)
    token_count: Optional[int] = None
    visible_to: List[str] = Field(default_factory=list)
    
    @validator("timestamp", pre=True, always=True)
    def set_timestamp(cls, v):
        """Ensure timestamp is a float."""
        if v is None:
            return time.time()
        return float(v)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary."""
        return {
            **asdict(self),
            "type": self.type.value,
            "priority": self.priority.value,
            "status": self.status.value,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        """Create message from dictionary."""
        # Convert enum string values to actual enum instances
        if isinstance(data.get("type"), str):
            data["type"] = MessageType(data["type"])
        if isinstance(data.get("priority"), int):
            data["priority"] = MessagePriority(data["priority"])
        if isinstance(data.get("status"), str):
            data["status"] = MessageStatus(data["status"])
        
        return cls(**data)


@dataclass
class KafkaConfig:
    """Configuration for Kafka connection and topics."""
    
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "agent_messaging_system"
    auto_offset_reset: str = "earliest"
    num_partitions: int = 3
    replication_factor: int = 1
    retention_ms: int = 604800000  # 7 days
    max_poll_interval_ms: int = 300000  # 5 minutes
    session_timeout_ms: int = 30000  # 30 seconds
    heartbeat_interval_ms: int = 10000  # 10 seconds
    consumer_timeout_ms: int = 1000  # 1 second


class SwarmsDB:
    """
    A production-grade message queue system for agent communication and LLM backend load balancing.
    This system uses Apache Kafka for scalable message distribution and provides auto-partitioning
    based on agent IDs for efficient message routing.
    
    Features:
    - Auto-partitioning based on agent IDs
    - Stateful management of message queues
    - Persistent storage of all interactions as JSON
    - Targeted message delivery between agents
    - Broadcast messaging capabilities
    - Priority-based message handling
    - Token counting for LLM context management
    - Visibility control for multi-agent scenarios
    
    Args:
        base_topic: The base Kafka topic name to use
        config: Kafka configuration options
        save_dir: Directory to save message history
        auto_save: Whether to automatically save message history
        save_interval: How often to save message history (in seconds)
        max_messages_per_file: Maximum number of messages per history file
        token_counter: Optional function to count tokens in messages
    """
    
    def __init__(
        self,
        base_topic: str = "agent_messaging",
        config: Optional[KafkaConfig] = None,
        save_dir: Optional[Union[str, Path]] = None,
        auto_save: bool = True,
        save_interval: int = 300,  # 5 minutes
        max_messages_per_file: int = 10000,
        token_counter: Optional[callable] = None,
    ):
        # Initialize configuration
        self.base_topic = base_topic
        self.config = config or KafkaConfig()
        
        # Setup logging
        logger.configure(
            handlers=[
                {
                    "sink": os.path.join(os.getcwd(), "agent_messaging.log") 
                        if save_dir is None else os.path.join(save_dir, "agent_messaging.log"),
                    "level": "INFO",
                    "rotation": "10 MB",
                    "compression": "zip",
                    "retention": "1 month",
                }
            ]
        )
        
        # Initialize Kafka producer
        self.producer = Producer({
            "bootstrap.servers": self.config.bootstrap_servers,
            "client.id": f"agent_producer_{uuid.uuid4().hex[:8]}",
            "acks": "all",  # Wait for all replicas
            "linger.ms": 10,  # Small batching delay for better throughput
        })
        
        # Set up Kafka admin client for topic management
        self.admin_client = AdminClient({
            "bootstrap.servers": self.config.bootstrap_servers
        })
        
        # Create topics if they don't exist
        self._ensure_topics_exist()
        
        # Setup consumer mapping
        self.consumers: Dict[str, Consumer] = {}
        
        # Message storage for stateful management
        self.messages: Dict[str, Message] = {}
        self.agent_inbox: Dict[str, List[str]] = {}
        self.message_count = 0
        
        # Token counting function for LLM context tracking
        self.token_counter = token_counter
        
        # Setup save configuration
        self.save_dir = Path(save_dir) if save_dir else Path(os.getcwd()) / "message_history"
        self.save_dir.mkdir(parents=True, exist_ok=True)
        self.auto_save = auto_save
        self.save_interval = save_interval
        self.max_messages_per_file = max_messages_per_file
        self.last_save_time = time.time()
        
        # Register agents that are currently known to the system
        self.registered_agents: Set[str] = set()
        
        logger.info(f"SwarmsDB initialized with base topic: {base_topic}")
    
    def _ensure_topics_exist(self) -> None:
        """Ensure that required Kafka topics exist, creating them if necessary."""
        topics = self.admin_client.list_topics(timeout=10).topics
        
        # List of topics to create if they don't exist
        topics_to_create = []
        
        # Check if base topic exists
        if self.base_topic not in topics:
            topics_to_create.append(
                NewTopic(
                    self.base_topic,
                    num_partitions=self.config.num_partitions,
                    replication_factor=self.config.replication_factor,
                    config={
                        "retention.ms": str(self.config.retention_ms),
                    },
                )
            )
        
        # Create the error topic if it doesn't exist
        error_topic = f"{self.base_topic}_errors"
        if error_topic not in topics:
            topics_to_create.append(
                NewTopic(
                    error_topic,
                    num_partitions=1,  # Single partition for error topic
                    replication_factor=self.config.replication_factor,
                    config={
                        "retention.ms": str(self.config.retention_ms * 2),  # Keep errors longer
                    },
                )
            )
        
        # Create topics if needed
        if topics_to_create:
            result = self.admin_client.create_topics(topics_to_create)
            
            # Wait for topic creation to complete
            for topic, future in result.items():
                try:
                    future.result()
                    logger.info(f"Topic {topic} created successfully")
                except KafkaException as e:
                    if "already exists" in str(e):
                        logger.warning(f"Topic {topic} already exists")
                    else:
                        logger.error(f"Failed to create topic {topic}: {e}")
                        raise
    
    def _count_tokens(self, content: Union[str, Dict[str, Any], List[Any]]) -> int:
        """Count tokens in the message content using the provided token counter."""
        if self.token_counter is None:
            return 0
        
        if isinstance(content, (dict, list)):
            content_str = json.dumps(content)
        else:
            content_str = str(content)
            
        return self.token_counter(content_str)
    
    def _get_partition(self, agent_id: str) -> int:
        """Determine Kafka partition for a given agent ID."""
        # Simple hashing to consistently map agent_id to a partition
        return hash(agent_id) % self.config.num_partitions
    
    def register_agent(self, agent_id: str) -> None:
        """
        Register an agent with the messaging system.
        
        Args:
            agent_id: Unique identifier for the agent
        """
        if agent_id in self.registered_agents:
            logger.debug(f"Agent {agent_id} already registered")
            return
        
        # Add agent to registered set
        self.registered_agents.add(agent_id)
        
        # Initialize agent inbox
        if agent_id not in self.agent_inbox:
            self.agent_inbox[agent_id] = []
        
        # Create consumer for this agent if it doesn't exist
        if agent_id not in self.consumers:
            consumer = Consumer({
                "bootstrap.servers": self.config.bootstrap_servers,
                "group.id": f"{self.config.group_id}_{agent_id}",
                "auto.offset.reset": self.config.auto_offset_reset,
                "max.poll.interval.ms": self.config.max_poll_interval_ms,
                "session.timeout.ms": self.config.session_timeout_ms,
                "heartbeat.interval.ms": self.config.heartbeat_interval_ms,
            })
            consumer.subscribe([self.base_topic])
            self.consumers[agent_id] = consumer
        
        logger.info(f"Agent {agent_id} registered with messaging system")
    
    def deregister_agent(self, agent_id: str) -> None:
        """
        Deregister an agent from the messaging system.
        
        Args:
            agent_id: Unique identifier for the agent
        """
        if agent_id not in self.registered_agents:
            logger.warning(f"Agent {agent_id} not registered")
            return
        
        # Remove agent from registered set
        self.registered_agents.remove(agent_id)
        
        # Close consumer if it exists
        if agent_id in self.consumers:
            self.consumers[agent_id].close()
            del self.consumers[agent_id]
        
        logger.info(f"Agent {agent_id} deregistered from messaging system")
    
    def _delivery_callback(self, err, msg) -> None:
        """Callback function for message delivery reports."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
            # Update message status in local store
            message_id = msg.key().decode("utf-8")
            if message_id in self.messages:
                self.messages[message_id].status = MessageStatus.FAILED
                self.messages[message_id].metadata["error"] = str(err)
        else:
            # Message delivered successfully
            message_id = msg.key().decode("utf-8")
            if message_id in self.messages:
                self.messages[message_id].status = MessageStatus.DELIVERED
    
    def send_message(
        self,
        sender_id: str,
        content: Union[str, Dict[str, Any], List[Any]],
        receiver_id: Optional[str] = None,
        message_type: MessageType = MessageType.CHAT,
        priority: MessagePriority = MessagePriority.NORMAL,
        metadata: Optional[Dict[str, Any]] = None,
        visible_to: Optional[List[str]] = None,
    ) -> str:
        """
        Send a message from one agent to another or broadcast to all agents.
        
        Args:
            sender_id: ID of the agent sending the message
            content: Content of the message (string or structured data)
            receiver_id: ID of the agent to receive the message (None for broadcast)
            message_type: Type of message being sent
            priority: Priority level of the message
            metadata: Additional information about the message
            visible_to: List of agent IDs that can see this message
            
        Returns:
            The message ID of the sent message
        """
        # Ensure sender is registered
        if sender_id not in self.registered_agents:
            self.register_agent(sender_id)
        
        # Ensure receiver is registered (if specified)
        if receiver_id is not None and receiver_id not in self.registered_agents:
            self.register_agent(receiver_id)
        
        # Count tokens if token counter is available
        token_count = self._count_tokens(content) if self.token_counter else None
        
        # Create message
        message = Message(
            sender_id=sender_id,
            receiver_id=receiver_id,
            content=content,
            type=message_type,
            priority=priority,
            metadata=metadata or {},
            token_count=token_count,
            visible_to=visible_to or [],
        )
        
        # If broadcast, add all registered agents to visible_to
        if receiver_id is None and not message.visible_to:
            message.visible_to = list(self.registered_agents)
        
        # Store message in local state
        self.messages[message.id] = message
        self.message_count += 1
        
        # Add message to receiver's inbox (or all inboxes for broadcast)
        if receiver_id is not None:
            if receiver_id in self.agent_inbox:
                self.agent_inbox[receiver_id].append(message.id)
        else:
            # Broadcast - add to all inboxes
            for agent_id in self.registered_agents:
                self.agent_inbox[agent_id].append(message.id)
        
        # Prepare message for Kafka
        message_data = json.dumps(message.to_dict()).encode("utf-8")
        
        # Determine partition based on receiver (or sender for broadcasts)
        target_id = receiver_id if receiver_id is not None else sender_id
        partition = self._get_partition(target_id)
        
        # Produce message to Kafka
        try:
            self.producer.produce(
                topic=self.base_topic,
                key=message.id.encode("utf-8"),
                value=message_data,
                partition=partition,
                callback=self._delivery_callback,
            )
            # Trigger any available delivery callbacks
            self.producer.poll(0)
            
            logger.info(
                f"Message sent: {message.id} from {sender_id} to "
                f"{receiver_id if receiver_id else 'broadcast'}, type={message_type.value}"
            )
            
            # Auto-save if necessary
            if self.auto_save and (
                time.time() - self.last_save_time > self.save_interval
                or self.message_count % self.max_messages_per_file == 0
            ):
                self.save_message_history()
            
            return message.id
            
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            # Update message status
            message.status = MessageStatus.FAILED
            message.metadata["error"] = str(e)
            
            # Send to error topic
            try:
                self.producer.produce(
                    topic=f"{self.base_topic}_errors",
                    key=message.id.encode("utf-8"),
                    value=message_data,
                )
            except Exception as nested_e:
                logger.error(f"Failed to send message to error topic: {nested_e}")
            
            raise
    
    def receive_messages(
        self, 
        agent_id: str, 
        max_messages: int = 100,
        timeout: float = 1.0
    ) -> List[Message]:
        """
        Receive messages for a specific agent.
        
        Args:
            agent_id: ID of the agent receiving messages
            max_messages: Maximum number of messages to receive
            timeout: Timeout in seconds to wait for messages
            
        Returns:
            List of Message objects received
        """
        if agent_id not in self.registered_agents:
            logger.warning(f"Agent {agent_id} not registered, registering now")
            self.register_agent(agent_id)
        
        if agent_id not in self.consumers:
            logger.error(f"No consumer for agent {agent_id}")
            return []
        
        consumer = self.consumers[agent_id]
        messages = []
        message_count = 0
        start_time = time.time()
        
        while message_count < max_messages and (time.time() - start_time) < timeout:
            msg = consumer.poll(timeout=self.config.consumer_timeout_ms / 1000)
            
            if msg is None:
                # No message received
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, no more messages
                    break
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
            
            try:
                # Parse message
                message_data = json.loads(msg.value().decode("utf-8"))
                message = Message.from_dict(message_data)
                
                # Check if message is for this agent
                if (message.receiver_id == agent_id or message.receiver_id is None) and (
                    agent_id in message.visible_to or len(message.visible_to) == 0
                ):
                    # Update message status
                    message.status = MessageStatus.READ
                    self.messages[message.id] = message
                    
                    # Add to result list
                    messages.append(message)
                    message_count += 1
                    
                    logger.debug(f"Message {message.id} received by agent {agent_id}")
            
            except Exception as e:
                logger.error(f"Error processing message: {e}")
        
        return messages
    
    def get_message(self, message_id: str) -> Optional[Message]:
        """
        Get a specific message by ID.
        
        Args:
            message_id: ID of the message to retrieve
            
        Returns:
            Message object if found, None otherwise
        """
        return self.messages.get(message_id)
    
    def get_agent_messages(
        self,
        agent_id: str,
        status: Optional[MessageStatus] = None,
        limit: int = 100,
        skip: int = 0,
    ) -> List[Message]:
        """
        Get all messages for a specific agent.
        
        Args:
            agent_id: ID of the agent
            status: Filter by message status
            limit: Maximum number of messages to return
            skip: Number of messages to skip (for pagination)
            
        Returns:
            List of Message objects for the agent
        """
        if agent_id not in self.agent_inbox:
            return []
        
        message_ids = self.agent_inbox[agent_id]
        filtered_messages = []
        
        for idx, msg_id in enumerate(reversed(message_ids)):
            if idx < skip:
                continue
                
            if len(filtered_messages) >= limit:
                break
                
            if msg_id in self.messages:
                message = self.messages[msg_id]
                if status is None or message.status == status:
                    filtered_messages.append(message)
        
        return filtered_messages
    
    def mark_message_as_processed(self, message_id: str) -> bool:
        """
        Mark a message as processed.
        
        Args:
            message_id: ID of the message to mark
            
        Returns:
            True if successful, False otherwise
        """
        if message_id not in self.messages:
            logger.warning(f"Message {message_id} not found")
            return False
            
        self.messages[message_id].status = MessageStatus.PROCESSED
        return True
    
    def query_messages(
        self,
        sender_id: Optional[str] = None,
        receiver_id: Optional[str] = None,
        message_type: Optional[MessageType] = None,
        status: Optional[MessageStatus] = None,
        after_timestamp: Optional[float] = None,
        before_timestamp: Optional[float] = None,
        limit: int = 100,
    ) -> List[Message]:
        """
        Query messages based on various filters.
        
        Args:
            sender_id: Filter by sender ID
            receiver_id: Filter by receiver ID
            message_type: Filter by message type
            status: Filter by message status
            after_timestamp: Filter messages after this timestamp
            before_timestamp: Filter messages before this timestamp
            limit: Maximum number of messages to return
            
        Returns:
            List of Message objects matching the query
        """
        result = []
        count = 0
        
        for message in reversed(list(self.messages.values())):
            if count >= limit:
                break
                
            # Apply filters
            if sender_id is not None and message.sender_id != sender_id:
                continue
                
            if receiver_id is not None and message.receiver_id != receiver_id:
                continue
                
            if message_type is not None and message.type != message_type:
                continue
                
            if status is not None and message.status != status:
                continue
                
            if after_timestamp is not None and message.timestamp <= after_timestamp:
                continue
                
            if before_timestamp is not None and message.timestamp >= before_timestamp:
                continue
                
            result.append(message)
            count += 1
        
        return result
    
    def search_messages(
        self,
        keyword: str,
        case_sensitive: bool = False,
        limit: int = 100,
    ) -> List[Message]:
        """
        Search messages for a keyword.
        
        Args:
            keyword: Keyword to search for
            case_sensitive: Whether to use case-sensitive search
            limit: Maximum number of messages to return
            
        Returns:
            List of Message objects containing the keyword
        """
        result = []
        count = 0
        
        for message in reversed(list(self.messages.values())):
            if count >= limit:
                break
                
            content_str = (
                json.dumps(message.content)
                if isinstance(message.content, (dict, list))
                else str(message.content)
            )
            
            if not case_sensitive:
                if keyword.lower() in content_str.lower():
                    result.append(message)
                    count += 1
            else:
                if keyword in content_str:
                    result.append(message)
                    count += 1
        
        return result
    
    def get_conversation(
        self,
        agent_id_1: str,
        agent_id_2: str,
        limit: int = 100,
    ) -> List[Message]:
        """
        Get conversation between two agents.
        
        Args:
            agent_id_1: First agent ID
            agent_id_2: Second agent ID
            limit: Maximum number of messages to return
            
        Returns:
            List of Message objects representing the conversation
        """
        return self.query_messages(
            sender_id=agent_id_1,
            receiver_id=agent_id_2,
            limit=limit // 2,
        ) + self.query_messages(
            sender_id=agent_id_2,
            receiver_id=agent_id_1,
            limit=limit // 2,
        )
    
    def broadcast_message(
        self,
        sender_id: str,
        content: Union[str, Dict[str, Any], List[Any]],
        message_type: MessageType = MessageType.CHAT,
        priority: MessagePriority = MessagePriority.NORMAL,
        metadata: Optional[Dict[str, Any]] = None,
        exclude_agents: Optional[List[str]] = None,
    ) -> str:
        """
        Broadcast a message to all registered agents.
        
        Args:
            sender_id: ID of the agent sending the message
            content: Content of the message
            message_type: Type of message being sent
            priority: Priority level of the message
            metadata: Additional information about the message
            exclude_agents: List of agent IDs to exclude from broadcast
            
        Returns:
            The message ID of the broadcast message
        """
        # Determine visible_to list (all agents except excluded ones)
        exclude_set = set(exclude_agents or [])
        visible_to = [
            agent_id for agent_id in self.registered_agents
            if agent_id != sender_id and agent_id not in exclude_set
        ]
        
        # Send broadcast message
        return self.send_message(
            sender_id=sender_id,
            content=content,
            receiver_id=None,  # Broadcast
            message_type=message_type,
            priority=priority,
            metadata=metadata,
            visible_to=visible_to,
        )
    
    def save_message_history(self, filename: Optional[str] = None) -> None:
        """
        Save the message history to a file.
        
        Args:
            filename: Name of the file to save to (default: auto-generated)
        """
        if not filename:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"message_history_{timestamp}_{self.message_count}.json"
        
        filepath = self.save_dir / filename
        
        try:
            with open(filepath, "w") as f:
                # Convert messages to dictionaries
                message_dicts = {
                    msg_id: msg.to_dict()
                    for msg_id, msg in self.messages.items()
                }
                
                # Create history object
                history = {
                    "messages": message_dicts,
                    "agent_inbox": self.agent_inbox,
                    "registered_agents": list(self.registered_agents),
                    "timestamp": time.time(),
                    "message_count": self.message_count,
                }
                
                json.dump(history, f, indent=2)
                
            logger.info(f"Message history saved to {filepath}")
            self.last_save_time = time.time()
            
        except Exception as e:
            logger.error(f"Failed to save message history: {e}")
    
    def load_message_history(self, filepath: Union[str, Path]) -> None:
        """
        Load message history from a file.
        
        Args:
            filepath: Path to the file to load from
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            logger.error(f"Message history file {filepath} does not exist")
            return
        
        try:
            with open(filepath, "r") as f:
                history = json.load(f)
                
            # Restore messages
            self.messages = {
                msg_id: Message.from_dict(msg_data)
                for msg_id, msg_data in history["messages"].items()
            }
            
            # Restore agent inboxes
            self.agent_inbox = history["agent_inbox"]
            
            # Restore registered agents
            for agent_id in history["registered_agents"]:
                self.register_agent(agent_id)
                
            # Restore message count
            self.message_count = history["message_count"]
            
            logger.info(f"Message history loaded from {filepath}")
            
        except Exception as e:
            logger.error(f"Failed to load message history: {e}")
    
    def export_as_yaml(self, filepath: Union[str, Path]) -> None:
        """
        Export message history as YAML.
        
        Args:
            filepath: Path to save the YAML file
        """
        filepath = Path(filepath)
        
        try:
            with open(filepath, "w") as f:
                # Convert messages to dictionaries
                message_dicts = {
                    msg_id: msg.to_dict()
                    for msg_id, msg in self.messages.items()
                }
                
                # Create history object
                history = {
                    "messages": message_dicts,
                    "agent_inbox": self.agent_inbox,
                    "registered_agents": list(self.registered_agents),
                    "timestamp": time.time(),
                    "message_count": self.message_count,
                }
                
                yaml.dump(history, f, sort_keys=False)
                
            logger.info(f"Message history exported as YAML to {filepath}")
            
        except Exception as e:
            logger.error(f"Failed to export message history as YAML: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the messaging system.
        
        Returns:
            Dictionary with statistics
        """
        # Count messages by type
        message_types = {msg_type.value: 0 for msg_type in MessageType}
        for msg in self.messages.values():
            message_types[msg.type.value] += 1
            
        # Count messages by status
        message_statuses = {status.value: 0 for status in MessageStatus}
        for msg in self.messages.values():
            message_statuses[msg.status.value] += 1
            
        # Count messages by agent
        messages_by_agent = {}
        for agent_id in self.registered_agents:
            sent = len([msg for msg in self.messages.values() if msg.sender_id == agent_id])
            received = len([msg for msg in self.messages.values() if msg.receiver_id == agent_id])
            messages_by_agent[agent_id] = {
                "sent": sent,
                "received": received,
                "total": sent + received,
            }
            
        return {
            "total_messages": self.message_count,
            "active_agents": len(self.registered_agents),
            "messages_by_type": message_types,
            "messages_by_status": message_statuses,
            "messages_by_agent": messages_by_agent,
            "last_save_time": self.last_save_time,
        }
    
    def get_unread_message_count(self, agent_id: str) -> int:
        """
        Get the number of unread messages for an agent.
        
        Args:
            agent_id: ID of the agent
            
        Returns:
            Number of unread messages
        """
        if agent_id not in self.agent_inbox:
            return 0
            
        return len([
            msg_id for msg_id in self.agent_inbox[agent_id]
            if msg_id in self.messages and self.messages[msg_id].status == MessageStatus.DELIVERED
        ])
    
    def get_agent_load(self, agent_id: str) -> Dict[str, Any]:
        """
        Get the load statistics for an agent.
        
        Args:
            agent_id: ID of the agent
            
        Returns:
            Dictionary with load statistics
        """
        if agent_id not in self.registered_agents:
            return {
                "registered": False,
                "message_count": 0,
                "inbox_size": 0,
                "unread_count": 0,
                "processing_rate": 0,
            }
            
        # Count messages in the last minute
        now = time.time()
        one_minute_ago = now - 60
        recent_messages = len([
            msg for msg in self.messages.values()
            if msg.receiver_id == agent_id and msg.timestamp > one_minute_ago
        ])
        
        return {
            "registered": True,
            "message_count": len([
                msg for msg in self.messages.values()
                if msg.receiver_id == agent_id or msg.sender_id == agent_id
            ]),
            "inbox_size": len(self.agent_inbox.get(agent_id, [])),
            "unread_count": self.get_unread_message_count(agent_id),
            "processing_rate": recent_messages / 60,  # messages per second
        }
    
    def resend_failed_messages(self) -> List[str]:
        """
        Resend all failed messages.
        
        Returns:
            List of message IDs that were resent
        """
        failed_messages = [
            msg for msg in self.messages.values()
            if msg.status == MessageStatus.FAILED
        ]
        
        resent_ids = []
        
        for message in failed_messages:
            # Create a new message based on the failed one
            new_id = self.send_message(
                sender_id=message.sender_id,
                content=message.content,
                receiver_id=message.receiver_id,
                message_type=message.type,
                priority=message.priority,
                metadata=message.metadata,
                visible_to=message.visible_to,
            )
            
            if new_id:
                resent_ids.append(new_id)
                # Update metadata to link to original message
                self.messages[new_id].metadata["resent_from"] = message.id
                
        return resent_ids
    
    def delete_message(self, message_id: str) -> bool:
        """
        Delete a message from the system.
        
        Args:
            message_id: ID of the message to delete
            
        Returns:
            True if successful, False otherwise
        """
        if message_id not in self.messages:
            logger.warning(f"Message {message_id} not found for deletion")
            return False
            
        # Remove from messages dict
        deleted_message = self.messages.pop(message_id)
        
        # Remove from agent inboxes
        for agent_id, inbox in self.agent_inbox.items():
            if message_id in inbox:
                inbox.remove(message_id)
                
        logger.info(f"Message {message_id} deleted")
        return True
    
    def flush_old_messages(self, older_than: Optional[float] = None) -> int:
        """
        Flush old messages from the system.
        
        Args:
            older_than: Timestamp threshold (default: 7 days ago)
            
        Returns:
            Number of messages flushed
        """
        if older_than is None:
            older_than = time.time() - (7 * 24 * 60 * 60)  # 7 days ago
            
        old_message_ids = [
            msg_id for msg_id, msg in self.messages.items()
            if msg.timestamp < older_than
        ]
        
        # Save old messages before deleting
        if old_message_ids:
            old_messages = {
                msg_id: self.messages[msg_id].to_dict()
                for msg_id in old_message_ids
            }
            
            archive_filename = f"archive_{int(time.time())}.json"
            archive_path = self.save_dir / "archives" / archive_filename
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(archive_path, "w") as f:
                json.dump(old_messages, f)
                
            # Delete old messages
            for msg_id in old_message_ids:
                self.delete_message(msg_id)
                
            logger.info(f"Flushed {len(old_message_ids)} old messages to {archive_path}")
            
        return len(old_message_ids)
    
    def add_agent_group(self, group_name: str, agent_ids: List[str]) -> None:
        """
        Create a named group of agents for easier messaging.
        
        Args:
            group_name: Name of the group
            agent_ids: List of agent IDs in the group
        """
        # Store group in metadata
        self.metadata = getattr(self, "metadata", {})
        self.metadata["agent_groups"] = self.metadata.get("agent_groups", {})
        self.metadata["agent_groups"][group_name] = agent_ids
        
        logger.info(f"Agent group '{group_name}' created with {len(agent_ids)} agents")
    
    def send_to_group(
        self,
        sender_id: str,
        group_name: str,
        content: Union[str, Dict[str, Any], List[Any]],
        message_type: MessageType = MessageType.CHAT,
        priority: MessagePriority = MessagePriority.NORMAL,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """
        Send a message to all agents in a group.
        
        Args:
            sender_id: ID of the agent sending the message
            group_name: Name of the agent group
            content: Content of the message
            message_type: Type of message being sent
            priority: Priority level of the message
            metadata: Additional information about the message
            
        Returns:
            List of message IDs sent
        """
        self.metadata = getattr(self, "metadata", {})
        agent_groups = self.metadata.get("agent_groups", {})
        
        if group_name not in agent_groups:
            logger.warning(f"Agent group '{group_name}' not found")
            return []
            
        group_agents = agent_groups[group_name]
        message_ids = []
        
        # Add group information to metadata
        msg_metadata = metadata or {}
        msg_metadata["group"] = group_name
        
        # Send message to each agent in the group
        for agent_id in group_agents:
            if agent_id != sender_id:  # Don't send to self
                msg_id = self.send_message(
                    sender_id=sender_id,
                    content=content,
                    receiver_id=agent_id,
                    message_type=message_type,
                    priority=priority,
                    metadata=msg_metadata,
                )
                message_ids.append(msg_id)
                
        return message_ids
    
    def set_llm_load_balancing(self, enabled: bool = True) -> None:
        """
        Enable or disable LLM load balancing.
        
        Args:
            enabled: Whether to enable load balancing
        """
        self.llm_load_balancing = enabled
        logger.info(f"LLM load balancing {'enabled' if enabled else 'disabled'}")
    
    def assign_llm_backend(self, agent_id: str, backend_id: str) -> None:
        """
        Assign an LLM backend to an agent.
        
        Args:
            agent_id: ID of the agent
            backend_id: ID of the LLM backend
        """
        self.metadata = getattr(self, "metadata", {})
        self.metadata["llm_backends"] = self.metadata.get("llm_backends", {})
        self.metadata["llm_backends"][agent_id] = backend_id
        
        logger.info(f"Agent {agent_id} assigned to LLM backend {backend_id}")
    
    def get_llm_backend(self, agent_id: str) -> Optional[str]:
        """
        Get the assigned LLM backend for an agent.
        
        Args:
            agent_id: ID of the agent
            
        Returns:
            ID of the assigned LLM backend, or None if not assigned
        """
        self.metadata = getattr(self, "metadata", {})
        backends = self.metadata.get("llm_backends", {})
        return backends.get(agent_id)
    
    def auto_scale_partitions(self) -> None:
        """
        Automatically scale Kafka partitions based on agent load.
        """
        # Get current topic configuration
        topics = self.admin_client.list_topics().topics
        current_partitions = len(topics.get(self.base_topic, {}).partitions)
        
        # Determine if we need more partitions based on agent count
        recommended_partitions = max(3, (len(self.registered_agents) + 9) // 10 * 3)
        
        if recommended_partitions > current_partitions:
            # Create new partitions request
            new_partitions_request = {
                self.base_topic: NewPartitions(recommended_partitions)
            }
            
            try:
                result = self.admin_client.create_partitions(new_partitions_request)
                
                for topic, future in result.items():
                    try:
                        future.result()
                        logger.info(f"Increased partitions for {topic} to {recommended_partitions}")
                    except Exception as e:
                        logger.error(f"Failed to increase partitions for {topic}: {e}")
            
            except Exception as e:
                logger.error(f"Failed to create new partitions: {e}")
    
    def close(self) -> None:
        """
        Close the messaging system, saving state and closing connections.
        """
        # Save message history
        if self.auto_save:
            self.save_message_history()
            
        # Close all consumers
        for agent_id, consumer in self.consumers.items():
            try:
                consumer.close()
                logger.debug(f"Closed consumer for agent {agent_id}")
            except Exception as e:
                logger.error(f"Error closing consumer for agent {agent_id}: {e}")
                
        # Flush the producer to ensure all messages are sent
        self.producer.flush()
        
        logger.info("SwarmsDB closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Example usage
if __name__ == "__main__":
    # Initialize messaging system
    messaging = SwarmsDB(
        base_topic="agent_messaging",
        save_dir="./message_history",
        auto_save=True,
    )
    
    try:
        # Register some agents
        messaging.register_agent("agent1")
        messaging.register_agent("agent2")
        messaging.register_agent("agent3")
        
        # Send messages between agents
        msg_id1 = messaging.send_message(
            sender_id="agent1",
            content="Hello, Agent 2!",
            receiver_id="agent2",
        )
        
        msg_id2 = messaging.send_message(
            sender_id="agent2",
            content="Hi, Agent 1! How are you?",
            receiver_id="agent1",
        )
        
        # Broadcast a message
        broadcast_id = messaging.broadcast_message(
            sender_id="agent3",
            content="Important announcement for everyone!",
        )
        
        # Receive messages for agent1
        agent1_messages = messaging.receive_messages("agent1")
        print(f"Agent 1 received {len(agent1_messages)} messages:")
        for msg in agent1_messages:
            print(f"  From: {msg.sender_id}, Content: {msg.content}")
            
        # Create an agent group
        messaging.add_agent_group("team_alpha", ["agent1", "agent2"])
        
        # Send to the group
        messaging.send_to_group(
            sender_id="agent3",
            group_name="team_alpha",
            content="Message for Team Alpha!",
        )
        
        # Get system stats
        stats = messaging.get_stats()
        print(f"System stats: {stats}")
        
    finally:
        # Close the messaging system
        messaging.close()