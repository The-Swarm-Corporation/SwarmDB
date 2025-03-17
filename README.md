# Swarms DB

[![Join our Discord](https://img.shields.io/badge/Discord-Join%20our%20server-5865F2?style=for-the-badge&logo=discord&logoColor=white)](https://discord.gg/agora-999382051935506503) [![Subscribe on YouTube](https://img.shields.io/badge/YouTube-Subscribe-red?style=for-the-badge&logo=youtube&logoColor=white)](https://www.youtube.com/@kyegomez3242) [![Connect on LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/kye-g-38759a207/) [![Follow on X.com](https://img.shields.io/badge/X.com-Follow-1DA1F2?style=for-the-badge&logo=x&logoColor=white)](https://x.com/kyegomezb)


A production-grade message queue system for agent communication and LLM backend load balancing in large-scale multi-agent systems. This system provides a scalable solution using Apache Kafka for reliable message distribution with comprehensive features for agent communication, message management, and load balancing.

## Features

- **Robust Message Distribution**: Built on Apache Kafka for reliable, scalable message handling
- **Auto-Partitioning**: Kafka topics automatically scale based on agent load
- **Stateful Management**: Comprehensive tracking of message states and history
- **Persistence**: Automatic saving of all interactions to JSON files
- **RESTful API**: Complete FastAPI interface for all messaging operations
- **Authentication & Authorization**: JWT-based secure agent authentication
- **Production-Ready**: Deployment with Gunicorn/Uvicorn for high performance
- **Comprehensive Logging**: Detailed logs using Loguru
- **Containerized**: Docker and Docker Compose setup for easy deployment
- **Message Types**: Support for different message types (chat, commands, function calls)
- **Group Messaging**: Ability to create agent groups for targeted communication
- **Load Balancing**: Built-in LLM backend load balancing capabilities

## System Architecture

The Agent Messaging System is built with the following components:

1. **Kafka Backend**: For reliable message distribution and queuing
2. **SwarmsDB Class**: Core messaging logic and state management
3. **FastAPI Server**: RESTful API for agent interaction
4. **Gunicorn/Uvicorn**: ASGI server for production deployment

## Installation

### Using Docker Compose (Recommended)

```bash
# Clone the repository
git clone https://github.com/yourusername/agent-messaging-system.git
cd agent-messaging-system

# Start the entire stack (Kafka, Zookeeper, API)
docker-compose up -d

# The API will be available at http://localhost:8000
# The Kafka UI will be available at http://localhost:8080
```

### Manual Installation

1. Install Poetry (dependency management):

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

2. Install dependencies:

```bash
poetry install
```

3. Make sure Kafka is running and accessible.

4. Start the API server:

```bash
# For development
./start.sh development

# For production
./start.sh production
```

## Environment Configuration

The system can be configured using environment variables:

```
# API Configuration
API_ENV=production
PORT=8000

# Security
JWT_SECRET=your_secret_key_here
JWT_ALGORITHM=HS256
TOKEN_EXPIRE_MINUTES=1440

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC_PREFIX=agent_messaging_
KAFKA_NUM_PARTITIONS=6
KAFKA_REPLICATION_FACTOR=1

# Message History Configuration
MESSAGE_HISTORY_DIR=/app/message_history
SAVE_INTERVAL_SECONDS=300

# Rate Limiting
RATE_LIMIT_PER_MINUTE=300
```

## API Usage

### Authentication

```bash
# Get access token
curl -X POST http://localhost:8000/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username":"agent1", "password":"password"}'
```

### Register an Agent

```bash
curl -X POST http://localhost:8000/agents/register \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"agent_id":"agent1", "description":"AI Assistant Agent"}'
```

### Send a Message

```bash
curl -X POST http://localhost:8000/messages \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "content": "Hello, can you analyze this data?",
    "receiver_id": "agent2",
    "message_type": "command",
    "priority": 2
  }'
```

### Receive Messages

```bash
curl -X POST http://localhost:8000/agents/receive \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d "max_messages=10&timeout=2.0"
```

### Create an Agent Group

```bash
curl -X POST http://localhost:8000/groups \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "group_name": "analysis_team",
    "agent_ids": ["agent2", "agent3", "agent4"]
  }'
```

### Send to a Group

```bash
curl -X POST http://localhost:8000/groups/message \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "group_name": "analysis_team",
    "content": "Everyone, please review these results.",
    "message_type": "chat"
  }'
```

## Development

### Running Tests

```bash
poetry run pytest
```

### Code Formatting

```bash
poetry run black .
poetry run isort .
```

### Type Checking

```bash
poetry run mypy .
```

## Production Deployment

For production deployment, we recommend using Docker Compose with appropriate environment variables for security.

1. Edit the environment variables in docker-compose.yml
2. Ensure the JWT_SECRET is a secure random string
3. Deploy with docker-compose:

```bash
docker-compose up -d
```

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.