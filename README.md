# twitchchat_stream_analytics

## Overview

This data engineering project streams Twitch chat messages, performs sentiment analysis, and stores the processed data in a Kafka topic. The pipeline utilizes Confluent Cloud for Kafka and ClickHouse for data storage. The parsed and sentiment-analyzed data can be visualized using Preset dashboards to observe how the Twitch stream chat evolves over time.

## Solution Architecture
![Solution Architecture](relative/path/to/twitch_solution_architecture.png)

## Pipeline Architecture

The pipeline consists of the following components:

1. **Twitch Chat Streamer**: A Python script (`production.py`) establishes a WebSocket connection with Twitch IRC to receive chat messages. The messages are then parsed and transformed into JSON format before being sent to a Kafka producer.

2. **Kafka Producer**: The Kafka producer is responsible for sending the processed chat messages to a specified Kafka topic. The producer configuration, including authentication details, is provided through a configuration file.

3. **Sentiment Analysis**: The parsed chat messages undergo sentiment analysis using a library called [Transformers from Hugging Face](https://huggingface.co/docs/transformers/index) to extract sentiment-related information. The sentiment results are included in the Kafka message payload.

4. **Configuration Management**: Configuration files (`.yaml` and `.properties`) are used to manage Twitch authentication details, Kafka producer configuration, and other environment-specific parameters.

5. **ClickHouse Integration**: The data is moved to ClickHouse for storage, allowing for efficient querying and analysis. 

6.  **Preset Integration**: Preset dashboards can be connected to ClickHouse to visualize trends and patterns in the Twitch stream chat.

## Getting Started

### Prerequisites

- Python 3.x
- Kafka
- Confluent Cloud account
- ClickHouse database

### Setup

1. Clone the repository:

    ```bash
    git clone https://github.com/yourusername/your-repo.git
    cd your-repo
    ```

2. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

3. Set up environment variables:

    Create a `.env` file with the following:

    ```env
    SERVER='irc.chat.twitch.tv'
    PORT=6667
    NICKNAME=<Your Twitch Nickname>
    TOKEN=<Your Twitch Authentication Token>
    ```

4. Configure `.yaml` and `.properties` files with appropriate values.

5. Run the Twitch Chat Data Engineering Pipeline:

    ```bash
    python stream_chat.py
    ```
    
## Improvements
1. **Airflow Integration**: Integrate Apache Airflow to schedule and orchestrate the data processing pipeline. This allows for better job management, scheduling, and monitoring of tasks.

2. **Consumer-Producer Decoupling**: Consider decoupling the consumer and producer components. Use Kafka to store messages temporarily, allowing a consumer to read from the producer at its own pace. This enhances scalability and handling potential errors in the stream.

3. **Multi-Stream Support**: Build on the pipeline to support processing multiple Twitch streams concurrently. This can be achieved by enhancing the configuration management to handle multiple channels and adapting functions to support multiple chennels.

4. **Dockerization**: Containerize the pipeline components using Docker to simplify deployment and ensure consistent execution environments across different systems.

5. **CI/CD Pipeline Implementation**: Implement a CI/CD pipeline to automate the testing, building, and deployment processes. This ensures that changes to the pipeline can be efficiently validated and deployed, reducing the risk of introducing errors into the production environment.

## Contributors

- [James Hill](https://github.com/jhill00)
- [Clayton Zhang](https://github.com/notyalc)
