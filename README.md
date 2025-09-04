# IBC Explorer Backend

## Overview
IBC Explorer Backend is a service that provides fundamental data display for the cross-chain ecosystem, focusing on querying information within the IBC cross-chain network.

## License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.

## Getting Started

### Building from Source

First, build the application:

```bash
make build
```

### Running the Application

Start the application with default configuration:

```bash
./ibc-explorer-backend start
```

Or start with a custom configuration file:

```bash
./ibc-explorer-backend start test -c configFilePath
```

### Running with Docker

1. Build the Docker image:

```bash
docker build -t ibc-explorer-backend .
```

2. Run the container:

```bash
docker run --name ibc-explorer-backend -p 8080:8080 ibc-explorer-backend
```

## Configuration

### Environment Variables
- CONFIG_FILE_PATH: `option` `string` Path to the configuration file
