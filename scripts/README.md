# MongoDB Utilities - Command Installation Scripts

This directory contains scripts for installing mongo-util commands as standalone executables in your system PATH.

## Installation

Run the installation script from the mongo-util project root:

```bash
./scripts/install-commands.sh
```

This will:
1. Check for Java 17+ installation
2. Build the project if needed (mvn clean package)
3. Install command wrappers to `~/.local/bin`
4. Verify the installation

## Installed Commands

### dataloader

Multi-threaded MongoDB test data loader for generating test data with various data types.

**Usage:**
```bash
dataloader --uri mongodb://localhost:27017 --dataSize 100MB [options]
```

**Options:**
- `--uri` (required): MongoDB connection URI
- `--dataSize` (required): Data size per collection (or per shard if sharded). Format: 1MB, 1GB, 1TB
- `--numCollections`: Number of collections to create (default: 1)
- `--fields`: Number of fields per document (default: 10)
- `--threads`: Number of loader threads (default: 4)
- `--batchSize`: Documents per insertMany batch (default: 1024)
- `--database`: Database name (default: testdb)
- `--collectionPrefix`: Collection name prefix (default: testcoll)

**Examples:**
```bash
# Load 100MB of test data
dataloader --uri mongodb://localhost:27017 --dataSize 100MB

# Load data into sharded cluster with 5 collections
dataloader --uri mongodb://mongos:27017 --dataSize 1GB --numCollections 5

# Customize fields, threads, and batch size
dataloader --uri mongodb://localhost:27017 --dataSize 500MB \
           --fields 20 --threads 8 --batchSize 2048

# Different data sizes for different collections
dataloader --uri mongodb://localhost:27017 \
           --dataSize 100MB,500MB,1GB --numCollections 3
```

## Adding New Commands

To add a new command to the installer:

1. **Create the main class** in `src/main/java/com/mongodb/<package>/`
   - Add picocli annotations for command-line parsing
   - Implement `Callable<Integer>` interface
   - Add a `main()` method

2. **Update install-commands.sh** by adding a new `create_command` call:

```bash
create_command "your-command-name" \
    "com.mongodb.package.YourMainClass" \
    "Description of your command"
```

3. **Update the installation output** in the script to include help text for the new command

4. **Rebuild and reinstall:**
```bash
mvn clean package -DskipTests
./scripts/install-commands.sh
```

### Example Template

```bash
# In install-commands.sh, add after the existing create_command calls:

create_command "my-new-command" \
    "com.mongodb.mypackage.MyNewCommand" \
    "Description of what my command does"
```

Then update the "Available Commands" section at the end of the script to document the new command.

## Updating

To update installed commands after code changes:

1. Make your code changes
2. Build the project: `mvn clean package -DskipTests`
3. Re-run the installer: `./scripts/install-commands.sh`

The installer will detect existing installations and update them.

## Uninstalling

To remove installed commands:

```bash
rm ~/.local/bin/dataloader
# Add more commands here as they are added
```

## PATH Configuration

The installer checks if `~/.local/bin` is in your PATH. If not, add it to your shell profile:

**For zsh (macOS default):**
```bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

**For bash:**
```bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

## Troubleshooting

### "java command not found"
Install Java 17 or later:
- **macOS**: `brew install openjdk@17`
- **Ubuntu**: `sudo apt install openjdk-17-jdk`

### "JAR file not found"
The installer will automatically build the project. If this fails:
```bash
cd /path/to/mongo-util
mvn clean package -DskipTests
```

### Command not found after installation
Check if `~/.local/bin` is in your PATH:
```bash
echo $PATH | grep -q "$HOME/.local/bin" && echo "In PATH" || echo "Not in PATH"
```

If not in PATH, see "PATH Configuration" above.

## How It Works

The installer:
1. Creates wrapper scripts in `~/.local/bin` (e.g., `dataloader`)
2. Each wrapper points to the mongo-util.jar in your project directory
3. When you run a command, it executes: `java -cp mongo-util.jar com.mongodb.package.MainClass "$@"`
4. The JAR stays in your project directory - no copying or duplication

This approach:
- Avoids JAR duplication
- Updates automatically when you rebuild
- Allows easy development and testing
- Provides clean command-line interface
