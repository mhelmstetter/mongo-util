package com.mongodb.custombalancer;

import java.io.File;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.custombalancer.commands.AnalyzeCommand;
import com.mongodb.custombalancer.commands.BalanceCommand;
import com.mongodb.custombalancer.commands.ManualMoveCommand;
import com.mongodb.custombalancer.commands.StatusCommand;

import ch.qos.logback.classic.ClassicConstants;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.PropertiesDefaultProvider;

@Command(name = "customBalancer",
         mixinStandardHelpOptions = true,
         version = "customBalancer 1.0",
         description = "MongoDB custom shard balancer optimized for multi-document transactions",
         defaultValueProvider = PropertiesDefaultProvider.class,
         subcommands = {
             BalanceCommand.class,
             AnalyzeCommand.class,
             ManualMoveCommand.class,
             StatusCommand.class
         })
public class CustomBalancerApp implements Callable<Integer> {

    public static final String CUSTOM_BALANCER_PROPERTIES_FILE = "custom-balancer.properties";

    private static Logger logger = LoggerFactory.getLogger(CustomBalancerApp.class);

    @Option(names = {"-c", "--config"},
            description = "Configuration properties file, default: " + CUSTOM_BALANCER_PROPERTIES_FILE,
            defaultValue = CUSTOM_BALANCER_PROPERTIES_FILE)
    private File configFile;

    @Option(names = {"-s", "--source"},
            description = "Source cluster connection uri (mongos)")
    private String sourceUri;

    @Option(names = {"--includeNamespace"},
            arity = "0..*",
            description = "Include specific namespace(s) (format: db.collection)")
    private String[] includeNamespace;

    @Option(names = {"--dryRun"},
            description = "Dry run only - analyze and log but don't execute migrations")
    private boolean dryRun;

    @Override
    public Integer call() throws Exception {
        CommandLine commandLine = new CommandLine(this);
        String subcommands = String.join(", ", commandLine.getSubcommands().keySet());
        System.out.println("Please specify a sub-command: " + subcommands);
        System.out.println("Use --help to see available options");
        return 1;
    }

    public String getSourceUri() {
        return sourceUri;
    }

    public String[] getIncludeNamespace() {
        return includeNamespace;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public static void main(String[] args) {
        System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, "custombalancer_logback.xml");

        try {
            // First pass: parse to extract config file
            CommandLine tempCmd = new CommandLine(new CustomBalancerApp());
            ParseResult tempResult = tempCmd.parseArgs(args);

            CustomBalancerApp app = new CustomBalancerApp();

            File defaultsFile = app.configFile;
            if (tempResult.hasMatchedOption("--config") || tempResult.hasMatchedOption("-c")) {
                defaultsFile = tempResult.matchedOption("--config") != null
                    ? new File(tempResult.matchedOption("--config").getValue().toString())
                    : new File(tempResult.matchedOption("-c").getValue().toString());
            }

            // Second pass: parse with defaults loaded from properties file
            CommandLine cmd = new CommandLine(app);
            if (defaultsFile.exists()) {
                cmd.setDefaultValueProvider(new PropertiesDefaultProvider(defaultsFile));
                logger.info("Loaded configuration from: {}", defaultsFile.getAbsolutePath());
            } else {
                logger.info("Configuration file not found: {}, using command-line arguments only",
                    defaultsFile.getAbsolutePath());
            }

            ParseResult parseResult = cmd.parseArgs(args);

            if (!CommandLine.printHelpIfRequested(parseResult)) {
                int exitCode = cmd.execute(args);
                System.exit(exitCode);
            }
        } catch (ParameterException ex) {
            System.err.println(ex.getMessage());
            ex.getCommandLine().usage(System.err);
            System.exit(1);
        } catch (Exception e) {
            logger.error("Fatal error", e);
            System.exit(1);
        }
    }
}