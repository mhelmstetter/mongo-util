package com.mongodb.dataloader;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "dataloader",
         mixinStandardHelpOptions = true,
         version = "1.0",
         description = "MongoDB test data loader and analysis tools",
         subcommands = {
             LoadCommand.class,
             DataSizeCommand.class
         })
public class TestDataLoader {

    public static void main(String[] args) {
        // Check if args are empty or first arg looks like an option (starts with --)
        // If so, default to "load" subcommand for backward compatibility
        // BUT: Don't prepend "load" if user is asking for help or version
        boolean isHelpOrVersion = args.length > 0 &&
            (args[0].equals("--help") || args[0].equals("-h") ||
             args[0].equals("--version") || args[0].equals("-V"));

        if (!isHelpOrVersion && (args.length == 0 || args[0].startsWith("--"))) {
            // Prepend "load" to make it the default subcommand
            String[] newArgs = new String[args.length + 1];
            newArgs[0] = "load";
            System.arraycopy(args, 0, newArgs, 1, args.length);
            args = newArgs;
        }

        int exitCode = new CommandLine(new TestDataLoader()).execute(args);
        System.exit(exitCode);
    }
}
