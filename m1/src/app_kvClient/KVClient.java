package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class KVClient implements IKVClient {
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;
    private boolean stop = false;

    private String serverAddress;
    private int serverPort;

    private KVStore kvStore;

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        // TODO Auto-generated method stub
        kvStore = new KVStore(hostname, port);
        kvStore.connect();
        System.out.println(PROMPT + "Connected to server");
    }

    @Override
    public KVCommInterface getStore() {
        // TODO Auto-generated method stub
        return kvStore;
    }

    private void disconnect() {
        if(kvStore != null) {
            kvStore.disconnect();
            kvStore = null;
            System.out.println(PROMPT + "Disconnected from server");
        }
    }

    public void run() {
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("connect")) {
            if (tokens.length == 3) {
                try {
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                } catch(NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                } catch (IOException e) {
                    printError("Could not establish connection!");
                } catch (Exception e) {
                    printError("Unexpected exception: " + e.getMessage());
                }
            } else {
                printError("Invalid number of parameters!");
            }
        }

        else if (tokens[0].equals("disconnect")) {
            disconnect();
        }

        else if (tokens[0].equals("put")) {
            if(tokens.length >= 2) {
                if (kvStore != null && kvStore.isRunning()) {
                    String key = tokens[1];
                    StringBuilder value = new StringBuilder();
                    for(int i = 2; i < tokens.length; i++) {
                        value.append(tokens[i]);
                        if (i != tokens.length -1 ) {
                            value.append(" ");
                        }
                    }
                    try {
                        IKVMessage ret = kvStore.put(key, value.toString());
                        System.out.println("STATUS: " + ret.getStatus().toString());
                        System.out.println("KEY: " + ret.getKey());
                        System.out.println("VALUE: " + ret.getValue());
                    } catch (IOException ioe) {
                        printError("Server not available");
                        disconnect();
                    } catch (Exception e) {
                        printError("Unexpected exception: " + e.getMessage());
                    }
                } else {
                    printError("Not connected to server!");
                }
            } else {
                printError("Invalid input! Usage: put <key> <value>");
            }
        }

        else if (tokens[0].equals("get")) {
            if(tokens.length == 2) {
                if (kvStore != null && kvStore.isRunning()) {
                    String key = tokens[1];
                    try {
                        IKVMessage ret = kvStore.get(key);
                        System.out.println("STATUS: " + ret.getStatus().toString());
                        System.out.println("KEY: " + ret.getKey());
                        System.out.println("VALUE: " + ret.getValue());
                    } catch (IOException ioe) {
                        printError("Server not available");
                        disconnect();
                    } catch (Exception e) {
                        printError("Unexpected exception: " + e.getMessage());
                    }
                } else {
                    printError("Not connected to server!");
                }
            } else {
                printError("Invalid input! Usage: get <key>");
            }
        }

        else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT + "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }
        }

        else if (tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");
        }

        else if (tokens[0].equals("help")) {
            printHelp();
        }

        else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();

        sb.append(PROMPT).append("KV CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");

        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server \n");

        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t\t add or overwrite a key-value pair; deletes the key if the given value is null \n");

        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t retrieve the associated value for the given key \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");

        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private String setLevel(String levelString) {
        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            KVClient client = new KVClient();
            client.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
