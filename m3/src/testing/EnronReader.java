package testing;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Objects;

public class EnronReader {
    public static HashMap<String, String> parse(String dataPath) {
        File dataDir = new File(dataPath);

        HashMap<String, String> dataMap = new HashMap<>();

        for (File user : Objects.requireNonNull(dataDir.listFiles())) {
            for (File folder : Objects.requireNonNull(user.listFiles())) {
                for (File email : Objects.requireNonNull(folder.listFiles())) {
                    if (!email.isFile()) {
                        continue;
                    }

                    try {
                        String key = email.getCanonicalPath();

                        String value = new String(Files.readAllBytes(email.toPath()));

                        dataMap.put(key, value);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return dataMap;
    }
}
