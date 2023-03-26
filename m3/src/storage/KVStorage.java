package storage;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class KVStorage {
    private static Logger logger = Logger.getRootLogger();

    private String dataDirectory;
    private String dataProperties;
    private String propertiesPath;

    private Map<String, String> db;

    public KVStorage(String dataDirectory, String dataProperties) {
        this.dataDirectory = dataDirectory;
        this.dataProperties = dataProperties;
        this.propertiesPath = dataDirectory + "/" + dataProperties;

        db = Collections.synchronizedMap(new HashMap<String, String>());

        try {
            File dataDir = new File(dataDirectory);

            if (!dataDir.exists() && !dataDir.mkdir()) {
                logger.error("Failed to create data directory");
            }

            File dataFile = new File(propertiesPath);

            if (!dataFile.exists() && !dataFile.createNewFile()) {
                logger.error("Failed to create properties file");
            }

            Properties dataProps = new Properties();
            FileInputStream fs = new FileInputStream(propertiesPath);
            dataProps.load(fs);
            fs.close();

            for (String key: dataProps.stringPropertyNames()) {
                db.put(key, dataProps.getProperty(key));
            }
        } catch (Exception e) {
           logger.error("Storage initialization failed", e);
        }
    }

    private synchronized void persist() {
        Properties dataProps = new Properties();

        dataProps.putAll(db);
        
        try {
            FileOutputStream fs = new FileOutputStream(this.propertiesPath);
            dataProps.store(fs, null);
            fs.close();
        } catch (Exception e) {
            logger.error("Failed to write database to disk", e);
        }
        
    }

    private synchronized void loadPersistedData() {
        Properties dataProps = new Properties();
        try {
            FileInputStream fs = new FileInputStream(this.propertiesPath);
			dataProps.load(fs);
            fs.close();
            for (String key: dataProps.stringPropertyNames()) {
                db.put(key, dataProps.getProperty(key));
            }
		} catch (Exception e) {
			logger.error("Failed to load data from disk", e);
		}
    }

    public synchronized String get(String key) {
        String value;

        try {
            if(db.get(key) == null){
                loadPersistedData();
            }
            value = db.get(key);
        } catch (Exception e) {
            logger.error("Failed to get key-value pair from database", e);
            value = null;
        }

        return value;
    }

    public synchronized boolean put(String key, String value) {
        try {
            db.put(key, value);
            persist();
            return true;
        } catch (Exception e) {
            logger.error("Failed to put key-value pair into database", e);
            return false;
        }
    }

    public synchronized boolean delete(String key) {
        try {
            db.remove(key);
            persist();
            return true;
        } catch (Exception e) {
            logger.error("Failed to delete key-value pair from database", e);
            return false;
        }
    }

    public synchronized boolean exists(String key) {
        return db.containsKey(key);
    }

    public synchronized Map<String, String> getDatabase() { return db; }

    public synchronized void clear() {
        db.clear();
        persist();
        logger.info("Database wiped");
    }

    public synchronized List<Entry<String, String>> findEntriesInRange(String low, String high) {
		
		List<Entry<String, String>> ret = new ArrayList<>();
		for(Entry<String, String> entry: db.entrySet()){
            if(isKeyInRange(entry.getKey(), low, high)){
                ret.add(entry);
            }
        }
		return ret;
	}


    public synchronized void removeEntriesBetweenrange(String low, String high){
        List<Entry<String, String>> toBeRemoved = this.findEntriesInRange(low, high);
        for(Entry<String, String> entry: toBeRemoved){
            db.remove(entry.getKey());
        }
        persist();
    }

    public boolean isKeyInRange(String key, String low, String high){
		if(low.compareTo(high) < 0) //if hash range does not wrap around
			return (key.compareTo(low) > 0) &&  (key.compareTo(high) <= 0);
		return (key.compareTo(low) < 0) ||  (key.compareTo(high) > 0); //hash range does wrap around
    }
}
