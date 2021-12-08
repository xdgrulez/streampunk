package org.xdgrulez.streampunk.helper;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.xdgrulez.streampunk.exception.IORuntimeException;
import org.xdgrulez.streampunk.exception.InvalidProtocolBufferRuntimeException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

public class Helpers {
    public static Map<Integer, Long> getZeroOffsets(int partitionsInt) {
        var zeroOffsets = new HashMap<Integer, Long>();
        for (var i = 0; i < partitionsInt; i++) {
            zeroOffsets.put(i, 0L);
        }
        return zeroOffsets;
    }

    public static Map<Integer, Long> getOffsets(int partitionsInt, long offsetLong) {
        var offsets = new HashMap<Integer, Long>();
        for (var i = 0; i < partitionsInt; i++) {
            offsets.put(i, offsetLong);
        }
        return offsets;
    }

    public static String getProtobufField(DynamicMessage dynamicMessage, String... fieldStrings) {
        List<String> allButLastFieldStringList = new ArrayList<>();
        String lastFieldString = null;
        var fieldStringLengthInt = fieldStrings.length;
        if (fieldStringLengthInt > 0) {
            var fieldStringList = Arrays.asList(fieldStrings);
            allButLastFieldStringList = fieldStringList.subList(0, fieldStringLengthInt - 1);
            lastFieldString = fieldStringList.get(fieldStringLengthInt - 1);
        }
        //
        var descriptor = dynamicMessage.getDescriptorForType();
        for (String fieldString : allButLastFieldStringList) {
            var fieldDescriptor = descriptor.findFieldByName(fieldString);
            if (fieldDescriptor == null) {
                return null;
            }
            dynamicMessage = (DynamicMessage) dynamicMessage.getField(fieldDescriptor);
            descriptor = dynamicMessage.getDescriptorForType();
        }
        //
        var lastFieldDescriptor = descriptor.findFieldByName(lastFieldString);
        return lastFieldDescriptor == null ? null : (String) dynamicMessage.getField(lastFieldDescriptor);
    }

    // graalpython gives us Integers where we expect Longs... workaround...
    public static long getLong(Object object) {
        if (object.getClass().getName().equals("java.lang.Integer")) {
            return (long) (int) object;
        } else {
            return (long) object;
        }
    }

    public static boolean yesNoPrompt(String promptString) {
        return yesNoPrompt(promptString, true);
    }

    public static boolean yesNoPrompt(String promptString, boolean defaultBoolean) {
//        System.out.println(promptString);
        var console = System.console();
        if (console != null) {
            // Read a line from the user input. The cursor blinks after the specified input.
            var lineString = console.readLine(promptString + " ");
            if (lineString.equalsIgnoreCase("y")) {
                return true;
            } else if (lineString.equalsIgnoreCase("n")) {
                return false;
            }
        }
        return defaultBoolean;
        //
//        try {
//            int readInt = System.in.read();
//            if (readInt == 'n') {
//                return false;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return true;
    }

    public static Properties loadProperties(String... propertiesFileStrings) {
        var properties = new Properties();
        //
        var currentPathString = Paths.get(".").toAbsolutePath().normalize().toString();
        for (var propertiesFileString : propertiesFileStrings) {
            var filePathString = Paths.get(currentPathString, propertiesFileString).toString();
            var file = new File(filePathString);
            try {
                var inputStream = new FileInputStream(file);
                properties.load(inputStream);
                inputStream.close();
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        }
        //
        return properties;
    }

    public static String escapeJson(String jsonString) {
        return jsonString
                .replace("\"", "\\\"")
                .replace(" ", "")
                .replace("\r", "")
                .replace("\n", "")
                .replace("\t", "");
    }

    public static List<String> listFiles(String pathString, String patternString) {
        var dirFile = new File(pathString);
        var files = dirFile.listFiles((dirFile1, fileString) -> fileString.matches(patternString));
        var fileStringList = new ArrayList<String>();
        if (files != null) {
            for (File file : files) {
                fileStringList.add(file.getName());
            }
        }
        return fileStringList;
    }

    public static GenericRecord jsonStringToGenericRecord(String jsonString, String schemaString) {
        // Parse the schema string into a Schema object
        var parser = new Schema.Parser();
        var schema = parser.parse(schemaString);

        // Convert the JSON string to GenericRecord
        var reader = new GenericDatumReader<Object>(schema);

        GenericRecord genericRecord = null;
        try {
            genericRecord = (GenericRecord) reader.read(null,
                    DecoderFactory.get().jsonDecoder(schema, jsonString));
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }

        return genericRecord;
    }

    public static DynamicMessage jsonStringToDynamicMessage(String jsonString, String schemaString) {
        var protobufSchema = new ProtobufSchema(schemaString);
        DynamicMessage dynamicMessage = null;
        try {
            dynamicMessage = (DynamicMessage) ProtobufSchemaUtils.toObject(jsonString, protobufSchema);
        } catch (InvalidProtocolBufferException e) {
            throw new InvalidProtocolBufferRuntimeException(e);
        }
        return dynamicMessage;
    }

    public static String epochToTs(long epochLong) {
        var formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        formatter.setTimeZone(TimeZone.getTimeZone("CET"));
        //
        var tsString = formatter.format(epochLong);
        //
        return tsString;
    }
}
