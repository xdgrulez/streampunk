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
        List<String> fieldStringListAllButLast = new ArrayList<String>();
        String fieldStringListLast = null;
        var fieldStringLengthInt = fieldStrings.length;
        if (fieldStringLengthInt > 0) {
            var fieldStringList = Arrays.asList(fieldStrings);
            fieldStringListAllButLast = fieldStringList.subList(0, fieldStringLengthInt - 1);
            fieldStringListLast = fieldStringList.get(fieldStringLengthInt - 1);
        }
        //
        var descriptor = dynamicMessage.getDescriptorForType();
        for (String fieldString: fieldStringListAllButLast) {
            dynamicMessage = (DynamicMessage) dynamicMessage.getField(descriptor.findFieldByName(fieldString));
            descriptor = dynamicMessage.getDescriptorForType();
        }
        //
        return (String) dynamicMessage.getField(descriptor.findFieldByName(fieldStringListLast));
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
        System.out.println(promptString);
        var console = System.console();
        if (console != null) {
            // Read a line from the user input. The cursor blinks after the specified input.
            var name = console.readLine("Name: ");
            System.out.println("Name entered: " + name);
        }//        try {
//            int readInt = System.in.read();
//            if (readInt == 'n') {
//                return false;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        return true;
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
}
