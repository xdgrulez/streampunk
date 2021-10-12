package org.xdgrulez.streampunk.addon;

import org.xdgrulez.streampunk.exception.IORuntimeException;
import org.xdgrulez.streampunk.helper.Helpers;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.*;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.stream.Collectors;

public class Schemas {
    public static int createSchema(String clusterString, String subjectString, String schemaString) {
        int returnInt = -1;
        //
        var properties = Helpers.loadProperties(String.format("./clusters/%s.properties", clusterString));
        //
        try {
            var schemaRegistryUrlString = (String) properties.get("schema.registry.url");
            var urlString = schemaRegistryUrlString + "/subjects/" + subjectString + "/versions";
            var url = new URL(urlString);
            var urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod("POST");
            urlConnection.setRequestProperty("Content-Type", "application/vnd.schemaregistry.v1+json");
            urlConnection.setDoOutput(true);
            //
            if ("USER_INFO".equals(properties.get("basic.auth.credentials.source"))) {
                var userInfoString = (String) properties.get("basic.auth.user.info");
                String basicAuthString = "Basic " + new String(Base64.getEncoder().encode(userInfoString.getBytes()));
                urlConnection.setRequestProperty("Authorization", basicAuthString);
            }
            //
            var nestedSchemaString = "{ \"schema\": \"" + Helpers.escapeJson(schemaString) + "\"}";
            var outputStream = urlConnection.getOutputStream();
            outputStream.write(nestedSchemaString.getBytes("UTF-8"));
            outputStream.close();
            //
            var responseCodeInt = urlConnection.getResponseCode();
            if (responseCodeInt >= 400) {
                var responseInputStream = urlConnection.getErrorStream();
                var responseString = new BufferedReader(new InputStreamReader(responseInputStream, StandardCharsets.UTF_8))
                        .lines()
                        .collect(Collectors.joining("\n"));
                System.out.println(responseString);
                //
                return -1;
            }
            var responseInputStream = urlConnection.getInputStream();
            var responseString = new BufferedReader(new InputStreamReader(responseInputStream, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));
            //
            JsonObject jsonObject = JsonParser.parseString(responseString).getAsJsonObject();
            returnInt = jsonObject.getAsJsonPrimitive("id").getAsInt();
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        //
        return returnInt;
    }

    public static JsonObject getSchema(String clusterString, String subjectString, Integer schemaIdInt) {
        JsonObject returnJsonObject = null;
        //
        try {
            var properties = Helpers.loadProperties(String.format("./clusters/%s.properties", clusterString));
            //
            var schemaRegistryUrlString = (String) properties.get("schema.registry.url");
            String urlString = schemaRegistryUrlString;
            if (subjectString != null) {
                urlString += "/subjects/" + subjectString + "/versions/latest";
            } else {
                urlString += "/schemas/ids/" + schemaIdInt;
            }
            var url = new URL(urlString);
            var urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod("GET");
            urlConnection.setRequestProperty("Content-Type", "application/vnd.schemaregistry.v1+json");
            //
            if ("USER_INFO".equals(properties.get("basic.auth.credentials.source"))) {
                var userInfoString = (String) properties.get("basic.auth.user.info");
                String basicAuthString = "Basic " + new String(Base64.getEncoder().encode(userInfoString.getBytes()));
                urlConnection.setRequestProperty("Authorization", basicAuthString);
            }
            //
            var responseCodeInt = urlConnection.getResponseCode();
            if (responseCodeInt >= 400) {
                var responseInputStream = urlConnection.getErrorStream();
                var responseString = new BufferedReader(new InputStreamReader(responseInputStream, StandardCharsets.UTF_8))
                        .lines()
                        .collect(Collectors.joining("\n"));
                System.out.println(responseString);
                //
                return null;
            }
            var responseInputStream = urlConnection.getInputStream();
            var responseString = new BufferedReader(new InputStreamReader(responseInputStream, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));
            //
            returnJsonObject = JsonParser.parseString(responseString).getAsJsonObject();
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        //
        return returnJsonObject;
    }

    public static JsonObject getSchema(String clusterString, String subjectString) {
        return getSchema(clusterString, subjectString, null);
    }

    public static String getSchemaString(String clusterString, String subjectString) {
        var jsonObject = getSchema(clusterString, subjectString, null);
        return jsonObject.getAsJsonPrimitive("schema").getAsString();
    }

    public static int getSchemaId(String clusterString, String subjectString) {
        var jsonObject = getSchema(clusterString, subjectString, null);
        return jsonObject.getAsJsonPrimitive("id").getAsInt();
    }

    public static String getSchemaStringById(String clusterString, int schemaIdInt) {
        var jsonObject = getSchema(clusterString, null, schemaIdInt);
        return jsonObject.getAsJsonPrimitive("schema").getAsString();
    }

    public static JsonObject getSchemaById(String clusterString, int schemaIdInt) {
        return getSchema(clusterString, null, schemaIdInt);
    }

    public static void deleteSchema(String clusterString,
                                    String subjectString,
                                    boolean hardBoolean) {
        try {
            var properties = Helpers.loadProperties(String.format("./clusters/%s.properties", clusterString));
            //
            var schemaRegistryUrlString = (String) properties.get("schema.registry.url");
            var urlString = schemaRegistryUrlString + "/subjects/" + subjectString;
            var url = new URL(urlString);
            var urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod("DELETE");
            //
            if ("USER_INFO".equals(properties.get("basic.auth.credentials.source"))) {
                var userInfoString = (String) properties.get("basic.auth.user.info");
                String basicAuthString = "Basic " + new String(Base64.getEncoder().encode(userInfoString.getBytes()));
                urlConnection.setRequestProperty("Authorization", basicAuthString);
            }
            //
            var responseCodeInt = urlConnection.getResponseCode();
            if (responseCodeInt >= 400) {
                var responseInputStream = urlConnection.getErrorStream();
                var responseString = new BufferedReader(new InputStreamReader(responseInputStream, StandardCharsets.UTF_8))
                        .lines()
                        .collect(Collectors.joining("\n"));
                System.out.println(responseString);
                //
                return;
            }
            //
            var responseInputStream = urlConnection.getInputStream();
            var responseString = new BufferedReader(new InputStreamReader(responseInputStream, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));
            urlConnection.disconnect();
            System.out.println(responseString);
            //
            if (hardBoolean) {
                urlString = schemaRegistryUrlString + "/subjects/" + subjectString + "?permanent=true";
                url = new URL(urlString);
                urlConnection = (HttpURLConnection) url.openConnection();
                urlConnection.setRequestMethod("DELETE");
                if ("USER_INFO".equals(properties.get("basic.auth.credentials.source"))) {
                    var userInfoString = (String) properties.get("basic.auth.user.info");
                    String basicAuthString = "Basic " + new String(Base64.getEncoder().encode(userInfoString.getBytes()));
                    urlConnection.setRequestProperty("Authorization", basicAuthString);
                }
                //
                responseCodeInt = urlConnection.getResponseCode();
                if (responseCodeInt >= 400) {
                    responseInputStream = urlConnection.getErrorStream();
                    responseString = new BufferedReader(new InputStreamReader(responseInputStream, StandardCharsets.UTF_8))
                            .lines()
                            .collect(Collectors.joining("\n"));
                    System.out.println(responseString);
                    //
                    return;
                }
                //
                responseInputStream = urlConnection.getInputStream();
                responseString = new BufferedReader(new InputStreamReader(responseInputStream, StandardCharsets.UTF_8))
                        .lines()
                        .collect(Collectors.joining("\n"));
                urlConnection.disconnect();
                System.out.println(responseString);
            }
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
    }

    public static GenericRecord jsonStringToGenericRecord(String jsonString, String schemaString) {
        // Parse the schema string into a Schema object
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);
        //
        return jsonStringToGenericRecord(jsonString, schema);
    }

    public static GenericRecord jsonStringToGenericRecord(String jsonString, Schema schema) {
        GenericRecord returnGenericRecord = null;
        //
        try {
            // Convert the JSON string to GenericRecord
            GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);

            returnGenericRecord = (GenericRecord) reader.read(null,
                    DecoderFactory.get().jsonDecoder(schema, jsonString));
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        //
        return returnGenericRecord;
    }

    public static String loadSchema(String... schemaFileStrings) {
        var currentPathString = Paths.get(".").toAbsolutePath().normalize().toString();
        String schemaListString = "";
        for (var schemaFileString : schemaFileStrings) {
            if (!schemaListString.isEmpty()) {
                schemaListString += ",\n";
            }
            //
            var filePathString = Paths.get(currentPathString, schemaFileString).toString();
            String schemaString = null;
            try {
                schemaString = Files.readString(Path.of(filePathString), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
            schemaListString += schemaString;
        }
        if (schemaFileStrings.length > 1) {
            schemaListString = "[" + schemaListString + "]";
        }
        return schemaListString;
    }

    public static int getSchemaIdFromBytes(byte[] bytes) {
        var magicByte = bytes[0];
        //
        var schemaIdBytes = new byte[4];
        System.arraycopy(bytes, 1, schemaIdBytes, 0, 4);
        //
        return new BigInteger(schemaIdBytes).intValue();
    }

    public static byte[] getDataBytesFromBytes(byte[] bytes) {
        var dataBytesInt = bytes.length - 5;
        var dataBytes = new byte[dataBytesInt];
        System.arraycopy(bytes, 5, dataBytes, 0, dataBytesInt);
        //
        return dataBytes;
    }

    public static GenericRecord avroBytesToGenericRecord(byte[] avroBytes, Schema schema) {
        var datumReader = new GenericDatumReader<GenericRecord>(schema);
        var byteArrayInputStream = new ByteArrayInputStream(avroBytes);
        byteArrayInputStream.reset();
        var binaryDecoder =
                new DecoderFactory().binaryDecoder(byteArrayInputStream, null);
        GenericRecord genericRecord = null;
        try {
            genericRecord = datumReader.read(null, binaryDecoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return genericRecord;
    }

    public static String genericRecordToString(GenericRecord genericRecord, Schema schema) {
        var byteArrayOutputStream = new ByteArrayOutputStream();
        var genericDatumWriter = new GenericDatumWriter<>(schema);
        BinaryEncoder encoder =
                EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        try {
            genericDatumWriter.write(genericRecord, encoder);
            encoder.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byteArrayOutputStream.toString();
    }

    public static JsonObject genericRecordToJsonObject(GenericRecord genericRecord, Schema schema) {
        var jsonString = genericRecordToString(genericRecord, schema);
        return JsonParser.parseString(jsonString).getAsJsonObject();
    }

    public static JsonObject avroBytesToJsonObject(byte[] avroBytes, Schema schema) {
        var genericRecord = avroBytesToGenericRecord(avroBytes, schema);
        return genericRecordToJsonObject(genericRecord, schema);
    }

    public static byte[] genericRecordToAvroBytes(GenericRecord genericRecord, Schema schema) {
        var genericDatumWriter = new GenericDatumWriter<>(schema);
        var byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.reset();
        var binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
        try {
            genericDatumWriter.write(genericRecord, binaryEncoder);
            binaryEncoder.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byteArrayOutputStream.toByteArray();
    }

    public static byte[] schemaIdAndDataBytesToValueBytes(int schemaIdInt, byte[] dataBytes) {
        var schemaIdBytes = ByteBuffer.allocate(4).putInt(schemaIdInt).array();
        var dataBytesLengthInt = dataBytes.length;
        //
        byte[] valueBytes = new byte[dataBytesLengthInt + 5];
        valueBytes[0] = 0; // https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html
        System.arraycopy(schemaIdBytes, 0, valueBytes, 1, 4);
        System.arraycopy(dataBytes, 0, valueBytes, 5, dataBytesLengthInt);
        //
        return valueBytes;
    }

    private static HashMap<String, String> getConfig(String clusterString, boolean specificBoolean) {
        var configStringStringMap = new HashMap<String, String>();
        var properties =
                Helpers.loadProperties(String.format("./clusters/%s.properties", clusterString));
        properties.entrySet().forEach(
                entry -> configStringStringMap.put((String) entry.getKey(), (String) entry.getValue()));
        if (specificBoolean) {
            configStringStringMap.put("specific.avro.reader", "true");
        }
        return configStringStringMap;
    }

    public static KafkaAvroDeserializer getKafkaAvroDeserializer(String clusterString, boolean specificBoolean) {
        var kafkaAvroDeserializer = new KafkaAvroDeserializer();
        var configStringStringMap = getConfig(clusterString, specificBoolean);
        kafkaAvroDeserializer.configure(configStringStringMap, false);
        return kafkaAvroDeserializer;
    }

    public static KafkaAvroSerializer getKafkaAvroSerializer(String clusterString, boolean specificBoolean) {
        var kafkaAvroSerializer = new KafkaAvroSerializer();
        var configStringStringMap = getConfig(clusterString, specificBoolean);
        kafkaAvroSerializer.configure(configStringStringMap, false);
        return kafkaAvroSerializer;
    }
}
