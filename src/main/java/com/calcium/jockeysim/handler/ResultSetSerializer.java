package com.calcium.jockeysim.handler;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.sql.*;

public class ResultSetSerializer extends JsonSerializer<ResultSet> {

    public static class ResultSetSerializerException extends JsonProcessingException{
        private static final long serialVersionUID = -914957626413580734L;

        public ResultSetSerializerException(Throwable cause){
            super(cause);
        }

    }

    @Override
    public Class<ResultSet> handledType(){
        return ResultSet.class;
    }

    @Override
    public void serialize(ResultSet resultSet, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        try {
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int numColumns = rsmd.getColumnCount();
            String[] columnNames = new String[numColumns];
            int[] columnTypes = new int[numColumns];

            for(int i=0; i < columnNames.length; i++){
                columnNames[i] = rsmd.getColumnLabel(i + 1);
                columnTypes[i] = rsmd.getColumnType(i + 1);
            }

            jsonGenerator.writeStartArray();

            while(resultSet.next()){
                boolean b;
                long l;
                double d;

                jsonGenerator.writeStartObject();

                for (int i=0; i<columnNames.length; i++){
                    jsonGenerator.writeFieldName(columnNames[i]);
                    switch(columnTypes[i]){
                        case Types.INTEGER:
                            l = resultSet.getInt(i + 1);
                            if (resultSet.wasNull()){
                                jsonGenerator.writeNull();
                            }else{
                                jsonGenerator.writeNumber(l);
                            }
                            break;
                        case Types.BIGINT:
                            l = resultSet.getLong(i + 1);
                            if (resultSet.wasNull()){
                                jsonGenerator.writeNull();
                            }else{
                                jsonGenerator.writeNumber(l);
                            }
                            break;
                        case Types.DECIMAL:
                        case Types.NUMERIC:
                            jsonGenerator.writeNumber(resultSet.getBigDecimal(i+1));
                            break;
                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                            d = resultSet.getDouble(i + 1);
                            if (resultSet.wasNull()){
                                jsonGenerator.writeNull();
                            }else{
                                jsonGenerator.writeNumber(d);
                            }
                            break;
                        case Types.NVARCHAR:
                        case Types.VARCHAR:
                        case Types.LONGNVARCHAR:
                        case Types.LONGVARCHAR:
                            jsonGenerator.writeString(resultSet.getString(i+1));
                            break;
                        case Types.BOOLEAN:
                        case Types.BIT:
                            b = resultSet.getBoolean(i + 1);
                            if (resultSet.wasNull()){
                                jsonGenerator.writeNull();
                            }else{
                                jsonGenerator.writeBoolean(b);
                            }
                            break;
                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.LONGVARBINARY:
                            jsonGenerator.writeBinary(resultSet.getBytes(i + 1));
                            break;
                        case Types.TINYINT:
                        case Types.SMALLINT:
                            l = resultSet.getShort(i + 1);
                            if (resultSet.wasNull()){
                                jsonGenerator.writeNull();
                            }else{
                                jsonGenerator.writeNumber(l);
                            }
                            break;
                        case Types.DATE:
                            serializerProvider.defaultSerializeDateValue(resultSet.getDate(i+1), jsonGenerator);
                            break;
                        case Types.TIMESTAMP:
                            serializerProvider.defaultSerializeDateValue(resultSet.getTime(i+1), jsonGenerator);
                            break;
                        case Types.BLOB:
                            Blob blob = resultSet.getBlob(i+1);
                            serializerProvider.defaultSerializeValue(blob.getBinaryStream(), jsonGenerator);
                        case Types.CLOB:
                            Clob clob = resultSet.getClob(i+1);
                            serializerProvider.defaultSerializeValue(clob.getCharacterStream(), jsonGenerator);
                        case Types.ARRAY:
                            throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type ARRAY");
                        case Types.STRUCT:
                            throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type STRUCT");
                        case Types.DISTINCT:
                            throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type DISTINCT");
                        case Types.REF:
                            throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type REF");
                        case Types.JAVA_OBJECT:
                        default:
                            serializerProvider.defaultSerializeValue(resultSet.getObject(i +1), jsonGenerator);
                            break;
                    }

                }
                jsonGenerator.writeEndObject();
            }
            jsonGenerator.writeEndArray();
        } catch (SQLException e) {
            throw new ResultSetSerializerException(e);
        }


    }
}
