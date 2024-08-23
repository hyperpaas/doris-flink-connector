package org.apache.doris.flink.sink.writer.serializer;

import static org.apache.doris.flink.sink.writer.LoadConstants.ARROW;
import static org.apache.doris.flink.sink.writer.LoadConstants.CSV;
import static org.apache.doris.flink.sink.writer.LoadConstants.DORIS_DELETE_SIGN;
import static org.apache.doris.flink.sink.writer.LoadConstants.JSON;
import static org.apache.doris.flink.sink.writer.LoadConstants.NULL_VALUE;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.doris.flink.deserialization.converter.DorisRowConverter;
import org.apache.doris.flink.sink.EscapeHandler;
import org.apache.doris.flink.sink.batch.RowDataWithMeta;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.serializers.ArrowSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowDataWithMetaSerializer implements DorisRecordSerializer<RowDataWithMeta> {

  private static final Logger LOG = LoggerFactory.getLogger(RowDataWithMetaSerializer.class);
  String type;
  private ObjectMapper objectMapper;
  private final String fieldDelimiter;
  private final boolean enableDelete;
  private final int arrowBatchCnt = 1000;
  private int arrowWriteCnt = 0;
  private ArrowSerializer arrowSerializer;
  ByteArrayOutputStream outputStream;

  private RowDataWithMetaSerializer(String type, String fieldDelimiter, boolean enableDelete) {
    this.type = type;
    this.fieldDelimiter = fieldDelimiter;
    this.enableDelete = enableDelete;
    if (JSON.equals(type)) {
      objectMapper = new ObjectMapper();
    }
  }

  @Override
  public DorisRecord serialize(RowDataWithMeta meta) throws IOException {
    RowData record = meta.getRowData();
    String[] fieldNames = meta.getFieldNames();
    DataType[] dataTypes = meta.getDataTypes();
    DorisRowConverter rowConverter = new DorisRowConverter().setExternalConverter(dataTypes);
    int maxIndex = Math.min(record.getArity(), fieldNames.length);
    String valString;
    if (JSON.equals(type)) {
      valString = buildJsonString(record, maxIndex, rowConverter, fieldNames);
    } else if (CSV.equals(type)) {
      valString = buildCSVString(record, maxIndex, rowConverter);
    } else if (ARROW.equals(type)) {
      arrowSerializer = meta.getArrowSerializer();
      outputStream = meta.getOutputStream();
      if (arrowSerializer == null || outputStream == null) {
        throw new IllegalArgumentException("rowDataWithMeta arrowSerializer is null!");
      }
      arrowWriteCnt += 1;
      arrowSerializer.write(record);
      if (arrowWriteCnt < arrowBatchCnt) {
        return DorisRecord.empty;
      }
      return arrowToDorisRecord();
    } else {
      throw new IllegalArgumentException("The type " + type + " is not supported!");
    }
    LOG.info("rowDataWithMeta serialize success, valString:{}", valString);
    return DorisRecord.of(
        meta.getDatabase(), meta.getTable(), valString.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public DorisRecord flush() {
    if (JSON.equals(type) || CSV.equals(type)) {
      return DorisRecord.empty;
    } else if (ARROW.equals(type)) {
      return arrowToDorisRecord();
    } else {
      throw new IllegalArgumentException("The type " + type + " is not supported!");
    }
  }

  @Override
  public void close() throws Exception {
    if (ARROW.equals(type) && arrowSerializer != null) {
      arrowSerializer.close();
    }
  }

  public DorisRecord arrowToDorisRecord() {
    if (arrowWriteCnt == 0) {
      return DorisRecord.empty;
    }
    arrowWriteCnt = 0;
    try {
      arrowSerializer.finishCurrentBatch();
      byte[] bytes = outputStream.toByteArray();
      outputStream.reset();
      arrowSerializer.resetWriter();
      return DorisRecord.of(bytes);
    } catch (Exception e) {
      LOG.error("Failed to convert arrow batch:", e);
    }
    return DorisRecord.empty;
  }

  public String buildJsonString(
      RowData record, int maxIndex, DorisRowConverter rowConverter, String[] fieldNames)
      throws IOException {
    int fieldIndex = 0;
    Map<String, String> valueMap = new HashMap<>();
    while (fieldIndex < maxIndex) {
      Object field = rowConverter.convertExternal(record, fieldIndex);
      String value = field != null ? field.toString() : null;
      valueMap.put(fieldNames[fieldIndex], value);
      fieldIndex++;
    }
    if (enableDelete) {
      valueMap.put(DORIS_DELETE_SIGN, parseDeleteSign(record.getRowKind()));
    }
    return objectMapper.writeValueAsString(valueMap);
  }

  public String buildCSVString(RowData record, int maxIndex, DorisRowConverter rowConverter)
      throws IOException {
    int fieldIndex = 0;
    StringJoiner joiner = new StringJoiner(fieldDelimiter);
    while (fieldIndex < maxIndex) {
      Object field = rowConverter.convertExternal(record, fieldIndex);
      String value = field != null ? field.toString() : NULL_VALUE;
      joiner.add(value);
      fieldIndex++;
    }
    if (enableDelete) {
      joiner.add(parseDeleteSign(record.getRowKind()));
    }
    return joiner.toString();
  }

  public String parseDeleteSign(RowKind rowKind) {
    if (RowKind.INSERT.equals(rowKind) || RowKind.UPDATE_AFTER.equals(rowKind)) {
      return "0";
    } else if (RowKind.DELETE.equals(rowKind) || RowKind.UPDATE_BEFORE.equals(rowKind)) {
      return "1";
    } else {
      throw new IllegalArgumentException("Unrecognized row kind:" + rowKind.toString());
    }
  }

  public static RowDataWithMetaSerializer.Builder builder() {
    return new RowDataWithMetaSerializer.Builder();
  }

  /** Builder for RowDataWithMetaSerializer. */
  public static class Builder {
    private String type;
    private String fieldDelimiter;
    private boolean deletable;

    public RowDataWithMetaSerializer.Builder setType(String type) {
      this.type = type;
      return this;
    }

    public RowDataWithMetaSerializer.Builder setFieldDelimiter(String fieldDelimiter) {
      this.fieldDelimiter = EscapeHandler.escapeString(fieldDelimiter);
      return this;
    }

    public RowDataWithMetaSerializer.Builder enableDelete(boolean deletable) {
      this.deletable = deletable;
      return this;
    }

    public RowDataWithMetaSerializer build() {
      Preconditions.checkState(
          CSV.equals(type) && fieldDelimiter != null || JSON.equals(type) || ARROW.equals(type));
      if (ARROW.equals(type)) {
        Preconditions.checkArgument(!deletable);
      }
      return new RowDataWithMetaSerializer(type, fieldDelimiter, deletable);
    }
  }
}
