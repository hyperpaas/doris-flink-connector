package org.apache.doris.flink.sink.batch;

import static org.apache.doris.flink.sink.writer.LoadConstants.ARROW;
import static org.apache.doris.flink.sink.writer.LoadConstants.CSV;
import static org.apache.doris.flink.sink.writer.LoadConstants.JSON;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.serializers.ArrowSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;

public class RowDataWithMeta {
  private String database;
  private String table;
  private String[] fieldNames;
  private DataType[] dataTypes;
  private RowData rowData;
  private String type;
  private ArrowSerializer arrowSerializer;
  private ByteArrayOutputStream outputStream;

  public RowDataWithMeta() {
  }

  private RowDataWithMeta(
      String type,
      String database,
      String table,
      String[] fieldNames,
      DataType[] dataTypes,
      RowData rowData) {
    this.type = type;
    this.database = database;
    this.table = table;
    this.fieldNames = fieldNames;
    this.dataTypes = dataTypes;
    this.rowData = rowData;
    if (ARROW.equals(type)) {
      LogicalType[] logicalTypes = TypeConversions.fromDataToLogicalType(dataTypes);
      RowType rowType = RowType.of(logicalTypes, fieldNames);
      arrowSerializer = new ArrowSerializer(rowType, rowType);
      outputStream = new ByteArrayOutputStream();
      try {
        arrowSerializer.open(new ByteArrayInputStream(new byte[0]), outputStream);
      } catch (Exception e) {
        throw new RuntimeException("failed to open arrow serializer:", e);
      }
    }
  }

  public static RowDataWithMeta.Builder builder() {
    return new RowDataWithMeta.Builder();
  }

  public static class Builder {
    private String database;
    private String table;
    private String[] fieldNames;
    private DataType[] dataTypes;
    private RowData rowData;
    private String type;

    public RowDataWithMeta.Builder setDatabase(String database) {
      this.database = database;
      return this;
    }

    public RowDataWithMeta.Builder setTable(String table) {
      this.table = table;
      return this;
    }

    public RowDataWithMeta.Builder setFieldNames(String[] fieldNames) {
      this.fieldNames = fieldNames;
      return this;
    }

    public RowDataWithMeta.Builder setDataTypes(DataType[] dataTypes) {
      this.dataTypes = dataTypes;
      return this;
    }

    public RowDataWithMeta.Builder setRowData(RowData rowData) {
      this.rowData = rowData;
      return this;
    }

    public RowDataWithMeta.Builder setType(String type) {
      this.type = type;
      return this;
    }

    public RowDataWithMeta build() {
      Preconditions.checkState(CSV.equals(type) || JSON.equals(type) || ARROW.equals(type));
      Preconditions.checkNotNull(dataTypes);
      Preconditions.checkNotNull(fieldNames);
      return new RowDataWithMeta(type, database, table, fieldNames, dataTypes, rowData);
    }
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public RowData getRowData() {
    return rowData;
  }

  public void setRowData(RowData rowData) {
    this.rowData = rowData;
  }

  public String getTableIdentifier() {
    return this.database + "." + this.table;
  }

  public String[] getFieldNames() {
    return fieldNames;
  }

  public void setFieldNames(String[] fieldNames) {
    Preconditions.checkNotNull(fieldNames);
    this.fieldNames = fieldNames;
  }

  public DataType[] getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(DataType[] dataTypes) {
    Preconditions.checkNotNull(dataTypes);
    this.dataTypes = dataTypes;
  }

  public ArrowSerializer getArrowSerializer() {
    return arrowSerializer;
  }

  public ByteArrayOutputStream getOutputStream() {
    return outputStream;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setArrowSerializer(
      ArrowSerializer arrowSerializer) {
    this.arrowSerializer = arrowSerializer;
  }

  public void setOutputStream(ByteArrayOutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public String toString() {
    return "RowDataWithMeta{"
        + "database='"
        + database
        + '\''
        + ", table='"
        + table
        + '\''
        + ", fieldNames="
        + Arrays.toString(fieldNames)
        + ", dataTypes="
        + Arrays.toString(dataTypes)
        + ", rowData="
        + rowData
        + ", type='"
        + type
        + '\''
        + '}';
  }
}
